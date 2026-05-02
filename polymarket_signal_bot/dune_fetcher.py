from __future__ import annotations

"""Fetch Polymarket OrderFilled history from Dune into the local indexer DB.

This module is an ingestion shortcut for real trades only. It does not replace
the Polygon indexer; it writes Dune rows into the same ``raw_transactions``
schema so scoring, cohorts, and training can reuse the existing pipeline.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from polymarket_signal_bot.indexer import (
    DEFAULT_INDEXER_DB_PATH,
    RawTransaction,
    normalize_address,
)

LOGGER = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DUNE_API_BASE_URL = "https://api.dune.com/api/v1"
DUNE_SOURCE_COUNTER = "dune_orderfilled"
DUNE_PAGE_SIZE = 10_000


class DuneFetcherError(RuntimeError):
    """Raised when Dune execution or ingestion fails."""


@dataclass(frozen=True)
class DuneFetcherConfig:
    api_key: str
    db_path: Path = DEFAULT_INDEXER_DB_PATH
    start_date: date | None = None
    end_date: date | None = None
    full: bool = False
    dry_run: bool = False
    page_size: int = DUNE_PAGE_SIZE
    performance: str = "medium"
    poll_seconds: float = 5.0
    max_wait_seconds: float = 3600.0
    request_timeout: float = 60.0
    base_url: str = DUNE_API_BASE_URL
    batch_size: int = 2_000


@dataclass(frozen=True)
class DuneTrade:
    block_time: str
    block_number: int
    tx_hash: str
    maker: str
    taker: str
    token_id: str
    price: float
    amount: float
    side: str
    market_id: str
    action: str = ""

    @property
    def timestamp(self) -> int:
        return int(parse_block_time(self.block_time).timestamp())


@dataclass(frozen=True)
class DedupeResult:
    trades: list[DuneTrade]
    raw_rows: int
    deduped_rows: int
    duplicate_rows: int


@dataclass(frozen=True)
class InsertResult:
    candidates: int
    inserted: int
    skipped_existing: int


class DuneApiClient:
    def __init__(self, config: DuneFetcherConfig) -> None:
        if not config.api_key:
            raise DuneFetcherError("DUNE_API_KEY is required")
        try:
            import httpx  # type: ignore
        except ModuleNotFoundError as exc:
            raise DuneFetcherError("Dune fetcher requires httpx. Install project indexer deps.") from exc
        self.config = config
        self._httpx = httpx
        self._client: Any | None = None

    async def __aenter__(self) -> "DuneApiClient":
        self._client = self._httpx.AsyncClient(
            base_url=self.config.base_url.rstrip("/"),
            timeout=self.config.request_timeout,
            headers={
                "X-Dune-Api-Key": self.config.api_key,
                "Content-Type": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def execute_sql(self, sql: str) -> str:
        if self._client is None:
            raise DuneFetcherError("Dune client is not connected")
        response = await self._client.post(
            "/sql/execute",
            json={"query_sql": sql, "performance": self.config.performance},
        )
        payload = self._json_response(response)
        execution_id = str(payload.get("execution_id") or "")
        if not execution_id:
            raise DuneFetcherError(f"Dune did not return execution_id: {payload}")
        LOGGER.info("Dune execution started execution_id=%s state=%s", execution_id, payload.get("state"))
        return execution_id

    async def wait_for_completion(self, execution_id: str) -> dict[str, Any]:
        if self._client is None:
            raise DuneFetcherError("Dune client is not connected")
        deadline = time.time() + self.config.max_wait_seconds
        while True:
            response = await self._client.get(f"/execution/{execution_id}/status")
            payload = self._json_response(response)
            state = str(payload.get("state") or "")
            if state in {"QUERY_STATE_COMPLETED", "QUERY_STATE_COMPLETED_PARTIAL"}:
                LOGGER.info("Dune execution completed execution_id=%s state=%s", execution_id, state)
                return payload
            if state in {"QUERY_STATE_FAILED", "QUERY_STATE_CANCELED", "QUERY_STATE_EXPIRED"}:
                error = payload.get("error") or payload
                raise DuneFetcherError(f"Dune execution {state}: {error}")
            if time.time() >= deadline:
                raise DuneFetcherError(f"Dune execution timed out after {self.config.max_wait_seconds}s")
            LOGGER.info("Dune execution pending execution_id=%s state=%s", execution_id, state)
            await asyncio.sleep(max(1.0, self.config.poll_seconds))

    async def result_pages(self, execution_id: str) -> AsyncIterator[list[dict[str, Any]]]:
        if self._client is None:
            raise DuneFetcherError("Dune client is not connected")
        offset = 0
        limit = max(1, self.config.page_size)
        while True:
            response = await self._client.get(
                f"/execution/{execution_id}/results",
                params={"limit": limit, "offset": offset, "allow_partial_results": "true"},
            )
            payload = self._json_response(response)
            rows = ((payload.get("result") or {}).get("rows") or [])
            if not isinstance(rows, list):
                rows = []
            yield [dict(row) for row in rows]
            next_offset = payload.get("next_offset")
            if next_offset is None or not rows:
                break
            offset = int(next_offset)

    @staticmethod
    def _json_response(response: Any) -> dict[str, Any]:
        try:
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001 - include Dune body for diagnostics.
            body = getattr(response, "text", "")
            raise DuneFetcherError(f"Dune API request failed: {body or exc}") from exc
        if not isinstance(payload, dict):
            raise DuneFetcherError(f"Unexpected Dune response: {payload}")
        return payload


class DuneTradeStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=30000")

    def __enter__(self) -> "DuneTradeStore":
        self.init_schema()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_transactions (
                hash TEXT NOT NULL,
                log_index INTEGER NOT NULL,
                block_number INTEGER NOT NULL,
                block_hash TEXT NOT NULL DEFAULT '',
                timestamp INTEGER NOT NULL,
                user_address TEXT NOT NULL DEFAULT '',
                market_id TEXT NOT NULL DEFAULT '',
                side TEXT NOT NULL DEFAULT '',
                price REAL NOT NULL DEFAULT 0,
                amount REAL NOT NULL DEFAULT 0,
                event_type TEXT NOT NULL,
                contract TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '',
                inserted_at INTEGER NOT NULL,
                PRIMARY KEY(hash, log_index)
            );
            CREATE INDEX IF NOT EXISTS idx_raw_transactions_user_time
                ON raw_transactions(user_address, timestamp);
            CREATE INDEX IF NOT EXISTS idx_raw_transactions_market_time
                ON raw_transactions(market_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_raw_transactions_block
                ON raw_transactions(block_number);
            CREATE TABLE IF NOT EXISTS indexer_counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        self.ensure_counter("raw_events")
        self.ensure_counter(DUNE_SOURCE_COUNTER)

    def ensure_counter(self, name: str) -> None:
        row = self.conn.execute(
            "SELECT value FROM indexer_counters WHERE name = ?",
            (name,),
        ).fetchone()
        if row is not None:
            return
        value = 0
        if name == "raw_events":
            value = int(self.conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0] or 0)
        now = int(time.time())
        self.conn.execute(
            "INSERT INTO indexer_counters(name, value, updated_at) VALUES (?, ?, ?)",
            (name, value, now),
        )
        self.conn.commit()

    def insert_trades(self, trades: Sequence[DuneTrade]) -> InsertResult:
        if not trades:
            return InsertResult(candidates=0, inserted=0, skipped_existing=0)
        existing_pairs = self.existing_tx_block_pairs(trades)
        candidates: list[RawTransaction] = []
        seen_pairs = set(existing_pairs)
        skipped_existing = 0
        for trade in trades:
            pair = (trade.tx_hash, trade.block_number)
            if pair in seen_pairs:
                skipped_existing += 1
                continue
            seen_pairs.add(pair)
            candidates.append(dune_trade_to_raw_transaction(trade))

        inserted = self.insert_raw_transactions(candidates)
        self.increment_counter(DUNE_SOURCE_COUNTER, inserted)
        self.conn.commit()
        return InsertResult(
            candidates=len(candidates),
            inserted=inserted,
            skipped_existing=skipped_existing,
        )

    def existing_tx_block_pairs(self, trades: Sequence[DuneTrade]) -> set[tuple[str, int]]:
        hashes = sorted({trade.tx_hash for trade in trades if trade.tx_hash})
        if not hashes:
            return set()
        existing: set[tuple[str, int]] = set()
        for chunk in chunked(hashes, 500):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""
                SELECT hash, block_number
                FROM raw_transactions
                WHERE hash IN ({placeholders})
                """,
                list(chunk),
            ).fetchall()
            existing.update((str(row["hash"]), int(row["block_number"] or 0)) for row in rows)
        return existing

    def insert_raw_transactions(self, rows: Sequence[RawTransaction]) -> int:
        if not rows:
            return 0
        now = int(time.time())
        with self.conn:
            cursor = self.conn.executemany(
                """
                INSERT OR IGNORE INTO raw_transactions(
                    hash, log_index, block_number, block_hash, timestamp,
                    user_address, market_id, side, price, amount, event_type,
                    contract, raw_json, inserted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.hash,
                        row.log_index,
                        row.block_number,
                        row.block_hash,
                        row.timestamp,
                        row.user_address,
                        row.market_id,
                        row.side,
                        row.price,
                        row.amount,
                        row.event_type,
                        row.contract,
                        row.raw_json,
                        now,
                    )
                    for row in rows
                ],
            )
            inserted = int(cursor.rowcount or 0)
            self.increment_counter("raw_events", inserted)
        return inserted

    def increment_counter(self, name: str, delta: int) -> None:
        if delta == 0:
            return
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO indexer_counters(name, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                value = MAX(0, indexer_counters.value + ?),
                updated_at = excluded.updated_at
            """,
            (name, max(0, delta), now, delta),
        )


async def fetch_and_store(config: DuneFetcherConfig) -> dict[str, Any]:
    try:
        return await _fetch_and_store_once(config, include_action=True)
    except DuneFetcherError as exc:
        if not is_missing_action_error(exc):
            raise
        LOGGER.warning("Dune table action column unavailable; retrying without action: %s", exc)
        return await _fetch_and_store_once(config, include_action=False)


async def _fetch_and_store_once(config: DuneFetcherConfig, *, include_action: bool) -> dict[str, Any]:
    sql = build_market_trades_sql(
        start_date=config.start_date,
        end_date=config.end_date,
        full=config.full,
        include_action=include_action,
    )
    LOGGER.info("Executing Dune market_trades query include_action=%s", include_action)
    raw_rows = 0
    deduped_rows = 0
    duplicate_rows = 0
    inserted = 0
    skipped_existing = 0
    examples: list[dict[str, Any]] = []
    seen_dedupe_keys: set[tuple[Any, ...]] = set()

    async with DuneApiClient(config) as client:
        execution_id = await client.execute_sql(sql)
        await client.wait_for_completion(execution_id)
        store_context = nullcontext_store(config.db_path) if config.dry_run else DuneTradeStore(config.db_path)
        with store_context as store:
            async for page_rows in client.result_pages(execution_id):
                dedupe = dedupe_trades(page_rows, seen_keys=seen_dedupe_keys)
                raw_rows += dedupe.raw_rows
                deduped_rows += dedupe.deduped_rows
                duplicate_rows += dedupe.duplicate_rows
                if len(examples) < 10:
                    examples.extend(dune_trade_to_example(trade) for trade in dedupe.trades[: 10 - len(examples)])
                if config.dry_run:
                    continue
                assert store is not None
                for batch in chunked(dedupe.trades, max(1, config.batch_size)):
                    result = store.insert_trades(batch)
                    inserted += result.inserted
                    skipped_existing += result.skipped_existing

    summary = {
        "raw_rows": raw_rows,
        "deduped_rows": deduped_rows,
        "duplicate_rows": duplicate_rows,
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "dry_run": config.dry_run,
        "include_action": include_action,
        "examples": examples[:10],
    }
    LOGGER.info(
        "Dune fetch complete raw=%s deduped=%s duplicates=%s inserted=%s skipped_existing=%s dry_run=%s",
        raw_rows,
        deduped_rows,
        duplicate_rows,
        inserted,
        skipped_existing,
        config.dry_run,
    )
    return summary


class nullcontext_store:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def build_market_trades_sql(
    *,
    start_date: date | None,
    end_date: date | None,
    full: bool,
    include_action: bool = True,
) -> str:
    if not full and start_date is None and end_date is None:
        raise DuneFetcherError("Use --full or provide --start-date/--end-date")
    action_select = ",\n        CAST(action AS VARCHAR) AS action" if include_action else ",\n        '' AS action"
    filters = ["1 = 1"]
    if start_date is not None:
        filters.append(f"block_time >= DATE '{start_date.isoformat()}'")
    if end_date is not None:
        filters.append(f"block_time < DATE '{end_date.isoformat()}' + INTERVAL '1' DAY")
    where = "\n        AND ".join(filters)
    return f"""
    SELECT
        CAST(block_time AS VARCHAR) AS block_time,
        CAST(block_number AS BIGINT) AS block_number,
        LOWER(CAST(tx_hash AS VARCHAR)) AS tx_hash,
        LOWER(CAST(maker AS VARCHAR)) AS maker,
        LOWER(CAST(taker AS VARCHAR)) AS taker,
        CAST(token_id AS VARCHAR) AS token_id,
        CAST(price AS DOUBLE) AS price,
        CAST(amount AS DOUBLE) AS amount,
        UPPER(CAST(side AS VARCHAR)) AS side,
        CAST(condition_id AS VARCHAR) AS market_id{action_select}
    FROM polymarket_polygon.market_trades
    WHERE {where}
    ORDER BY block_time ASC, block_number ASC, tx_hash ASC
    """.strip()


def dedupe_trades(
    rows: Sequence[dict[str, Any]],
    *,
    seen_keys: set[tuple[Any, ...]] | None = None,
) -> DedupeResult:
    seen_keys = seen_keys if seen_keys is not None else set()
    deduped: list[DuneTrade] = []
    duplicates = 0
    trades = [normalize_dune_trade(raw) for raw in rows]
    trades.sort(key=action_priority, reverse=True)
    for trade in trades:
        key = dedupe_key(trade)
        if key in seen_keys:
            duplicates += 1
            continue
        seen_keys.add(key)
        deduped.append(trade)
    return DedupeResult(
        trades=deduped,
        raw_rows=len(rows),
        deduped_rows=len(deduped),
        duplicate_rows=duplicates,
    )


def dedupe_key(trade: DuneTrade) -> tuple[Any, ...]:
    return (
        trade.tx_hash,
        trade.block_time[:19],
        round(trade.price, 10),
        round(trade.amount, 10),
        trade.side,
        trade.market_id,
    )


def normalize_dune_trade(row: dict[str, Any]) -> DuneTrade:
    market_id = str(row.get("market_id") or row.get("condition_id") or row.get("token_id") or "")
    return DuneTrade(
        block_time=str(row.get("block_time") or ""),
        block_number=int(row.get("block_number") or 0),
        tx_hash=str(row.get("tx_hash") or row.get("hash") or "").lower(),
        maker=normalize_address(str(row.get("maker") or "")),
        taker=normalize_address(str(row.get("taker") or "")),
        token_id=str(row.get("token_id") or ""),
        price=float(row.get("price") or 0.0),
        amount=float(row.get("amount") or 0.0),
        side=normalize_side(str(row.get("side") or "")),
        market_id=market_id,
        action=str(row.get("action") or ""),
    )


def dune_trade_to_raw_transaction(trade: DuneTrade) -> RawTransaction:
    user_address = trade_user_address(trade)
    raw_json = {
        "source": "dune",
        "block_time": trade.block_time,
        "tx_hash": trade.tx_hash,
        "maker": trade.maker,
        "taker": trade.taker,
        "token_id": trade.token_id,
        "condition_id": trade.market_id,
        "action": trade.action,
    }
    return RawTransaction(
        hash=trade.tx_hash,
        log_index=synthetic_log_index(trade),
        block_number=trade.block_number,
        block_hash="",
        timestamp=trade.timestamp,
        user_address=user_address,
        market_id=trade.market_id or trade.token_id,
        side=trade.side,
        price=trade.price,
        amount=trade.amount,
        event_type="OrderFilled",
        contract="dune:polymarket_polygon.market_trades",
        raw_json=json.dumps(raw_json, sort_keys=True),
    )


def trade_user_address(trade: DuneTrade) -> str:
    action = trade.action.upper()
    if "TAKER" in action and trade.taker:
        return trade.taker
    return trade.maker or trade.taker


def synthetic_log_index(trade: DuneTrade) -> int:
    digest = hashlib.sha1(
        f"{trade.tx_hash}|{trade.block_number}|{trade.market_id}|{trade.side}|{trade.price}|{trade.amount}".encode(
            "utf-8"
        )
    ).hexdigest()
    return 2_000_000_000 + (int(digest[:8], 16) % 1_000_000_000)


def dune_trade_to_example(trade: DuneTrade) -> dict[str, Any]:
    return {
        "block_time": trade.block_time,
        "block_number": trade.block_number,
        "tx_hash": trade.tx_hash,
        "maker": trade.maker,
        "taker": trade.taker,
        "market_id": trade.market_id,
        "side": trade.side,
        "price": trade.price,
        "amount": trade.amount,
        "action": trade.action,
    }


def parse_block_time(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.fromtimestamp(0, tz=UTC)
    text = text.replace(" UTC", "").replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def normalize_side(value: str) -> str:
    side = value.strip().upper()
    if side in {"BUY", "YES", "LONG"}:
        return "BUY"
    if side in {"SELL", "NO", "SHORT"}:
        return "SELL"
    return side


def action_priority(trade: DuneTrade) -> int:
    action = trade.action.upper()
    if "TAKER" in action:
        return 4
    if "TRADE" in action or "FILL" in action or "ORDER" in action:
        return 3
    if action:
        return 2
    return 1


def is_missing_action_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "action" in text and (
        "cannot be resolved" in text
        or "not found" in text
        or "column" in text
        or "does not exist" in text
    )


def chunked(items: Sequence[Any], size: int) -> list[Sequence[Any]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except ModuleNotFoundError:
        return
    load_dotenv(ROOT / ".env")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Polymarket OrderFilled trades from Dune.")
    parser.add_argument("--start-date", help="Inclusive start date YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Inclusive end date YYYY-MM-DD.")
    parser.add_argument("--full", action="store_true", help="Fetch all available history.")
    parser.add_argument("--dry-run", action="store_true", help="Print stats and examples without saving.")
    parser.add_argument("--db", default=os.getenv("INDEXER_DB_PATH", str(DEFAULT_INDEXER_DB_PATH)))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("DUNE_PAGE_SIZE", str(DUNE_PAGE_SIZE))))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("DUNE_BATCH_SIZE", "2000")))
    parser.add_argument("--performance", choices=("small", "medium", "large"), default=os.getenv("DUNE_PERFORMANCE", "medium"))
    parser.add_argument("--poll-seconds", type=float, default=float(os.getenv("DUNE_POLL_SECONDS", "5")))
    parser.add_argument("--max-wait-seconds", type=float, default=float(os.getenv("DUNE_MAX_WAIT_SECONDS", "3600")))
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    return parser


def config_from_args(args: argparse.Namespace) -> DuneFetcherConfig:
    return DuneFetcherConfig(
        api_key=os.getenv("DUNE_API_KEY", ""),
        db_path=Path(args.db),
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        full=bool(args.full),
        dry_run=bool(args.dry_run),
        page_size=max(1, int(args.page_size)),
        batch_size=max(1, int(args.batch_size)),
        performance=str(args.performance),
        poll_seconds=max(1.0, float(args.poll_seconds)),
        max_wait_seconds=max(1.0, float(args.max_wait_seconds)),
    )


async def async_main(argv: Sequence[str] | None = None) -> int:
    load_dotenv_if_available()
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = config_from_args(args)
    summary = await fetch_and_store(config)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(async_main(argv))
    except DuneFetcherError as exc:
        LOGGER.error("%s", exc)
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 1
    except KeyboardInterrupt:
        LOGGER.info("Dune fetcher stopped by user")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
