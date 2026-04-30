from __future__ import annotations

"""Polygon event indexer for Polymarket on-chain history.

This module indexes Polymarket smart-contract logs directly from Polygon RPC and
stores them in a SQLite table shaped for the existing analytics pipeline. It is
designed as the first local storage backend before moving the same event stream
to ClickHouse.

Example:
    $env:POLYGON_RPC_URL="https://polygon-mainnet.g.alchemy.com/v2/..."
    python -m src.indexer --start-block 50000000 --end-block 50001000 --dry-run
    python -m src.indexer --sync --batch-size 2000 --max-workers 8
"""

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

DEFAULT_INDEXER_DB_PATH = Path("data/indexer.db")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
USDC_ASSET_ID = 0
TOKEN_DECIMALS = 1_000_000

CONDITIONAL_TOKENS_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
LEGACY_CTF_EXCHANGE_ADDRESS = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
LEGACY_NEG_RISK_CTF_EXCHANGE_ADDRESS = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
CTF_EXCHANGE_ADDRESS = "0xe111180000d2663c0091e4f400237545b87b996b"
NEG_RISK_CTF_EXCHANGE_ADDRESS = "0xe2222d279d744050d28e00520010520000310f59"

# Hard-coded topics keep every eth_getLogs call narrowly filtered, even when
# optional keccak helpers are not installed.
TRANSFER_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
TRANSFER_BATCH_TOPIC = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)
CONDITION_PREPARATION_TOPIC = (
    "0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177"
)
PAYOUT_REDEMPTION_TOPIC = (
    "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d"
)
ORDER_FILLED_V1_TOPIC = (
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
)
ORDER_FILLED_V2_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)
ORDERS_MATCHED_V2_TOPIC = (
    "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c"
)
ORDER_CANCELLED_TOPIC = (
    "0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d"
)
ORDER_PLACED_TOPIC = (
    "0xb087bc729ec5e16d080749aa735677ed7a580ff3f3e04fcd376155d6a89d6a9a"
)
ORDER_FILLED_TOPIC = ORDER_FILLED_V1_TOPIC
OWNER_SELECTOR = "0x8da5cb5b"
GET_CTF_FUNCTION = "getCtf()"
GET_ORACLE_FUNCTION = "getOracle()"

EVENT_SIGNATURES: dict[str, str] = {
    "ConditionPreparation": "ConditionPreparation(bytes32,address,bytes32,uint256)",
    "PayoutRedemption": "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)",
    "OrderFilledV2": "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)",
    "OrdersMatchedV2": "OrdersMatched(bytes32,address,uint8,uint256,uint256,uint256)",
    "OrderPlaced": "OrderPlaced(bytes32,address,uint256,uint256,uint256,uint256)",
}

EVENT_TOPIC_FALLBACKS: dict[str, str] = {
    "TransferSingle": TRANSFER_SINGLE_TOPIC,
    "TransferBatch": TRANSFER_BATCH_TOPIC,
    "ConditionPreparation": CONDITION_PREPARATION_TOPIC,
    "PayoutRedemption": PAYOUT_REDEMPTION_TOPIC,
    "OrderFilledV1": ORDER_FILLED_V1_TOPIC,
    "OrderFilledV2": ORDER_FILLED_V2_TOPIC,
    "OrdersMatchedV2": ORDERS_MATCHED_V2_TOPIC,
    "OrderCancelled": ORDER_CANCELLED_TOPIC,
    "OrderPlaced": ORDER_PLACED_TOPIC,
}


class MissingIndexerDependencyError(RuntimeError):
    """Raised when an optional runtime dependency is missing."""


class RpcExecutionReverted(RuntimeError):
    """Raised for non-retryable eth_call execution reverts."""


class RpcLogRangeTooLarge(RuntimeError):
    """Raised when a log query range is too large for the RPC provider."""


@dataclass(frozen=True)
class IndexerConfig:
    rpc_url: str
    db_path: Path = DEFAULT_INDEXER_DB_PATH
    start_block: int | None = None
    end_block: int | None = None
    sync: bool = False
    dry_run: bool = False
    batch_size: int = 1_000
    max_workers: int = 5
    block_chunk_size: int = 100
    min_log_chunk_size: int = 10
    poll_seconds: float = 12.0
    log_every: int = 1_000
    reorg_depth: int = 1_000
    default_start_block: int = 0
    rpc_rps: float = 25.0
    verify_contracts: bool = True
    test: bool = False
    dry_run_print_limit: int | None = None
    sample_log_limit: int = 5
    request_timeout: float = 45.0
    max_retries: int = 6


@dataclass(frozen=True)
class ContractSpec:
    name: str
    address: str
    event_names: tuple[str, ...]

    @property
    def normalized_address(self) -> str:
        return normalize_address(self.address)


@dataclass(frozen=True)
class BlockInfo:
    number: int
    hash: str
    timestamp: int


@dataclass(frozen=True)
class RawTransaction:
    hash: str
    log_index: int
    block_number: int
    block_hash: str
    timestamp: int
    user_address: str
    market_id: str
    side: str
    price: float
    amount: float
    event_type: str
    contract: str
    raw_json: str


class IndexerStore:
    def __init__(self, path: str | Path = DEFAULT_INDEXER_DB_PATH) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")

    def __enter__(self) -> "IndexerStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
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

            CREATE TABLE IF NOT EXISTS indexer_state (
                name TEXT PRIMARY KEY,
                last_block INTEGER NOT NULL,
                last_block_hash TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS indexer_blocks (
                block_number INTEGER PRIMARY KEY,
                block_hash TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        self.conn.commit()

    def last_block(self, name: str = "polygon") -> int | None:
        row = self.conn.execute(
            "SELECT last_block FROM indexer_state WHERE name = ?",
            (name,),
        ).fetchone()
        return int(row["last_block"]) if row else None

    def set_last_block(
        self, block_number: int, block_hash: str = "", name: str = "polygon"
    ) -> None:
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO indexer_state(name, last_block, last_block_hash, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                last_block = excluded.last_block,
                last_block_hash = excluded.last_block_hash,
                updated_at = excluded.updated_at
            """,
            (name, block_number, block_hash, now),
        )
        self.conn.commit()

    def insert_raw_transactions(self, rows: Sequence[RawTransaction]) -> int:
        if not rows:
            return 0
        now = int(time.time())
        with self.conn:
            cursor = self.conn.executemany(
                """
                INSERT OR REPLACE INTO raw_transactions(
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
        return int(cursor.rowcount or 0)

    def upsert_blocks(self, blocks: Iterable[BlockInfo]) -> None:
        block_rows = list(blocks)
        if not block_rows:
            return
        now = int(time.time())
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO indexer_blocks(block_number, block_hash, timestamp, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(block_number) DO UPDATE SET
                    block_hash = excluded.block_hash,
                    timestamp = excluded.timestamp,
                    updated_at = excluded.updated_at
                """,
                [(block.number, block.hash, block.timestamp, now) for block in block_rows],
            )

    def recent_blocks(self, limit: int = 1_000) -> list[BlockInfo]:
        rows = self.conn.execute(
            """
            SELECT block_number, block_hash, timestamp
            FROM indexer_blocks
            ORDER BY block_number DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [
            BlockInfo(
                number=int(row["block_number"]),
                hash=str(row["block_hash"]),
                timestamp=int(row["timestamp"]),
            )
            for row in rows
        ]

    def delete_from_block(self, block_number: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM raw_transactions WHERE block_number >= ?",
                (block_number,),
            )
            self.conn.execute(
                "DELETE FROM indexer_blocks WHERE block_number >= ?",
                (block_number,),
            )


class AsyncRateLimiter:
    def __init__(self, rps: float) -> None:
        self.rps = float(rps or 0)
        self._interval = 1.0 / self.rps if self.rps > 0 else 0.0
        self._lock: asyncio.Lock | None = asyncio.Lock() if self.rps > 0 else None
        self._next_at = 0.0

    async def acquire(self) -> None:
        if self._lock is None:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            if now < self._next_at:
                await asyncio.sleep(self._next_at - now)
                now = loop.time()
            self._next_at = max(self._next_at, now) + self._interval


class PolygonRpcClient:
    def __init__(
        self,
        rpc_url: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 5,
        rps: float = 5.0,
    ) -> None:
        if not rpc_url:
            raise ValueError("POLYGON_RPC_URL is required")
        try:
            import httpx  # type: ignore
        except ModuleNotFoundError as exc:
            raise MissingIndexerDependencyError(
                "Indexer requires httpx. Install it with: python -m pip install httpx"
            ) from exc

        self._httpx = httpx
        self.rpc_url = rpc_url
        self.timeout = timeout
        self.max_retries = max_retries
        self._rate_limiter = AsyncRateLimiter(rps)
        self._client: Any | None = None
        self._next_id = 1

    async def __aenter__(self) -> "PolygonRpcClient":
        self._client = self._httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def call(self, method: str, params: list[Any] | None = None) -> Any:
        if self._client is None:
            raise RuntimeError("RPC client is not connected")
        params = params or []
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            payload = {
                "jsonrpc": "2.0",
                "id": self._next_request_id(),
                "method": method,
                "params": params,
            }
            try:
                await self._rate_limiter.acquire()
                response = await self._client.post(self.rpc_url, json=payload)
                if method == "eth_getLogs" and response.status_code == 400:
                    raise RpcLogRangeTooLarge(summarize_rpc_response(response))
                response.raise_for_status()
                body = response.json()
                if "error" in body:
                    error = body["error"]
                    if method == "eth_call" and isinstance(error, dict) and error.get("code") == 3:
                        raise RpcExecutionReverted(f"RPC error for {method}: {error}")
                    if method == "eth_getLogs" and is_log_range_error(error):
                        raise RpcLogRangeTooLarge(f"RPC error for {method}: {error}")
                    raise RuntimeError(f"RPC error for {method}: {error}")
                return body.get("result")
            except RpcExecutionReverted:
                raise
            except RpcLogRangeTooLarge:
                raise
            except Exception as exc:  # noqa: BLE001 - transient RPC failures are retried.
                last_error = exc
                delay = min(30.0, 0.5 * (2**attempt))
                LOGGER.warning(
                    "RPC %s failed on attempt %s/%s: %s: %s",
                    method,
                    attempt + 1,
                    self.max_retries,
                    type(exc).__name__,
                    str(exc) or repr(exc),
                )
                await asyncio.sleep(delay)
        raise RuntimeError(f"RPC {method} failed after retries: {last_error}")

    async def current_block(self) -> int:
        return hex_to_int(await self.call("eth_blockNumber"))

    async def get_block(self, block_number: int) -> BlockInfo:
        result = await self.call("eth_getBlockByNumber", [hex(block_number), False])
        if not result:
            raise RuntimeError(f"Block {block_number} not found")
        return BlockInfo(
            number=hex_to_int(result["number"]),
            hash=str(result.get("hash") or ""),
            timestamp=hex_to_int(result.get("timestamp", "0x0")),
        )

    async def get_code(self, address: str) -> str:
        result = await self.call("eth_getCode", [normalize_address(address), "latest"])
        return str(result or "0x")

    async def eth_call(self, address: str, data: str) -> str:
        result = await self.call(
            "eth_call",
            [{"to": normalize_address(address), "data": data}, "latest"],
        )
        return str(result or "0x")

    async def get_transaction(self, tx_hash: str) -> dict[str, Any] | None:
        result = await self.call("eth_getTransactionByHash", [tx_hash])
        return dict(result) if result else None

    async def get_logs(
        self,
        *,
        address: str,
        from_block: int,
        to_block: int,
        topics: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            "address": normalize_address(address),
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
        }
        if topics:
            query["topics"] = [topics]
        result = await self.call("eth_getLogs", [query])
        if not result:
            return []
        return list(result)

    def _next_request_id(self) -> int:
        value = self._next_id
        self._next_id += 1
        return value


class PolygonEventIndexer:
    def __init__(
        self,
        config: IndexerConfig,
        *,
        store: IndexerStore | None = None,
        contracts: Sequence[ContractSpec] | None = None,
    ) -> None:
        self.config = config
        self.store = store or IndexerStore(config.db_path)
        self.contracts = list(contracts or default_contract_specs())
        self._topic_to_event = build_topic_map(self.contracts)
        self._order_fill_samples_logged = 0

    async def run(self) -> None:
        self.store.init_schema()
        async with PolygonRpcClient(
            self.config.rpc_url,
            timeout=self.config.request_timeout,
            max_retries=self.config.max_retries,
            rps=self.config.rpc_rps,
        ) as rpc:
            if self.config.verify_contracts:
                await self._verify_contracts(rpc)
            if self.config.test:
                await self._run_test(rpc)
                return
            await self._handle_reorg(rpc)
            if self.config.sync:
                await self._run_sync(rpc)
            else:
                start_block, end_block = await self._resolve_range(rpc)
                await self.index_range(rpc, start_block, end_block)

    async def _run_test(self, rpc: PolygonRpcClient) -> None:
        block_number = self.config.start_block or self.config.end_block or await rpc.current_block()
        LOGGER.info("Running one-block indexer test for block %s", block_number)
        _, _, rows, _ = await self._index_chunk(rpc, block_number, block_number)
        for row in rows[:10]:
            print(json.dumps(raw_transaction_to_dict(row), sort_keys=True))
        LOGGER.info("Test block %s decoded %s events, printed %s", block_number, len(rows), len(rows[:10]))

    async def _run_sync(self, rpc: PolygonRpcClient) -> None:
        while True:
            start_block, end_block = await self._resolve_range(rpc)
            if start_block <= end_block:
                await self.index_range(rpc, start_block, end_block)
            await asyncio.sleep(self.config.poll_seconds)

    async def _resolve_range(self, rpc: PolygonRpcClient) -> tuple[int, int]:
        current = await rpc.current_block()
        stored_last = self.store.last_block()
        start_block = self.config.start_block
        if start_block is None:
            start_block = (
                stored_last + 1 if stored_last is not None else self.config.default_start_block
            )
        end_block = self.config.end_block if self.config.end_block is not None else current
        if end_block > current:
            end_block = current
        return max(0, start_block), max(0, end_block)

    async def index_range(
        self, rpc: PolygonRpcClient, start_block: int, end_block: int
    ) -> None:
        if start_block > end_block:
            LOGGER.info("No blocks to index: start=%s end=%s", start_block, end_block)
            return

        LOGGER.info(
            "Indexing Polygon blocks %s..%s with %s workers, chunk=%s",
            start_block,
            end_block,
            self.config.max_workers,
            self.config.block_chunk_size,
        )
        total_events = 0
        last_logged = start_block
        next_start_to_store = start_block
        completed: dict[int, tuple[int, int, list[RawTransaction], list[BlockInfo]]] = {}
        pending: set[asyncio.Task[tuple[int, int, list[RawTransaction], list[BlockInfo]]]] = set()
        task_ranges: dict[
            asyncio.Task[tuple[int, int, list[RawTransaction], list[BlockInfo]]],
            tuple[int, int],
        ] = {}
        chunk_failures: dict[tuple[int, int], int] = {}

        def schedule(
            block_range: tuple[int, int],
        ) -> None:
            task = asyncio.create_task(self._index_chunk(rpc, block_range[0], block_range[1]))
            pending.add(task)
            task_ranges[task] = block_range

        async def record_done(
            done_tasks: Iterable[
                asyncio.Task[tuple[int, int, list[RawTransaction], list[BlockInfo]]]
            ],
        ) -> None:
            for task in done_tasks:
                block_range = task_ranges.pop(task, None)
                try:
                    result = task.result()
                except Exception as exc:
                    if block_range is None:
                        raise
                    chunk_start, chunk_end = block_range
                    chunk_failures[block_range] = chunk_failures.get(block_range, 0) + 1
                    if chunk_start < chunk_end:
                        mid = (chunk_start + chunk_end) // 2
                        LOGGER.warning(
                            "Chunk %s..%s failed (%s); splitting to %s..%s and %s..%s",
                            chunk_start,
                            chunk_end,
                            exc,
                            chunk_start,
                            mid,
                            mid + 1,
                            chunk_end,
                        )
                        schedule((chunk_start, mid))
                        schedule((mid + 1, chunk_end))
                    else:
                        delay = min(60.0, 2.0 ** min(chunk_failures[block_range], 6))
                        LOGGER.warning(
                            "Block %s failed (%s); retrying in %.1fs",
                            chunk_start,
                            exc,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        schedule(block_range)
                    continue
                completed[result[0]] = result

        def flush_ready() -> int:
            nonlocal next_start_to_store
            stored_events = 0
            while next_start_to_store in completed:
                result = completed.pop(next_start_to_store)
                chunk_start, chunk_end, rows, blocks = result
                stored_events += self._store_chunk(chunk_start, chunk_end, rows, blocks)
                next_start_to_store = chunk_end + 1
            return stored_events

        for block_range in chunk_ranges(start_block, end_block, self.config.block_chunk_size):
            while len(pending) >= self.config.max_workers:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                await record_done(done)
                total_events += flush_ready()
            schedule(block_range)

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            await record_done(done)
            total_events += flush_ready()
            max_done_block = self.store.last_block() or start_block
            if max_done_block - last_logged >= self.config.log_every:
                LOGGER.info("Indexed through block %s, events=%s", max_done_block, total_events)
                last_logged = max_done_block

        LOGGER.info("Indexing finished: blocks=%s..%s events=%s", start_block, end_block, total_events)

    def _store_chunk(
        self,
        chunk_start: int,
        chunk_end: int,
        rows: Sequence[RawTransaction],
        blocks: Sequence[BlockInfo],
    ) -> int:
        inserted = 0
        if self.config.dry_run:
            printable_rows = list(rows)
            if self.config.dry_run_print_limit is not None:
                printable_rows = printable_rows[: max(0, self.config.dry_run_print_limit)]
            for row in printable_rows:
                print(json.dumps(raw_transaction_to_dict(row), sort_keys=True))
            LOGGER.info(
                "Dry-run chunk %s..%s decoded %s events, printed %s",
                chunk_start,
                chunk_end,
                len(rows),
                len(printable_rows),
            )
            return len(rows)

        self.store.upsert_blocks(blocks)
        for batch in batched(rows, max(1, self.config.batch_size)):
            inserted += self.store.insert_raw_transactions(batch)
        last_hash = blocks[-1].hash if blocks else ""
        current_last = self.store.last_block()
        if current_last is None or chunk_end >= current_last:
            self.store.set_last_block(chunk_end, last_hash)
        else:
            LOGGER.debug(
                "Stored historical chunk %s..%s without moving checkpoint %s",
                chunk_start,
                chunk_end,
                current_last,
            )
        LOGGER.debug("Stored chunk %s..%s rows=%s", chunk_start, chunk_end, len(rows))
        return inserted

    async def _index_chunk(
        self, rpc: PolygonRpcClient, start_block: int, end_block: int
    ) -> tuple[int, int, list[RawTransaction], list[BlockInfo]]:
        logs: list[dict[str, Any]] = []
        for contract in self.contracts:
            topics = topics_for_contract(contract)
            if not topics:
                LOGGER.warning(
                    "Skipping %s log query because no event topics are available",
                    contract.name,
                )
                continue
            contract_logs = await self._fetch_logs_for_contract(
                rpc,
                contract,
                start_block,
                end_block,
                topics,
            )
            logs.extend(contract_logs)

        block_numbers = sorted({hex_to_int(log.get("blockNumber", "0x0")) for log in logs})
        blocks = [await rpc.get_block(block_number) for block_number in block_numbers]
        block_map = {block.number: block for block in blocks}
        tx_condition_ids = await self._load_tx_condition_ids(rpc, logs)
        rows: list[RawTransaction] = []
        for log in logs:
            decoded = decode_log(
                log,
                block_map=block_map,
                topic_to_event=self._topic_to_event,
                tx_condition_ids=tx_condition_ids,
            )
            for row in decoded:
                self._log_order_fill_sample(row)
            rows.extend(decoded)
        rows.sort(key=lambda row: (row.block_number, row.log_index, row.hash))
        return start_block, end_block, rows, blocks

    async def _fetch_logs_for_contract(
        self,
        rpc: PolygonRpcClient,
        contract: ContractSpec,
        start_block: int,
        end_block: int,
        topics: Sequence[str],
    ) -> list[dict[str, Any]]:
        try:
            return await rpc.get_logs(
                address=contract.normalized_address,
                from_block=start_block,
                to_block=end_block,
                topics=list(topics),
            )
        except RpcLogRangeTooLarge as exc:
            if end_block - start_block + 1 <= self.config.min_log_chunk_size:
                raise
            mid = (start_block + end_block) // 2
            LOGGER.warning(
                "RPC rejected logs for %s blocks %s..%s; splitting to %s..%s and %s..%s: %s",
                contract.name,
                start_block,
                end_block,
                start_block,
                mid,
                mid + 1,
                end_block,
                exc,
            )
            left, right = await asyncio.gather(
                self._fetch_logs_for_contract(rpc, contract, start_block, mid, topics),
                self._fetch_logs_for_contract(rpc, contract, mid + 1, end_block, topics),
            )
            return [*left, *right]

    async def _load_tx_condition_ids(
        self, rpc: PolygonRpcClient, logs: Sequence[dict[str, Any]]
    ) -> dict[str, str]:
        order_filled_events = {"OrderFilledV2", "OrdersMatchedV2"}
        tx_hashes = sorted(
            {
                str(log.get("transactionHash") or "")
                for log in logs
                if self._topic_to_event.get(first_topic(log)) in order_filled_events
            }
        )
        tx_hashes = [tx_hash for tx_hash in tx_hashes if tx_hash]
        condition_ids: dict[str, str] = {}
        for tx_hash in tx_hashes:
            try:
                tx = await rpc.get_transaction(tx_hash)
            except Exception as exc:  # noqa: BLE001 - missing tx input falls back to tokenId.
                LOGGER.debug("Failed to load transaction %s for conditionId: %s", tx_hash, exc)
                continue
            condition_id = condition_id_from_tx_input(str((tx or {}).get("input") or ""))
            if condition_id:
                condition_ids[tx_hash] = condition_id
        return condition_ids

    def _log_order_fill_sample(self, row: RawTransaction) -> None:
        if row.event_type != "OrderFilled":
            return
        if self._order_fill_samples_logged >= self.config.sample_log_limit:
            return
        self._order_fill_samples_logged += 1
        LOGGER.info(
            "OrderFilled sample: side=%s market_id=%s user=%s price=%.6f amount=%.6f tx=%s",
            row.side,
            row.market_id,
            row.user_address,
            row.price,
            row.amount,
            row.hash,
        )

    async def _handle_reorg(self, rpc: PolygonRpcClient) -> None:
        recent = self.store.recent_blocks(limit=self.config.reorg_depth)
        if not recent:
            return
        newest = recent[0]
        chain_block = await rpc.get_block(newest.number)
        if chain_block.hash.lower() == newest.hash.lower():
            return
        cutoff = max(0, newest.number - self.config.reorg_depth)
        LOGGER.warning(
            "Reorg mismatch at block %s; deleting indexed rows from %s",
            newest.number,
            cutoff,
        )
        self.store.delete_from_block(cutoff)
        self.store.set_last_block(max(0, cutoff - 1), "")

    async def _verify_contracts(self, rpc: PolygonRpcClient) -> None:
        for contract in self.contracts:
            code = await rpc.get_code(contract.normalized_address)
            if code in ("0x", "0x0", ""):
                raise RuntimeError(
                    f"{contract.name} has no bytecode at {contract.normalized_address}"
                )

            if "exchange" not in contract.name.lower():
                continue

            probes = await self._exchange_probe_results(rpc, contract)
            if not any(result for result in probes.values()):
                if contract.name.lower().startswith("legacy"):
                    LOGGER.info(
                        "Verified %s at %s via bytecode only; legacy contract probes unavailable",
                        contract.name,
                        contract.normalized_address,
                    )
                    continue
                raise RuntimeError(
                    f"{contract.name} at {contract.normalized_address} did not respond "
                    "to exchange probes getCtf()/owner()/getOracle(). Use --skip-contract-check "
                    "only if this is expected."
                )
            LOGGER.info(
                "Verified %s at %s via %s",
                contract.name,
                contract.normalized_address,
                ", ".join(name for name, result in probes.items() if result),
            )

    async def _exchange_probe_results(
        self, rpc: PolygonRpcClient, contract: ContractSpec
    ) -> dict[str, bool]:
        probes = {
            GET_CTF_FUNCTION: function_selector(GET_CTF_FUNCTION),
            "owner()": OWNER_SELECTOR,
            GET_ORACLE_FUNCTION: function_selector(GET_ORACLE_FUNCTION),
        }
        results: dict[str, bool] = {}
        for name, selector in probes.items():
            if not selector:
                results[name] = False
                continue
            try:
                result = await rpc.eth_call(contract.normalized_address, selector)
            except Exception as exc:  # noqa: BLE001 - probes are best effort.
                LOGGER.debug("Probe %s failed for %s: %s", name, contract.name, exc)
                results[name] = False
                continue
            results[name] = is_nonzero_eth_call_result(result)
            if name == GET_CTF_FUNCTION and results[name]:
                returned = decode_address_return(result)
                results[name] = returned == CONDITIONAL_TOKENS_ADDRESS
        return results


def decode_log(
    log: dict[str, Any],
    *,
    block_map: dict[int, BlockInfo],
    topic_to_event: dict[str, str] | None = None,
    tx_condition_ids: dict[str, str] | None = None,
) -> list[RawTransaction]:
    topics = [str(topic).lower() for topic in log.get("topics", [])]
    if not topics:
        return []
    topic_to_event = topic_to_event or build_topic_map(default_contract_specs())
    event_type = topic_to_event.get(topics[0])
    if not event_type:
        return []

    block_number = hex_to_int(log.get("blockNumber", "0x0"))
    block = block_map.get(
        block_number,
        BlockInfo(
            number=block_number,
            hash=str(log.get("blockHash") or ""),
            timestamp=0,
        ),
    )
    context = _LogContext(
        log=log,
        topics=topics,
        block=block,
        tx_hash=str(log.get("transactionHash") or ""),
        base_log_index=hex_to_int(log.get("logIndex", "0x0")),
        contract=normalize_address(str(log.get("address") or "")),
        event_type=event_type,
        tx_condition_id=(tx_condition_ids or {}).get(str(log.get("transactionHash") or ""), ""),
    )

    if event_type == "OrderFilledV2":
        return [_decode_order_filled_v2(context)]
    if event_type == "OrdersMatchedV2":
        return [_decode_orders_matched_v2(context)]
    if event_type == "OrderFilledV1":
        return [_decode_order_filled_v1(context)]
    if event_type == "OrderCancelled":
        return [_decode_order_cancelled(context)]
    if event_type == "TransferSingle":
        return [_decode_transfer_single(context)]
    if event_type == "TransferBatch":
        return _decode_transfer_batch(context)
    if event_type == "ConditionPreparation":
        return [_decode_condition_preparation(context)]
    if event_type == "PayoutRedemption":
        return [_decode_payout_redemption(context)]
    if event_type == "OrderPlaced":
        return [_decode_generic_order_placed(context)]
    return []


@dataclass(frozen=True)
class _LogContext:
    log: dict[str, Any]
    topics: list[str]
    block: BlockInfo
    tx_hash: str
    base_log_index: int
    contract: str
    event_type: str
    tx_condition_id: str = ""

    def row(
        self,
        *,
        log_index: int | None = None,
        user_address: str = "",
        market_id: str = "",
        side: str = "",
        price: float = 0.0,
        amount: float = 0.0,
        event_type: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> RawTransaction:
        raw = dict(self.log)
        if extra:
            raw.update(extra)
        return RawTransaction(
            hash=self.tx_hash,
            log_index=self.base_log_index if log_index is None else log_index,
            block_number=self.block.number,
            block_hash=self.block.hash,
            timestamp=self.block.timestamp,
            user_address=normalize_address(user_address) if user_address else "",
            market_id=str(market_id or ""),
            side=side,
            price=float(price or 0.0),
            amount=float(amount or 0.0),
            event_type=event_type or self.event_type,
            contract=self.contract,
            raw_json=json.dumps(raw, sort_keys=True),
        )


def _decode_order_filled_v2(context: _LogContext) -> RawTransaction:
    topics = context.topics
    words = decode_uint_words(str(context.log.get("data") or "0x"))
    order_hash = topics[1] if len(topics) > 1 else ""
    maker = topic_to_address(topics[2]) if len(topics) > 2 else ""
    taker = topic_to_address(topics[3]) if len(topics) > 3 else ""
    side_value = words[0] if len(words) > 0 else -1
    token_id = words[1] if len(words) > 1 else 0
    maker_amount = words[2] if len(words) > 2 else 0
    taker_amount = words[3] if len(words) > 3 else 0
    fee = words[4] if len(words) > 4 else 0
    builder = int_to_bytes32(words[5]) if len(words) > 5 else ""
    metadata = int_to_bytes32(words[6]) if len(words) > 6 else ""
    side = side_from_order_side(side_value)
    if side == "BUY":
        price = ratio(maker_amount, taker_amount)
        amount = scale_token_amount(taker_amount)
    elif side == "SELL":
        price = ratio(taker_amount, maker_amount)
        amount = scale_token_amount(maker_amount)
    else:
        price = 0.0
        amount = scale_token_amount(max(maker_amount, taker_amount))

    condition_id = context.tx_condition_id
    return context.row(
        user_address=maker,
        market_id=condition_id or str(token_id),
        side=side,
        price=price,
        amount=amount,
        event_type="OrderFilled",
        extra={
            "event_version": "v2",
            "order_hash": order_hash,
            "maker": maker,
            "taker": taker,
            "order_side": side_value,
            "condition_id": condition_id,
            "token_id": str(token_id),
            "maker_amount_filled": str(maker_amount),
            "taker_amount_filled": str(taker_amount),
            "fee": str(fee),
            "builder": builder,
            "metadata": metadata,
        },
    )


def _decode_orders_matched_v2(context: _LogContext) -> RawTransaction:
    topics = context.topics
    words = decode_uint_words(str(context.log.get("data") or "0x"))
    order_hash = topics[1] if len(topics) > 1 else ""
    maker = topic_to_address(topics[2]) if len(topics) > 2 else ""
    side_value = words[0] if len(words) > 0 else -1
    token_id = words[1] if len(words) > 1 else 0
    maker_amount = words[2] if len(words) > 2 else 0
    taker_amount = words[3] if len(words) > 3 else 0
    side = side_from_order_side(side_value)
    return context.row(
        user_address=maker,
        market_id=context.tx_condition_id or str(token_id),
        side=side,
        price=ratio(maker_amount, taker_amount) if side == "BUY" else ratio(taker_amount, maker_amount),
        amount=scale_token_amount(taker_amount if side == "BUY" else maker_amount),
        event_type="OrdersMatched",
        extra={
            "event_version": "v2",
            "order_hash": order_hash,
            "maker": maker,
            "order_side": side_value,
            "condition_id": context.tx_condition_id,
            "token_id": str(token_id),
            "maker_amount_filled": str(maker_amount),
            "taker_amount_filled": str(taker_amount),
        },
    )


def _decode_order_filled_v1(context: _LogContext) -> RawTransaction:
    topics = context.topics
    words = decode_uint_words(str(context.log.get("data") or "0x"))
    order_hash = topics[1] if len(topics) > 1 else ""
    maker = topic_to_address(topics[2]) if len(topics) > 2 else ""
    taker = topic_to_address(topics[3]) if len(topics) > 3 else ""
    maker_asset_id = words[0] if len(words) > 0 else 0
    taker_asset_id = words[1] if len(words) > 1 else 0
    maker_amount = words[2] if len(words) > 2 else 0
    taker_amount = words[3] if len(words) > 3 else 0
    fee = words[4] if len(words) > 4 else 0

    if maker_asset_id == USDC_ASSET_ID and taker_asset_id != USDC_ASSET_ID:
        side = "BUY"
        market_id = str(taker_asset_id)
        amount = scale_token_amount(taker_amount)
        price = ratio(maker_amount, taker_amount)
    elif taker_asset_id == USDC_ASSET_ID and maker_asset_id != USDC_ASSET_ID:
        side = "SELL"
        market_id = str(maker_asset_id)
        amount = scale_token_amount(maker_amount)
        price = ratio(taker_amount, maker_amount)
    else:
        side = "MATCH"
        market_id = str(maker_asset_id or taker_asset_id or order_hash)
        amount = scale_token_amount(max(maker_amount, taker_amount))
        price = ratio(min_nonzero(maker_amount, taker_amount), max(maker_amount, taker_amount))

    return context.row(
        user_address=maker,
        market_id=market_id,
        side=side,
        price=price,
        amount=amount,
        event_type="OrderFilled",
        extra={
            "event_version": "v1",
            "order_hash": order_hash,
            "maker": maker,
            "taker": taker,
            "maker_asset_id": str(maker_asset_id),
            "taker_asset_id": str(taker_asset_id),
            "maker_amount_filled": str(maker_amount),
            "taker_amount_filled": str(taker_amount),
            "fee": str(fee),
        },
    )


def _decode_order_cancelled(context: _LogContext) -> RawTransaction:
    order_hash = context.topics[1] if len(context.topics) > 1 else ""
    return context.row(market_id=order_hash, side="CANCEL", extra={"order_hash": order_hash})


def _decode_transfer_single(context: _LogContext) -> RawTransaction:
    topics = context.topics
    words = decode_uint_words(str(context.log.get("data") or "0x"))
    operator = topic_to_address(topics[1]) if len(topics) > 1 else ""
    from_address = topic_to_address(topics[2]) if len(topics) > 2 else ""
    to_address = topic_to_address(topics[3]) if len(topics) > 3 else ""
    token_id = words[0] if words else 0
    value = words[1] if len(words) > 1 else 0
    if from_address == ZERO_ADDRESS:
        side = "MINT"
        user_address = to_address
    elif to_address == ZERO_ADDRESS:
        side = "BURN"
        user_address = from_address
    else:
        side = "TRANSFER"
        user_address = to_address
    return context.row(
        user_address=user_address,
        market_id=str(token_id),
        side=side,
        amount=scale_token_amount(value),
        extra={
            "operator": operator,
            "from": from_address,
            "to": to_address,
            "token_id": str(token_id),
            "value": str(value),
        },
    )


def _decode_transfer_batch(context: _LogContext) -> list[RawTransaction]:
    topics = context.topics
    operator = topic_to_address(topics[1]) if len(topics) > 1 else ""
    from_address = topic_to_address(topics[2]) if len(topics) > 2 else ""
    to_address = topic_to_address(topics[3]) if len(topics) > 3 else ""
    ids, values = decode_two_uint_arrays(str(context.log.get("data") or "0x"))
    rows: list[RawTransaction] = []
    if from_address == ZERO_ADDRESS:
        side = "MINT"
        user_address = to_address
    elif to_address == ZERO_ADDRESS:
        side = "BURN"
        user_address = from_address
    else:
        side = "TRANSFER"
        user_address = to_address
    for index, token_id in enumerate(ids):
        value = values[index] if index < len(values) else 0
        rows.append(
            context.row(
                log_index=compose_log_index(context.base_log_index, index),
                user_address=user_address,
                market_id=str(token_id),
                side=side,
                amount=scale_token_amount(value),
                extra={
                    "operator": operator,
                    "from": from_address,
                    "to": to_address,
                    "token_id": str(token_id),
                    "value": str(value),
                    "batch_index": index,
                },
            )
        )
    return rows


def _decode_condition_preparation(context: _LogContext) -> RawTransaction:
    topics = context.topics
    words = decode_uint_words(str(context.log.get("data") or "0x"))
    condition_id = topics[1] if len(topics) > 1 else ""
    oracle = topic_to_address(topics[2]) if len(topics) > 2 else ""
    question_id = topics[3] if len(topics) > 3 else ""
    outcome_slot_count = words[0] if words else 0
    return context.row(
        user_address=oracle,
        market_id=condition_id,
        side="PREPARE",
        amount=float(outcome_slot_count),
        extra={
            "condition_id": condition_id,
            "oracle": oracle,
            "question_id": question_id,
            "outcome_slot_count": outcome_slot_count,
        },
    )


def _decode_payout_redemption(context: _LogContext) -> RawTransaction:
    topics = context.topics
    words = decode_uint_words(str(context.log.get("data") or "0x"))
    redeemer = topic_to_address(topics[1]) if len(topics) > 1 else ""
    collateral_token = topic_to_address(topics[2]) if len(topics) > 2 else ""
    parent_collection_id = topics[3] if len(topics) > 3 else ""
    condition_id = int_to_bytes32(words[0]) if words else ""
    payout = words[2] if len(words) > 2 else 0
    return context.row(
        user_address=redeemer,
        market_id=condition_id,
        side="REDEEM",
        amount=scale_token_amount(payout),
        extra={
            "redeemer": redeemer,
            "collateral_token": collateral_token,
            "parent_collection_id": parent_collection_id,
            "condition_id": condition_id,
            "payout": str(payout),
        },
    )


def _decode_generic_order_placed(context: _LogContext) -> RawTransaction:
    user_address = topic_to_address(context.topics[1]) if len(context.topics) > 1 else ""
    words = decode_uint_words(str(context.log.get("data") or "0x"))
    market_id = str(words[0]) if words else ""
    amount = scale_token_amount(words[1]) if len(words) > 1 else 0.0
    price = ratio(words[2], words[1]) if len(words) > 2 else 0.0
    return context.row(
        user_address=user_address,
        market_id=market_id,
        side="PLACE",
        amount=amount,
        price=price,
        extra={"decoded_words": [str(word) for word in words]},
    )


def default_contract_specs() -> list[ContractSpec]:
    legacy_exchange_events = ("OrderFilledV1", "OrderCancelled", "OrderPlaced")
    exchange_events = ("OrderFilledV2", "OrdersMatchedV2", "OrderCancelled", "OrderPlaced")
    return [
        ContractSpec(
            name="ConditionalTokens",
            address=CONDITIONAL_TOKENS_ADDRESS,
            event_names=(
                "TransferSingle",
                "TransferBatch",
                "ConditionPreparation",
                "PayoutRedemption",
            ),
        ),
        ContractSpec(
            name="LegacyCLOBExchange",
            address=LEGACY_CTF_EXCHANGE_ADDRESS,
            event_names=legacy_exchange_events,
        ),
        ContractSpec(
            name="LegacyNegRiskCLOBExchange",
            address=LEGACY_NEG_RISK_CTF_EXCHANGE_ADDRESS,
            event_names=legacy_exchange_events,
        ),
        ContractSpec(name="CLOBExchange", address=CTF_EXCHANGE_ADDRESS, event_names=exchange_events),
        ContractSpec(
            name="NegRiskCLOBExchange",
            address=NEG_RISK_CTF_EXCHANGE_ADDRESS,
            event_names=exchange_events,
        ),
    ]


def topics_for_contract(contract: ContractSpec) -> list[str]:
    topics: list[str] = []
    for event_name in contract.event_names:
        topic = topic_for_event(event_name)
        if topic:
            topics.append(topic)
    return topics


def build_topic_map(contracts: Sequence[ContractSpec]) -> dict[str, str]:
    topic_map: dict[str, str] = {}
    for contract in contracts:
        for event_name in contract.event_names:
            topic = topic_for_event(event_name)
            if topic:
                topic_map[topic] = event_name
    return topic_map


def topic_for_event(event_name: str) -> str | None:
    fallback = EVENT_TOPIC_FALLBACKS.get(event_name)
    if fallback:
        return fallback.lower()
    signature = EVENT_SIGNATURES.get(event_name)
    if not signature:
        return None
    try:
        return keccak_topic(signature)
    except MissingIndexerDependencyError:
        LOGGER.debug(
            "Cannot compute topic for %s. Install eth-hash[pycryptodome] or web3.",
            event_name,
        )
        return None


def function_selector(signature: str) -> str | None:
    try:
        return keccak_topic(signature)[:10]
    except MissingIndexerDependencyError:
        return None


def keccak_topic(signature: str) -> str:
    data = signature.encode("utf-8")
    try:
        from eth_hash.auto import keccak  # type: ignore

        return "0x" + keccak(data).hex()
    except ModuleNotFoundError:
        pass
    try:
        from Crypto.Hash import keccak as crypto_keccak  # type: ignore

        hasher = crypto_keccak.new(digest_bits=256)
        hasher.update(data)
        return "0x" + hasher.hexdigest()
    except ModuleNotFoundError:
        pass
    try:
        from web3 import Web3  # type: ignore

        return Web3.keccak(text=signature).hex()
    except ModuleNotFoundError as exc:
        raise MissingIndexerDependencyError(
            "Keccak topic calculation requires eth-hash[pycryptodome] or web3."
        ) from exc


def chunk_ranges(start_block: int, end_block: int, chunk_size: int) -> list[tuple[int, int]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if start_block > end_block:
        return []
    ranges: list[tuple[int, int]] = []
    cursor = start_block
    while cursor <= end_block:
        chunk_end = min(end_block, cursor + chunk_size - 1)
        ranges.append((cursor, chunk_end))
        cursor = chunk_end + 1
    return ranges


def batched(items: Sequence[RawTransaction], batch_size: int) -> list[Sequence[RawTransaction]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def first_topic(log: dict[str, Any]) -> str:
    topics = log.get("topics", [])
    if not topics:
        return ""
    return str(topics[0]).lower()


def is_log_range_error(error: Any) -> bool:
    text = json.dumps(error, sort_keys=True).lower() if isinstance(error, dict) else str(error).lower()
    markers = (
        "too many",
        "response size",
        "block range",
        "more than",
        "limit exceeded",
        "query returned more",
        "log response size",
    )
    return any(marker in text for marker in markers)


def summarize_rpc_response(response: Any) -> str:
    try:
        body = response.json()
    except Exception:  # noqa: BLE001 - response body is best-effort context.
        body = getattr(response, "text", "")
    if isinstance(body, dict):
        error = body.get("error", body)
        return json.dumps(error, sort_keys=True)[:500]
    return str(body)[:500]


def hex_to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    text = str(value)
    if not text:
        return 0
    return int(text, 16) if text.startswith("0x") else int(text)


def normalize_address(value: str) -> str:
    value = str(value or "").strip().lower()
    if not value:
        return ""
    if not value.startswith("0x"):
        value = "0x" + value
    return value


def topic_to_address(topic: str) -> str:
    topic = normalize_address(topic)
    if len(topic) < 42:
        return ""
    return "0x" + topic[-40:]


def decode_address_return(value: str) -> str:
    value = normalize_address(value)
    if len(value) < 42:
        return ""
    return "0x" + value[-40:]


def is_nonzero_eth_call_result(value: str) -> bool:
    value = str(value or "0x").lower()
    return value not in ("0x", "0x0") and bool(value.replace("0x", "").strip("0"))


def condition_id_from_tx_input(data: str) -> str:
    data = str(data or "")
    if not data.startswith("0x") or len(data) < 74:
        return ""
    return "0x" + data[10:74].lower()


def decode_uint_words(data: str) -> list[int]:
    data = data[2:] if data.startswith("0x") else data
    if not data:
        return []
    words: list[int] = []
    for offset in range(0, len(data), 64):
        chunk = data[offset : offset + 64]
        if len(chunk) == 64:
            words.append(int(chunk, 16))
    return words


def decode_two_uint_arrays(data: str) -> tuple[list[int], list[int]]:
    words = decode_uint_words(data)
    if len(words) < 2:
        return [], []
    first_offset = words[0] // 32
    second_offset = words[1] // 32
    return decode_uint_array_from_words(words, first_offset), decode_uint_array_from_words(
        words, second_offset
    )


def decode_uint_array_from_words(words: Sequence[int], offset_words: int) -> list[int]:
    if offset_words < 0 or offset_words >= len(words):
        return []
    length = words[offset_words]
    start = offset_words + 1
    end = min(len(words), start + length)
    return [int(word) for word in words[start:end]]


def int_to_bytes32(value: int) -> str:
    return "0x" + int(value).to_bytes(32, byteorder="big").hex()


def scale_token_amount(value: int) -> float:
    return float(value) / TOKEN_DECIMALS


def ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def side_from_order_side(value: int) -> str:
    if value == 0:
        return "BUY"
    if value == 1:
        return "SELL"
    return "UNKNOWN"


def min_nonzero(left: int, right: int) -> int:
    values = [value for value in (left, right) if value > 0]
    return min(values) if values else 0


def compose_log_index(log_index: int, sub_index: int) -> int:
    return (log_index * 100_000) + sub_index


def raw_transaction_to_dict(row: RawTransaction) -> dict[str, Any]:
    return {
        "hash": row.hash,
        "log_index": row.log_index,
        "block_number": row.block_number,
        "block_hash": row.block_hash,
        "timestamp": row.timestamp,
        "user_address": row.user_address,
        "market_id": row.market_id,
        "side": row.side,
        "price": row.price,
        "amount": row.amount,
        "event_type": row.event_type,
        "contract": row.contract,
        "raw_json": row.raw_json,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Index Polymarket Polygon contract events into raw_transactions."
    )
    parser.add_argument("--rpc-url", default=os.getenv("POLYGON_RPC_URL", ""))
    parser.add_argument(
        "--db",
        default=os.getenv(
            "INDEXER_DB_PATH",
            os.getenv("POLYSIGNAL_DB", str(DEFAULT_INDEXER_DB_PATH)),
        ),
        help="SQLite database path. Defaults to INDEXER_DB_PATH or data/indexer.db.",
    )
    parser.add_argument("--start-block", type=int, default=None)
    parser.add_argument("--end-block", type=int, default=None)
    parser.add_argument("--sync", action="store_true", help="Continuously index new blocks.")
    parser.add_argument("--dry-run", action="store_true", help="Print decoded rows without writing.")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Index one block in dry-run mode and print up to 10 decoded events.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("BATCH_SIZE", "1000")),
        help="Reserved insert batch size for future ClickHouse/stream sinks.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.getenv("MAX_WORKERS", "5")),
    )
    parser.add_argument(
        "--rpc-rps",
        type=float,
        default=float(os.getenv("RPC_RPS", "25")),
        help="Max JSON-RPC request starts per second. Use 0 to disable throttling.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=int(os.getenv("CHUNK_SIZE", os.getenv("BLOCK_CHUNK_SIZE", "100"))),
    )
    parser.add_argument("--poll-seconds", type=float, default=12.0)
    parser.add_argument(
        "--min-log-chunk-size",
        type=int,
        default=int(os.getenv("MIN_LOG_CHUNK_SIZE", "5")),
        help="Smallest block range after adaptive eth_getLogs splitting.",
    )
    parser.add_argument(
        "--default-start-block",
        type=int,
        default=int(os.getenv("POLYGON_START_BLOCK", "0")),
        help="Start block used when the database has no checkpoint.",
    )
    parser.add_argument(
        "--skip-contract-check",
        action="store_true",
        help="Skip startup bytecode/getCtf()/owner()/getOracle() contract probes.",
    )
    parser.add_argument(
        "--sample-log-limit",
        type=int,
        default=int(os.getenv("ORDER_FILLED_SAMPLE_LIMIT", "5")),
        help="Number of decoded OrderFilled samples to log for side/market debugging.",
    )
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    return parser


def config_from_args(args: argparse.Namespace) -> IndexerConfig:
    return IndexerConfig(
        rpc_url=str(args.rpc_url or ""),
        db_path=Path(args.db),
        start_block=args.start_block,
        end_block=args.end_block,
        sync=bool(args.sync),
        dry_run=bool(args.dry_run),
        batch_size=int(args.batch_size),
        max_workers=max(1, int(args.max_workers)),
        block_chunk_size=max(1, int(args.chunk_size)),
        min_log_chunk_size=max(1, int(args.min_log_chunk_size)),
        poll_seconds=float(args.poll_seconds),
        default_start_block=max(0, int(args.default_start_block)),
        rpc_rps=max(0.0, float(args.rpc_rps)),
        verify_contracts=not bool(args.skip_contract_check),
        test=bool(args.test),
        dry_run_print_limit=10 if bool(args.test) else None,
        sample_log_limit=max(0, int(args.sample_log_limit)),
    )


async def async_main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    config = config_from_args(args)
    indexer = PolygonEventIndexer(config)
    try:
        await indexer.run()
    finally:
        indexer.store.close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        LOGGER.info("Indexer stopped by user")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
