from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from typing import Any

from .api import ApiError, PolymarketClient
from .models import Trade
from .storage import Store


@dataclass(frozen=True)
class MarketFlowConfig:
    market_limit: int = 25
    market_offset: int = 0
    market_order: str = "volume24hr"
    market_ascending: bool = False
    min_market_volume: float = 1000.0
    min_market_liquidity: float = 250.0
    trades_per_market: int = 200
    min_trade_cash: float = 25.0
    taker_only: bool = True
    promote_wallets: bool = True
    wallet_limit: int = 5000
    min_wallet_notional: float = 100.0
    min_wallet_trades: int = 2
    wallet_since_days: int = 30
    stop_on_error: bool = False


def sync_market_flow(
    store: Store,
    *,
    client: PolymarketClient | None = None,
    config: MarketFlowConfig | None = None,
) -> dict[str, Any]:
    client = client or PolymarketClient()
    config = config or MarketFlowConfig()
    started = time.time()
    markets = fetch_market_rows(client, config)
    seen = 0
    inserted = 0
    queried = 0
    skipped = 0
    errors = 0
    error_samples: list[str] = []

    for market in markets:
        condition_id = market_condition_id(market)
        if not condition_id:
            skipped += 1
            continue
        queried += 1
        try:
            rows = client.trades(
                market=condition_id,
                limit=config.trades_per_market,
                taker_only=config.taker_only,
                filter_type="CASH" if config.min_trade_cash > 0 else None,
                filter_amount=config.min_trade_cash if config.min_trade_cash > 0 else None,
            )
        except ApiError as exc:
            errors += 1
            error_samples.append(f"{condition_id[:10]} {str(exc)[:120]}")
            if config.stop_on_error:
                raise
            continue
        trade_rows = _as_rows(rows)
        seen += len(trade_rows)
        trades = [
            Trade.from_api(_with_market_context(row, market, condition_id), source="market-flow")
            for row in trade_rows
        ]
        inserted += store.insert_trades(trades)

    promoted = 0
    if config.promote_wallets:
        since_ts = int(time.time()) - max(1, config.wallet_since_days) * 86400
        wallets = store.discover_wallets_from_trades(
            limit=config.wallet_limit,
            min_notional=config.min_wallet_notional,
            min_trades=config.min_wallet_trades,
            since_ts=since_ts,
            source="market-flow",
        )
        promoted = store.insert_wallets_ignore_existing(wallets)

    summary = {
        "market_rows": len(markets),
        "markets_queried": queried,
        "markets_skipped": skipped,
        "trades_seen": seen,
        "trades_inserted": inserted,
        "wallets_promoted": promoted,
        "errors": errors,
        "error_samples": error_samples[:5],
        "elapsed_seconds": round(time.time() - started, 3),
        "config": asdict(config),
    }
    store.set_runtime_state("market_flow_last_seen", str(int(time.time())))
    store.set_runtime_state("market_flow_last_summary", format_market_flow_summary(summary))
    return summary


def fetch_market_rows(client: PolymarketClient, config: MarketFlowConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    remaining = max(0, config.market_limit)
    offset = max(0, config.market_offset)
    while remaining > 0:
        page_limit = min(100, remaining)
        page = client.markets(
            limit=page_limit,
            offset=offset,
            closed=False,
            order=config.market_order,
            ascending=config.market_ascending,
            volume_num_min=config.min_market_volume,
            liquidity_num_min=config.min_market_liquidity,
        )
        page_rows = _as_rows(page)
        rows.extend(page_rows)
        fetched = len(page_rows)
        if fetched < page_limit:
            break
        remaining -= fetched
        offset += fetched
    return rows[: config.market_limit]


def market_condition_id(row: dict[str, Any]) -> str:
    for key in ("conditionId", "condition_id", "conditionID"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    for key in ("conditionIds", "condition_ids"):
        values = _json_list(row.get(key))
        if values:
            return str(values[0]).strip()
    return ""


def format_market_flow_summary(summary: dict[str, Any]) -> str:
    return (
        f"market_flow markets={summary['markets_queried']} "
        f"trades={summary['trades_inserted']}/{summary['trades_seen']} "
        f"wallets={summary['wallets_promoted']} errors={summary['errors']}"
    )


def _with_market_context(row: dict[str, Any], market: dict[str, Any], condition_id: str) -> dict[str, Any]:
    item = dict(row)
    item.setdefault("conditionId", condition_id)
    item.setdefault("title", market_title(market))
    item.setdefault("slug", str(market.get("slug") or ""))
    item.setdefault("eventSlug", str(market.get("eventSlug") or market.get("event_slug") or ""))
    return item


def market_title(row: dict[str, Any]) -> str:
    return str(row.get("question") or row.get("title") or row.get("slug") or "")


def _as_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("data", "results", "trades", "markets"):
            rows = value.get(key)
            if isinstance(rows, list):
                return [item for item in rows if isinstance(item, dict)]
    return []


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(parsed, list):
        return parsed
    return []
