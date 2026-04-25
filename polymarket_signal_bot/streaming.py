from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib
import importlib.util
import json
import time
from dataclasses import dataclass, field
from typing import Any

from .api import PolymarketClient
from .models import OrderBookSnapshot, Trade
from .monitor import Monitor, MonitorConfig
from .storage import Store


MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class MissingWebsocketError(RuntimeError):
    pass


@dataclass(frozen=True)
class StreamConfig:
    assets: tuple[str, ...] = field(default_factory=tuple)
    asset_limit: int = 40
    lookback_minutes: int = 24 * 60
    market_ws_url: str = MARKET_WS_URL
    custom_feature_enabled: bool = True
    queue_size: int = 1000
    batch_size: int = 50
    flush_interval_seconds: float = 1.0
    heartbeat_seconds: int = 10
    reconnect_seconds: int = 5
    max_events: int = 0
    reconcile_trades: bool = True
    reconcile_min_notional: float = 50.0
    reconcile_limit: int = 30
    reconcile_window_seconds: int = 45
    scan_every_events: int = 0
    wallet_limit: int = 100
    bankroll: float = 200.0
    min_wallet_score: float = 0.55
    min_trade_usdc: float = 50.0
    max_signals: int = 20


@dataclass
class StreamFlushResult:
    stream_events: int = 0
    books: int = 0
    reconciled_trades: int = 0
    scans: int = 0


class StreamProcessor:
    def __init__(
        self,
        store: Store,
        *,
        client: PolymarketClient | None = None,
        config: StreamConfig | None = None,
    ) -> None:
        self.store = store
        self.client = client or PolymarketClient()
        self.config = config or StreamConfig()
        self._events_since_scan = 0

    def run(self) -> None:
        asyncio.run(self.run_async())

    async def run_async(self) -> None:
        self.store.init_schema()
        assets = self._subscription_assets()
        if not assets:
            raise RuntimeError("No assets available for CLOB stream subscription.")
        self.store.set_runtime_state("stream_status", "starting")
        self.store.set_runtime_state("stream_assets", str(len(assets)))
        queue: asyncio.Queue[dict[str, object] | None] = asyncio.Queue(maxsize=self.config.queue_size)
        while True:
            try:
                self.store.set_runtime_state("stream_status", "connecting")
                await asyncio.gather(
                    self._produce(assets, queue),
                    self._consume(queue),
                )
                self.store.set_runtime_state("stream_status", "stopped")
                return
            except MissingWebsocketError:
                self.store.set_runtime_state("stream_status", "missing_dependency")
                raise
            except Exception as exc:  # noqa: BLE001 - stream should reconnect.
                self.store.set_runtime_state("stream_status", "reconnecting")
                self.store.set_runtime_state("stream_last_error", str(exc)[:300])
                if self.config.max_events:
                    raise
                await asyncio.sleep(max(1, self.config.reconnect_seconds))

    async def _produce(
        self,
        assets: list[str],
        queue: asyncio.Queue[dict[str, object] | None],
    ) -> None:
        websockets = _load_websockets()
        subscription = {
            "assets_ids": assets,
            "type": "market",
            "custom_feature_enabled": self.config.custom_feature_enabled,
        }
        seen = 0
        async with websockets.connect(self.config.market_ws_url, ping_interval=None) as ws:
            await ws.send(json.dumps(subscription, separators=(",", ":")))
            self.store.set_runtime_state("stream_status", "listening")
            heartbeat_task = asyncio.create_task(self._heartbeat(ws))
            try:
                async for raw in ws:
                    for event in normalize_market_payload(raw):
                        await queue.put(event)
                        seen += 1
                        if self.config.max_events and seen >= self.config.max_events:
                            await queue.put(None)
                            return
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def _heartbeat(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(max(1, self.config.heartbeat_seconds))
            await ws.send("PING")

    async def _consume(self, queue: asyncio.Queue[dict[str, object] | None]) -> None:
        pending: list[dict[str, object]] = []
        last_flush = time.monotonic()
        while True:
            timeout = max(0.1, self.config.flush_interval_seconds)
            try:
                event = await asyncio.wait_for(queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                event = {}
            if event is None:
                if pending:
                    await asyncio.to_thread(self._flush, pending)
                return
            elif event:
                pending.append(event)
            else:
                pass
            age = time.monotonic() - last_flush
            if pending and (len(pending) >= self.config.batch_size or age >= self.config.flush_interval_seconds):
                await asyncio.to_thread(self._flush, pending)
                pending = []
                last_flush = time.monotonic()

    def _flush(self, events: list[dict[str, object]]) -> StreamFlushResult:
        if not events:
            return StreamFlushResult()
        result = StreamFlushResult()
        result.stream_events = self.store.insert_stream_events(events)
        books = [_book_from_stream_event(event) for event in events]
        books = [book for book in books if book is not None]
        if books:
            result.books = self.store.upsert_order_books(books, raw_by_asset={book.asset: json.loads(book.raw_json) for book in books})
        if self.config.reconcile_trades:
            result.reconciled_trades = self._reconcile_trade_events(events)
        self._events_since_scan += len(events)
        if self.config.scan_every_events > 0 and self._events_since_scan >= self.config.scan_every_events:
            self._events_since_scan = 0
            Monitor(
                store=self.store,
                client=self.client,
                config=MonitorConfig(
                    live_discover=False,
                    live_sync=False,
                    sync_books=False,
                    wallet_limit=self.config.wallet_limit,
                    bankroll=self.config.bankroll,
                    min_wallet_score=self.config.min_wallet_score,
                    min_trade_usdc=self.config.min_trade_usdc,
                    lookback_minutes=self.config.lookback_minutes,
                    max_signals=self.config.max_signals,
                ),
            ).scan()
            result.scans = 1
        self.store.set_runtime_state("stream_last_seen", str(int(time.time())))
        self.store.set_runtime_state(
            "stream_last_summary",
            (
                f"events={result.stream_events} books={result.books} "
                f"reconciled={result.reconciled_trades} scans={result.scans}"
            ),
        )
        return result

    def _reconcile_trade_events(self, events: list[dict[str, object]]) -> int:
        inserted = 0
        for event in events:
            if str(event.get("event_type") or "") != "last_trade_price":
                continue
            if float(event.get("notional") or 0.0) < self.config.reconcile_min_notional:
                continue
            market = str(event.get("market") or "")
            if not market:
                continue
            rows = self.client.trades(
                market=market,
                limit=self.config.reconcile_limit,
                filter_type="CASH",
                filter_amount=max(1.0, self.config.reconcile_min_notional),
            )
            matches = _matching_trade_rows(rows, event, self.config.reconcile_window_seconds)
            inserted += self.store.insert_trades(Trade.from_api(row, source="clob-stream-reconcile") for row in matches)
        return inserted

    def _subscription_assets(self) -> list[str]:
        explicit = [asset for asset in self.config.assets if asset]
        if explicit:
            return list(dict.fromkeys(explicit))
        since_ts = int(time.time()) - max(1, self.config.lookback_minutes) * 60
        return self.store.recent_assets(limit=self.config.asset_limit, since_ts=since_ts)


def websocket_available() -> bool:
    return importlib.util.find_spec("websockets") is not None


def missing_websocket_message() -> str:
    return (
        "WebSocket streaming requires the optional 'websockets' package. "
        "Install it with: python -m pip install websockets"
    )


def normalize_market_payload(raw: str | bytes | dict[str, Any] | list[Any]) -> list[dict[str, object]]:
    payload = _decode_payload(raw)
    received_at = int(time.time())
    return [_normalize_event(item, received_at) for item in _iter_events(payload)]


def _decode_payload(raw: str | bytes | dict[str, Any] | list[Any]) -> Any:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        if raw in {"PONG", "PING", "pong", "ping"}:
            return []
        return json.loads(raw)
    return raw


def _iter_events(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        events: list[dict[str, Any]] = []
        for item in payload:
            events.extend(_iter_events(item))
        return events
    if not isinstance(payload, dict):
        return []
    event_type = str(payload.get("event_type") or payload.get("type") or "")
    if event_type == "price_change" and isinstance(payload.get("price_changes"), list):
        rows = []
        for change in payload["price_changes"]:
            if not isinstance(change, dict):
                continue
            merged = {**payload, **change}
            merged.pop("price_changes", None)
            merged["event_type"] = "price_change"
            rows.append(merged)
        return rows
    return [payload]


def _normalize_event(payload: dict[str, Any], received_at: int) -> dict[str, object]:
    event_type = str(payload.get("event_type") or payload.get("type") or "unknown")
    price = _float(payload.get("price") or payload.get("last_trade_price"))
    size = _float(payload.get("size"))
    raw_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    event = {
        "received_at": received_at,
        "event_ts": _event_ts(payload.get("timestamp")),
        "event_type": event_type,
        "source": "clob-market-ws",
        "market": str(payload.get("market") or payload.get("condition_id") or ""),
        "asset": str(payload.get("asset_id") or payload.get("asset") or ""),
        "side": str(payload.get("side") or "").upper(),
        "price": price,
        "size": size,
        "notional": max(0.0, price * size),
        "raw_json": raw_json,
        "processed_at": received_at,
    }
    event["event_id"] = _event_id(event)
    return event


def _book_from_stream_event(event: dict[str, object]) -> OrderBookSnapshot | None:
    if str(event.get("event_type") or "") != "book":
        return None
    try:
        payload = json.loads(str(event.get("raw_json") or "{}"))
    except json.JSONDecodeError:
        return None
    if not payload.get("asset_id"):
        return None
    book = OrderBookSnapshot.from_api(payload)
    return OrderBookSnapshot(
        asset=book.asset,
        market=book.market,
        timestamp=_event_ts(payload.get("timestamp")),
        best_bid=book.best_bid,
        best_ask=book.best_ask,
        mid=book.mid,
        spread=book.spread,
        bid_depth_usdc=book.bid_depth_usdc,
        ask_depth_usdc=book.ask_depth_usdc,
        liquidity_score=book.liquidity_score,
        last_trade_price=book.last_trade_price,
        raw_json=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    )


def _matching_trade_rows(rows: list[dict[str, Any]], event: dict[str, object], window_seconds: int) -> list[dict[str, Any]]:
    asset = str(event.get("asset") or "")
    side = str(event.get("side") or "").upper()
    event_ts = int(event.get("event_ts") or 0)
    matches = []
    for row in rows:
        row_asset = str(row.get("asset") or row.get("asset_id") or "")
        row_side = str(row.get("side") or "").upper()
        row_ts = _event_ts(row.get("timestamp") or row.get("match_time") or row.get("last_update"))
        if asset and row_asset and row_asset != asset:
            continue
        if side and row_side and row_side != side:
            continue
        if event_ts and row_ts and abs(row_ts - event_ts) > max(1, window_seconds):
            continue
        matches.append(row)
    return matches or rows[:1]


def _event_id(event: dict[str, object]) -> str:
    raw = "|".join(
        [
            str(event.get("source") or ""),
            str(event.get("event_type") or ""),
            str(event.get("market") or ""),
            str(event.get("asset") or ""),
            str(event.get("event_ts") or ""),
            str(event.get("side") or ""),
            f"{float(event.get('price') or 0.0):.8f}",
            f"{float(event.get('size') or 0.0):.8f}",
            str(event.get("raw_json") or ""),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _event_ts(value: Any) -> int:
    timestamp = int(_float(value))
    if timestamp > 10_000_000_000:
        return timestamp // 1000
    return timestamp


def _float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _load_websockets() -> Any:
    try:
        return importlib.import_module("websockets")
    except ImportError as exc:
        raise MissingWebsocketError(missing_websocket_message()) from exc

