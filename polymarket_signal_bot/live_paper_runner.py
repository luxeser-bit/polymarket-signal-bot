from __future__ import annotations

"""Async real-time paper runner for PolySignal.

The project package is named ``polymarket_signal_bot`` rather than ``src``. This
module is the requested live-paper runner layer and can be started directly:

    python -m polymarket_signal_bot.live_paper_runner --db data/polysignal.db

Typical paper-only run:

    python -m polymarket_signal_bot live-paper --poll-interval 60 --price-interval 15

Manual confirmation:

    python -m polymarket_signal_bot live-paper --manual-confirm

Dry run:

    python -m polymarket_signal_bot live-paper --dry-run

Stream queue mode:

    python -m polymarket_signal_bot stream --asset-limit 80 --reconcile-min-notional 100
    python -m polymarket_signal_bot live-paper --use-stream-queue --dry-run

Direct WebSocket mode:

    python -m polymarket_signal_bot live-paper --use-websocket --dry-run

Standalone external-signal mode:

    python -m polymarket_signal_bot live-paper --monitor-standalone --dry-run

Runner state is mirrored into ``data/paper_state.db``. JSON state is an optional
debug export that can be enabled with ``--export-json-state``.

Confidential values, if a future authenticated API client is added, should come
from environment variables such as ``POLYMARKET_API_KEY``,
``POLYMARKET_API_SECRET`` and ``POLYMARKET_API_PASSPHRASE``. The current runner
uses public market data only and never stores private keys.
"""

import argparse
import asyncio
import contextlib
import hashlib
import importlib
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterator

from .api import ApiError, PolymarketClient
from .consensus_engine import (
    ConsensusConfig,
    consensus_filter_signals,
    consensus_runtime_summary,
    generate_strategy_signals,
    persist_consensus_decisions,
)
from .cohorts import CohortConfig, wallet_cohort_report
from .learning import wallet_outcome_lookup, wallet_outcome_report
from .models import PaperEvent, PaperPosition, Signal, Wallet
from .monitor import Monitor, MonitorConfig
from .paper import ExitConfig, PaperBroker, RiskConfig
from .policy_optimizer import policy_settings_from_recommendation
from .scoring import score_wallets
from .signals import SignalConfig
from .storage import DEFAULT_DB_PATH, Store
from .streaming import (
    MARKET_WS_URL,
    MissingWebsocketError,
    StreamConfig,
    StreamProcessor,
    missing_websocket_message,
    normalize_market_payload,
)


LOGGER = logging.getLogger("polymarket_signal_bot.live_paper")


@dataclass(frozen=True)
class LivePaperConfig:
    db_path: str = str(DEFAULT_DB_PATH)
    poll_interval_seconds: int = 60
    price_interval_seconds: int = 15
    monitor_standalone: bool = False
    external_signal_channel: str = "polysignal:signals"
    use_websocket: bool = False
    websocket_url: str = MARKET_WS_URL
    websocket_assets: tuple[str, ...] = ()
    websocket_asset_limit: int = 40
    websocket_timeout_seconds: float = 30.0
    websocket_reconnect_seconds: float = 5.0
    use_stream_queue: bool = False
    stream_queue_interval_seconds: float = 1.0
    stream_batch_limit: int = 500
    stream_min_events: int = 1
    manual_confirm: bool = False
    dry_run: bool = False
    close_on_stop: bool = True
    state_db_path: str = "data/paper_state.db"
    export_json_state: bool = False
    state_path: str = "data/live_paper_state.json"
    log_path: str = "data/live_paper_runner.log"
    leaderboard_limit: int = 0
    discover_every: int = 10
    wallet_limit: int = 100
    days: int = 7
    per_wallet_limit: int = 150
    bankroll: float = 200.0
    min_wallet_score: float = 0.55
    min_trade_usdc: float = 50.0
    lookback_minutes: int = 120
    max_signals: int = 20
    book_asset_limit: int = 40
    max_spread: float = 0.08
    min_liquidity_score: float = 0.10
    min_depth_usdc: float = 25.0
    max_book_price_deviation: float = 0.12
    max_wallet_trades_per_day: float = 60.0
    min_cluster_wallets: int = 1
    min_cluster_notional: float = 0.0
    use_cohort_policy: bool = True
    use_learning_policy: bool = True
    stop_loss_pct: float = 0.28
    take_profit_pct: float = 0.40
    max_total_exposure_pct: float = 0.65
    max_open_positions: int = 20
    max_new_positions_per_run: int = 6
    max_market_exposure_usdc: float = 40.0
    max_wallet_exposure_usdc: float = 60.0
    max_daily_loss_usdc: float = 20.0
    max_worst_stop_loss_usdc: float = 35.0
    paper_max_hold_hours: int = 36
    paper_stale_price_hours: int = 48
    max_risk_trim_per_run: int = 4
    risk_trim_enabled: bool = True


class PaperStateStore:
    """Small SQLite state mirror for the live paper runner.

    The main application database remains the source of market data, signals,
    and the full paper journal. This store is a runner-local recovery surface:
    open/closed position rows plus balance history after each cycle.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def init_schema(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    size REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    tp_pct REAL NOT NULL,
                    sl_pct REAL NOT NULL,
                    status TEXT NOT NULL,
                    opened_at INTEGER NOT NULL,
                    closed_at INTEGER,
                    pnl REAL NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS balance_history (
                    timestamp INTEGER NOT NULL,
                    balance REAL NOT NULL,
                    pnl REAL NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status, opened_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_balance_timestamp ON balance_history(timestamp DESC)")

    def load_latest_state(self) -> dict[str, Any]:
        self.init_schema()
        with self._connect() as conn:
            open_positions = conn.execute(
                "SELECT * FROM positions WHERE status = 'OPEN' ORDER BY opened_at DESC"
            ).fetchall()
            latest_balance = conn.execute(
                """
                SELECT timestamp, balance, pnl
                FROM balance_history
                ORDER BY timestamp DESC
                LIMIT 1
                """
            ).fetchone()
            total_positions = conn.execute("SELECT COUNT(*) AS count FROM positions").fetchone()
        return {
            "open_positions": [dict(row) for row in open_positions],
            "latest_balance": dict(latest_balance) if latest_balance else None,
            "total_positions": int(total_positions["count"] or 0) if total_positions else 0,
        }

    def upsert_open_position(self, position: PaperPosition) -> None:
        self.init_schema()
        row = _state_position_row(position)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO positions(
                    id, market_id, side, size, entry_price, tp_pct, sl_pct,
                    status, opened_at, closed_at, pnl
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    market_id = excluded.market_id,
                    side = excluded.side,
                    size = excluded.size,
                    entry_price = excluded.entry_price,
                    tp_pct = excluded.tp_pct,
                    sl_pct = excluded.sl_pct,
                    status = excluded.status,
                    opened_at = excluded.opened_at,
                    closed_at = excluded.closed_at,
                    pnl = excluded.pnl
                """,
                row,
            )

    def close_position(self, position: PaperPosition, *, closed_at: int, exit_price: float) -> None:
        self.init_schema()
        pnl = round(position.shares * exit_price - position.size_usdc, 4)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO positions(
                    id, market_id, side, size, entry_price, tp_pct, sl_pct,
                    status, opened_at, closed_at, pnl
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _state_position_row(position),
            )
            conn.execute(
                """
                UPDATE positions
                SET status = 'CLOSED', closed_at = ?, pnl = ?
                WHERE id = ?
                """,
                (closed_at, pnl, position.position_id),
            )

    def record_balance(self, *, timestamp: int, balance: float, pnl: float) -> None:
        self.init_schema()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO balance_history(timestamp, balance, pnl) VALUES (?, ?, ?)",
                (timestamp, round(float(balance), 4), round(float(pnl), 4)),
            )

    @contextlib.contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


class RedisSignalListener:
    """Future Redis pub/sub bridge for standalone monitor mode.

    ``connect`` is intentionally a stub until Redis is introduced. Tests and
    local integrations can pass an ``asyncio.Queue`` now, which gives the runner
    the same external-signal contract without adding infrastructure yet.
    """

    def __init__(
        self,
        channel: str,
        *,
        queue: asyncio.Queue[Any] | None = None,
        timeout_seconds: float = 1.0,
    ) -> None:
        self.channel = channel
        self.queue = queue or asyncio.Queue()
        self.timeout_seconds = timeout_seconds

    async def connect(self) -> None:
        pass

    async def get_signal(self) -> Any | None:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=max(0.1, self.timeout_seconds))
        except asyncio.TimeoutError:
            return None


class WebSocketSignalListener:
    """Listen to the public Polymarket CLOB WebSocket and enqueue stream events."""

    def __init__(
        self,
        *,
        url: str,
        assets: list[str],
        queue: asyncio.Queue[dict[str, object]] | None = None,
        timeout_seconds: float = 30.0,
        reconnect_seconds: float = 5.0,
    ) -> None:
        self.url = url
        self.assets = list(dict.fromkeys(asset for asset in assets if asset))
        self.queue = queue or asyncio.Queue()
        self.timeout_seconds = timeout_seconds
        self.reconnect_seconds = reconnect_seconds
        self._closed = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._websockets: Any | None = None

    async def __aenter__(self) -> "WebSocketSignalListener":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def connect(self) -> None:
        if not self.assets:
            raise RuntimeError("No assets available for live-paper WebSocket subscription.")
        self._websockets = _load_websockets()
        self._closed.clear()
        self._task = asyncio.create_task(self._listen_forever(), name="live-paper-websocket-listener")

    async def close(self) -> None:
        self._closed.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def get_signal(self) -> dict[str, object] | None:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=max(0.1, self.timeout_seconds))
        except asyncio.TimeoutError:
            return None

    async def _listen_forever(self) -> None:
        websockets = self._websockets or _load_websockets()
        while not self._closed.is_set():
            try:
                await self._listen_once(websockets)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - live stream should reconnect.
                LOGGER.warning("websocket reconnecting error=%s", str(exc)[:300])
                await _sleep_until_stop(self._closed, max(1.0, self.reconnect_seconds))

    async def _listen_once(self, websockets: Any) -> None:
        subscription = {
            "assets_ids": self.assets,
            "type": "market",
            "custom_feature_enabled": True,
        }
        async with websockets.connect(self.url, ping_interval=None) as ws:
            await ws.send(json.dumps(subscription, separators=(",", ":")))
            heartbeat = asyncio.create_task(self._heartbeat(ws), name="live-paper-websocket-heartbeat")
            try:
                async for raw in ws:
                    if self._closed.is_set():
                        return
                    for event in normalize_market_payload(raw):
                        await self.queue.put(event)
            finally:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat

    async def _heartbeat(self, ws: Any) -> None:
        while not self._closed.is_set():
            await _sleep_until_stop(self._closed, 10)
            if not self._closed.is_set():
                await ws.send("PING")


class LivePaperRunner:
    """Combine periodic monitoring with real-time paper position management."""

    def __init__(
        self,
        config: LivePaperConfig | None = None,
        *,
        client: PolymarketClient | None = None,
        signal_queue: asyncio.Queue[Any] | None = None,
    ) -> None:
        self.config = config or LivePaperConfig()
        self.client = client or PolymarketClient()
        self.signal_queue = signal_queue
        self.paper_state = PaperStateStore(self.config.state_db_path)
        self._stop = asyncio.Event()
        self._seen_signal_ids: set[str] = set()
        self._stream_cursor = 0
        self._last_policy_label = ""

    async def run(self) -> None:
        configure_logging(self.config.log_path)
        _load_secret_env()
        self.paper_state.init_schema()
        state = self.paper_state.load_latest_state()
        with Store(self.config.db_path) as store:
            store.init_schema()
            self._seen_signal_ids = {signal.signal_id for signal in store.fetch_recent_signals(limit=500)}
            self._stream_cursor = _int_runtime(store, "live_paper_stream_cursor", 0)
            store.set_runtime_state("live_paper_status", "starting")
            self._write_state(store)

        LOGGER.info(
            (
                "live paper runner started dry_run=%s manual_confirm=%s standalone=%s "
                "websocket=%s stream_queue=%s state_open=%s state_positions=%s"
            ),
            self.config.dry_run,
            self.config.manual_confirm,
            self.config.monitor_standalone,
            self.config.use_websocket,
            self.config.use_stream_queue,
            len(state["open_positions"]),
            state["total_positions"],
        )
        signal_task = asyncio.create_task(self._selected_signal_loop(), name="live-paper-signals")
        price_task = asyncio.create_task(self._position_loop(), name="live-paper-positions")
        try:
            await self._stop.wait()
        finally:
            signal_task.cancel()
            price_task.cancel()
            await asyncio.gather(signal_task, price_task, return_exceptions=True)
            if self.config.close_on_stop and not self.config.dry_run:
                await self.close_all_open_positions("manual_stop")
            with Store(self.config.db_path) as store:
                store.init_schema()
                store.set_runtime_state("live_paper_status", "stopped")
                self._write_state(store)
            LOGGER.info("live paper runner stopped")

    def request_stop(self) -> None:
        self._stop.set()

    async def _selected_signal_loop(self) -> None:
        if self.config.monitor_standalone:
            listener = RedisSignalListener(
                self.config.external_signal_channel,
                queue=self.signal_queue,
                timeout_seconds=min(1.0, max(0.1, self.config.poll_interval_seconds)),
            )
            await listener.connect()
            await self._external_signal_loop(listener, "standalone")
            return
        if self.config.use_websocket:
            await self._websocket_signal_loop()
            return
        if self.config.use_stream_queue:
            await self._stream_signal_loop()
            return
        await self._signal_loop()

    async def _signal_loop(self) -> None:
        iteration = 0
        while not self._stop.is_set():
            iteration += 1
            started = time.monotonic()
            try:
                with Store(self.config.db_path) as store:
                    store.init_schema()
                    self._log_portfolio_state(store, "signal_tick_start")
                signals, summary = await asyncio.to_thread(self.tick, iteration)
                LOGGER.info("tick=%s %s fresh_signals=%s", iteration, summary, len(signals))
                await self._handle_signals(signals)
                with Store(self.config.db_path) as store:
                    store.init_schema()
                    self._log_portfolio_state(store, "signal_tick_end")
            except ApiError as exc:
                LOGGER.warning("api error in signal loop: %s", str(exc)[:300])
            except Exception:
                LOGGER.exception("unexpected error in signal loop")
            elapsed = time.monotonic() - started
            await _sleep_until_stop(self._stop, max(1.0, self.config.poll_interval_seconds - elapsed))

    async def _stream_signal_loop(self) -> None:
        iteration = 0
        while not self._stop.is_set():
            iteration += 1
            started = time.monotonic()
            summary = "stream_events=0 signals=0"
            try:
                with Store(self.config.db_path) as store:
                    store.init_schema()
                    self._log_portfolio_state(store, "stream_tick_start")
                signals, summary = await asyncio.to_thread(self.stream_tick)
                LOGGER.info("stream_tick=%s %s fresh_signals=%s", iteration, summary, len(signals))
                await self._handle_signals(signals)
                with Store(self.config.db_path) as store:
                    store.init_schema()
                    self._log_portfolio_state(store, "stream_tick_end")
            except Exception:
                LOGGER.exception("unexpected error in stream signal loop")
            elapsed = time.monotonic() - started
            interval = self.config.stream_queue_interval_seconds if "stream_events=0" in summary else 0.1
            await _sleep_until_stop(self._stop, max(0.1, interval - elapsed))

    async def _websocket_signal_loop(self) -> None:
        assets = self._websocket_assets()
        if not assets:
            LOGGER.warning("websocket mode has no recent assets; waiting without polling")
            while not self._stop.is_set():
                await _sleep_until_stop(self._stop, max(1.0, self.config.websocket_reconnect_seconds))
                assets = self._websocket_assets()
                if assets:
                    break
        if not assets or self._stop.is_set():
            return
        try:
            async with WebSocketSignalListener(
                url=self.config.websocket_url,
                assets=assets,
                timeout_seconds=self.config.websocket_timeout_seconds,
                reconnect_seconds=self.config.websocket_reconnect_seconds,
            ) as listener:
                await self._external_signal_loop(listener, "websocket")
        except MissingWebsocketError:
            LOGGER.error(missing_websocket_message())
            self.request_stop()

    async def _external_signal_loop(self, listener: Any, source_name: str) -> None:
        iteration = 0
        while not self._stop.is_set():
            iteration += 1
            try:
                with Store(self.config.db_path) as store:
                    store.init_schema()
                    store.set_runtime_state("live_paper_status", f"waiting_{source_name}")
                    self._log_portfolio_state(store, f"{source_name}_tick_start")
                payload = await listener.get_signal()
                if payload is None:
                    await _sleep_until_stop(self._stop, 0.1)
                    continue
                signals, summary = await asyncio.to_thread(self._consume_external_payload, payload, source_name)
                LOGGER.info("%s_tick=%s %s fresh_signals=%s", source_name, iteration, summary, len(signals))
                await self._handle_signals(signals)
                with Store(self.config.db_path) as store:
                    store.init_schema()
                    self._log_portfolio_state(store, f"{source_name}_tick_end")
            except Exception:
                LOGGER.exception("unexpected error in %s signal loop", source_name)
                await _sleep_until_stop(self._stop, max(1.0, self.config.websocket_reconnect_seconds))

    async def _position_loop(self) -> None:
        while not self._stop.is_set():
            try:
                with Store(self.config.db_path) as store:
                    store.init_schema()
                    self._log_portfolio_state(store, "position_tick_start")
                    if not self.config.dry_run:
                        closed = await self._check_open_positions(store)
                        if closed:
                            LOGGER.info("closed_positions=%s", len(closed))
                    self._write_state(store)
                    self._log_portfolio_state(store, "position_tick_end")
            except Exception:
                LOGGER.exception("unexpected error in position loop")
            await _sleep_until_stop(self._stop, max(1, self.config.price_interval_seconds))

    def tick(self, iteration: int = 1) -> tuple[list[Signal], str]:
        """Run one monitoring tick and return newly generated signals.

        This intentionally does not call ``Monitor.scan()``, because that method
        opens paper positions immediately. The runner reuses monitor sync helpers
        and then performs signal opening itself, with optional manual approval.
        """

        with Store(self.config.db_path) as store:
            store.init_schema()
            monitor = Monitor(store=store, client=self.client, config=self._monitor_config())
            wallets_added = 0
            if self.config.leaderboard_limit > 0 and _should_discover(iteration, self.config.discover_every):
                wallets_added = monitor.discover_wallets()
            trades_added = monitor.sync_wallet_activity()
            books_synced = monitor.sync_order_books()
            signals = self._generate_signals(store)
            created = store.insert_signals(signals)
            PaperBroker(store, self._risk_config(), self._exit_config()).record_signal_created(signals)
            store.set_runtime_state("live_paper_status", "running")
            store.set_runtime_state("live_paper_last_seen", str(int(time.time())))
            store.set_runtime_state(
                "live_paper_last_summary",
                f"wallets={wallets_added} trades={trades_added} books={books_synced} signals={created}",
            )
            self._write_state(store)
            fresh = [signal for signal in signals if signal.signal_id not in self._seen_signal_ids]
            self._seen_signal_ids.update(signal.signal_id for signal in signals)
            summary = f"wallets={wallets_added} trades={trades_added} books={books_synced} signals={created}"
            return fresh, summary

    def stream_tick(self) -> tuple[list[Signal], str]:
        """Consume new WebSocket events from the SQLite stream queue."""

        with Store(self.config.db_path) as store:
            store.init_schema()
            events = store.fetch_stream_events_after_rowid(self._stream_cursor, limit=self.config.stream_batch_limit)
            if not events:
                store.set_runtime_state("live_paper_status", "waiting_stream")
                self._write_state(store)
                return [], "stream_events=0 signals=0"
            self._stream_cursor = max(int(event["queue_id"] or 0) for event in events)
            store.set_runtime_state("live_paper_stream_cursor", str(self._stream_cursor))
            if len(events) < max(1, self.config.stream_min_events):
                summary = f"stream_events={len(events)} below_min={self.config.stream_min_events} signals=0"
                store.set_runtime_state("live_paper_last_summary", summary)
                self._write_state(store)
                return [], summary
            signals = self._generate_signals(store)
            created = store.insert_signals(signals)
            PaperBroker(store, self._risk_config(), self._exit_config()).record_signal_created(signals)
            store.set_runtime_state("live_paper_status", "running_stream")
            store.set_runtime_state("live_paper_last_seen", str(int(time.time())))
            policy = self._policy_label(store)
            summary = f"stream_events={len(events)} cursor={self._stream_cursor} signals={created} policy={policy}"
            store.set_runtime_state("live_paper_last_summary", summary)
            self._write_state(store)
            fresh = [signal for signal in signals if signal.signal_id not in self._seen_signal_ids]
            self._seen_signal_ids.update(signal.signal_id for signal in signals)
            return fresh, summary

    def _consume_external_payload(self, payload: Any, source_name: str) -> tuple[list[Signal], str]:
        direct = _signal_from_payload(payload)
        if direct is not None:
            if direct.signal_id in self._seen_signal_ids:
                return [], f"{source_name}_direct_signal=duplicate"
            self._seen_signal_ids.add(direct.signal_id)
            return [direct], f"{source_name}_direct_signal=1"

        events = _stream_events_from_payload(payload)
        if not events:
            return [], f"{source_name}_events=0 signals=0"
        with Store(self.config.db_path) as store:
            store.init_schema()
            flush = StreamProcessor(store, client=self.client, config=self._stream_flush_config())._flush(events)
        signals, summary = self.stream_tick()
        return (
            signals,
            (
                f"{source_name}_events={len(events)} inserted={flush.stream_events} "
                f"books={flush.books} reconciled={flush.reconciled_trades} {summary}"
            ),
        )

    def _websocket_assets(self) -> list[str]:
        explicit = [asset for asset in self.config.websocket_assets if asset]
        if explicit:
            return list(dict.fromkeys(explicit))
        with Store(self.config.db_path) as store:
            store.init_schema()
            return store.recent_assets(
                limit=max(1, self.config.websocket_asset_limit),
                since_ts=int(time.time()) - max(1, self.config.lookback_minutes) * 60,
            )

    def _generate_signals(self, store: Store) -> list[Signal]:
        wallets = store.fetch_wallets(limit=self.config.wallet_limit)
        since_score = int(time.time()) - max(1, self.config.days) * 86400
        trades_by_wallet = {
            wallet.address: store.fetch_trades_for_wallet(wallet.address, since_score)
            for wallet in wallets
        }
        store.upsert_scores(score_wallets(wallets, trades_by_wallet))
        recent_trades = store.fetch_recent_trades(
            since_ts=int(time.time()) - self.config.lookback_minutes * 60,
            min_notional=self.config.min_trade_usdc,
            limit=2500,
        )
        order_books = store.fetch_order_books([trade.asset for trade in recent_trades])
        score_map = store.fetch_scores(min_score=self.config.min_wallet_score)
        policy_enabled, policy_mode = self._active_signal_policy(store)
        wallet_cohorts: dict[str, dict[str, object]] = {}
        if policy_enabled and self.config.use_cohort_policy:
            cohort_report = wallet_cohort_report(
                store,
                CohortConfig(
                    history_days=max(1, self.config.days),
                    min_notional=1,
                    min_trades=1,
                    limit=max(100, len(wallets)),
                ),
            )
            wallet_cohorts = {str(item["wallet"]): item for item in cohort_report["wallets"]}
        wallet_outcomes = (
            wallet_outcome_lookup(
                wallet_outcome_report(
                    store,
                    since_days=max(30, self.config.days),
                    min_events=1,
                    limit=50000,
                )
            )
            if self.config.use_learning_policy
            else {}
        )
        store.set_runtime_state(
            "signal_policy_active",
            f"enabled={int(policy_enabled)} mode={policy_mode} learning={int(self.config.use_learning_policy)}",
        )
        signals = generate_strategy_signals(
            recent_trades,
            score_map,
            SignalConfig(
                bankroll=self.config.bankroll,
                min_wallet_score=self.config.min_wallet_score,
                min_trade_usdc=self.config.min_trade_usdc,
                lookback_minutes=self.config.lookback_minutes,
                max_signals=self.config.max_signals,
                max_spread=self.config.max_spread,
                min_liquidity_score=self.config.min_liquidity_score,
                min_depth_usdc=self.config.min_depth_usdc,
                max_book_price_deviation=self.config.max_book_price_deviation,
                max_wallet_trades_per_day=self.config.max_wallet_trades_per_day,
                min_cluster_wallets=self.config.min_cluster_wallets,
                min_cluster_notional=self.config.min_cluster_notional,
                use_cohort_policy=policy_enabled and self.config.use_cohort_policy,
                cohort_policy_mode=policy_mode,
                use_learning_policy=self.config.use_learning_policy,
                stop_loss_pct=self.config.stop_loss_pct,
                take_profit_pct=self.config.take_profit_pct,
            ),
            order_books=order_books,
            wallet_cohorts=wallet_cohorts,
            wallet_outcomes=wallet_outcomes,
        )
        signals, consensus_decisions = consensus_filter_signals(
            signals,
            recent_trades=recent_trades,
            scores=score_map,
            order_books=order_books,
            config=ConsensusConfig(
                min_strategy_confidence=self.config.min_wallet_score,
                max_spread=self.config.max_spread,
                min_liquidity_score=self.config.min_liquidity_score,
                min_depth_usdc=self.config.min_depth_usdc,
                max_wallet_trades_per_day=self.config.max_wallet_trades_per_day,
            ),
        )
        persist_consensus_decisions(store.conn, consensus_decisions)
        store.set_runtime_state("consensus_last_summary", consensus_runtime_summary(consensus_decisions))
        return signals

    async def _handle_signals(self, signals: list[Signal]) -> None:
        for signal_item in signals:
            await self._handle_signal(signal_item)

    async def _handle_signal(self, signal_item: Signal) -> None:
        if not _is_active_signal(signal_item):
            LOGGER.info("skip expired/inactive signal=%s", signal_item.signal_id)
            return
        if not _auto_open_allowed(signal_item):
            LOGGER.info("skip signal=%s reason=cohort_policy reason_text=%s", signal_item.signal_id, signal_item.reason)
            return
        priced_signal = await self._signal_with_current_price(signal_item)
        LOGGER.info(
            "signal=%s action=%s asset=%s price=%.4f size=%.2f confidence=%.3f dry_run=%s",
            priced_signal.signal_id,
            priced_signal.action,
            priced_signal.asset,
            priced_signal.suggested_price,
            priced_signal.size_usdc,
            priced_signal.confidence,
            self.config.dry_run,
        )
        if self.config.dry_run:
            return
        if self.config.manual_confirm:
            approved = await self._confirm_signal(priced_signal)
            if not approved:
                LOGGER.info("manual reject signal=%s", priced_signal.signal_id)
                return
        with Store(self.config.db_path) as store:
            store.init_schema()
            opened = PaperBroker(store, self._risk_config(), self._exit_config()).open_from_signals([priced_signal])
            if opened:
                LOGGER.info("opened position=%s asset=%s entry=%.4f", opened[0].position_id, opened[0].asset, opened[0].entry_price)
                self.paper_state.upsert_open_position(opened[0])
            else:
                LOGGER.info("signal did not open position=%s", priced_signal.signal_id)
            self._write_state(store)

    async def _signal_with_current_price(self, signal_item: Signal) -> Signal:
        price = await self._current_price(signal_item.asset, side="BUY")
        if price <= 0:
            price = signal_item.suggested_price or signal_item.observed_price
        stop_loss = max(0.01, price * (1 - self.config.stop_loss_pct))
        take_profit = min(0.99, price * (1 + self.config.take_profit_pct))
        return replace(
            signal_item,
            observed_price=round(price, 4),
            suggested_price=round(price, 4),
            stop_loss=round(stop_loss, 4),
            take_profit=round(take_profit, 4),
        )

    async def _check_open_positions(self, store: Store) -> list[tuple[PaperPosition, float, str]]:
        closed: list[tuple[PaperPosition, float, str]] = []
        now = int(time.time())
        for position in store.fetch_open_positions():
            price = await self._current_price(position.asset, side="SELL")
            if price <= 0:
                price = position.entry_price
            reason = ""
            if price <= position.stop_loss:
                reason = "stop_loss"
            elif price >= position.take_profit:
                reason = "take_profit"
            if not reason:
                continue
            closed.append(self._close_position(store, position, price, reason, now))
        return closed

    async def close_all_open_positions(self, reason: str = "manual_stop") -> list[tuple[PaperPosition, float, str]]:
        closed: list[tuple[PaperPosition, float, str]] = []
        now = int(time.time())
        with Store(self.config.db_path) as store:
            store.init_schema()
            for position in store.fetch_open_positions():
                price = await self._current_price(position.asset, side="SELL")
                if price <= 0:
                    price = position.entry_price
                closed.append(self._close_position(store, position, price, reason, now))
            self._write_state(store)
        if closed:
            LOGGER.info("manual shutdown closed=%s reason=%s", len(closed), reason)
        return closed

    def _close_position(
        self,
        store: Store,
        position: PaperPosition,
        price: float,
        reason: str,
        now: int,
    ) -> tuple[PaperPosition, float, str]:
        pnl = position.shares * price - position.size_usdc
        store.close_position(position.position_id, now, price, round(pnl, 4), reason)
        store.insert_paper_events([_position_close_event(position, now, price, reason)])
        self.paper_state.close_position(position, closed_at=now, exit_price=price)
        LOGGER.info(
            "closed position=%s asset=%s price=%.4f pnl=%.4f reason=%s",
            position.position_id,
            position.asset,
            price,
            pnl,
            reason,
        )
        return position, price, reason

    async def _current_price(self, asset: str, *, side: str) -> float:
        try:
            midpoint = await asyncio.to_thread(self.client.midpoint, asset)
            if midpoint and midpoint > 0:
                return float(midpoint)
        except Exception as exc:  # noqa: BLE001 - fallback to local marks.
            LOGGER.debug("midpoint lookup failed asset=%s error=%s", asset, exc)
        try:
            price = await asyncio.to_thread(self.client.market_price, asset, side)
            if price and price > 0:
                return float(price)
        except Exception as exc:  # noqa: BLE001 - fallback to local marks.
            LOGGER.debug("best price lookup failed asset=%s side=%s error=%s", asset, side, exc)
        with Store(self.config.db_path) as store:
            store.init_schema()
            book = store.latest_order_book(asset)
            if book:
                if side.upper() == "BUY" and book.best_ask > 0:
                    return float(book.best_ask)
                if side.upper() == "SELL" and book.best_bid > 0:
                    return float(book.best_bid)
                if book.mid > 0:
                    return float(book.mid)
                if book.last_trade_price > 0:
                    return float(book.last_trade_price)
            mark = store.latest_trade_mark(asset)
            if mark and float(mark["price"]) > 0:
                return float(mark["price"])
        return 0.0

    async def _confirm_signal(self, signal_item: Signal) -> bool:
        prompt = (
            f"Open paper position? {signal_item.outcome or signal_item.asset} "
            f"price={signal_item.suggested_price:.4f} size=${signal_item.size_usdc:.2f} "
            f"confidence={signal_item.confidence:.2%} [y/N]: "
        )
        answer = await asyncio.to_thread(input, prompt)
        return answer.strip().lower() in {"y", "yes", "\u0434", "\u0434\u0430"}

    def _log_portfolio_state(self, store: Store, label: str) -> None:
        summary = _portfolio_snapshot(store, self.config.bankroll)
        LOGGER.info(
            (
                "%s balance=%.2f total_pnl=%.2f realized_pnl=%.2f unrealized_pnl=%.2f "
                "open_positions=%s trades=%s closed=%s win_rate=%.1f%% policy=%s"
            ),
            label,
            summary["balance"],
            summary["total_pnl"],
            summary["realized_pnl"],
            summary["unrealized_pnl"],
            summary["open_positions"],
            summary["trade_count"],
            summary["closed_trades"],
            summary["win_rate"] * 100,
            self._policy_label(store),
        )
        store.set_runtime_state("live_paper_metrics", _metrics_summary(summary))

    def _write_state(self, store: Store) -> None:
        summary = _portfolio_snapshot(store, self.config.bankroll)
        now = int(time.time())
        self.paper_state.record_balance(timestamp=now, balance=summary["balance"], pnl=summary["total_pnl"])
        if not self.config.export_json_state:
            return
        path = Path(self.config.state_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updatedAt": now,
            "dbPath": str(store.path),
            "dryRun": self.config.dry_run,
            "manualConfirm": self.config.manual_confirm,
            "monitorStandalone": self.config.monitor_standalone,
            "useWebSocket": self.config.use_websocket,
            "useStreamQueue": self.config.use_stream_queue,
            "stateDbPath": self.config.state_db_path,
            "streamCursor": self._stream_cursor,
            "activePolicy": self._policy_label(store),
            "summary": summary,
            "openPositions": [_position_payload(position, store) for position in store.fetch_open_positions()],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _active_signal_policy(self, store: Store) -> tuple[bool, str]:
        if not self.config.use_cohort_policy:
            enabled, mode = False, "baseline"
            label = f"enabled={int(enabled)} mode={mode}"
            store.set_runtime_state("live_paper_policy_active", label)
            return enabled, mode
        runtime = store.runtime_state()
        recommended = runtime.get("policy_optimizer_recommended")
        if not recommended:
            enabled, mode = True, "strict"
        else:
            enabled, mode = policy_settings_from_recommendation(str(recommended.get("value") or ""), default_enabled=True)
        label = f"enabled={int(enabled)} mode={mode}"
        if label != self._last_policy_label:
            LOGGER.info("policy_optimizer_reload %s", label)
            self._last_policy_label = label
        store.set_runtime_state("live_paper_policy_active", label)
        return enabled, mode

    def _policy_label(self, store: Store) -> str:
        enabled, mode = self._active_signal_policy(store)
        return f"enabled={int(enabled)} mode={mode}"

    def _monitor_config(self) -> MonitorConfig:
        return MonitorConfig(
            interval_seconds=self.config.poll_interval_seconds,
            leaderboard_limit=max(0, self.config.leaderboard_limit),
            discover_every=self.config.discover_every,
            wallet_limit=self.config.wallet_limit,
            days=self.config.days,
            per_wallet_limit=self.config.per_wallet_limit,
            bankroll=self.config.bankroll,
            min_wallet_score=self.config.min_wallet_score,
            min_trade_usdc=self.config.min_trade_usdc,
            lookback_minutes=self.config.lookback_minutes,
            max_signals=self.config.max_signals,
            live_discover=self.config.leaderboard_limit > 0,
            live_sync=True,
            sync_books=True,
            book_asset_limit=self.config.book_asset_limit,
            max_spread=self.config.max_spread,
            min_liquidity_score=self.config.min_liquidity_score,
            min_depth_usdc=self.config.min_depth_usdc,
            max_book_price_deviation=self.config.max_book_price_deviation,
            max_wallet_trades_per_day=self.config.max_wallet_trades_per_day,
            min_cluster_wallets=self.config.min_cluster_wallets,
            min_cluster_notional=self.config.min_cluster_notional,
            use_learning_policy=self.config.use_learning_policy,
            max_total_exposure_pct=self.config.max_total_exposure_pct,
            max_open_positions=self.config.max_open_positions,
            max_new_positions_per_run=self.config.max_new_positions_per_run,
            max_market_exposure_usdc=self.config.max_market_exposure_usdc,
            max_wallet_exposure_usdc=self.config.max_wallet_exposure_usdc,
            max_daily_loss_usdc=self.config.max_daily_loss_usdc,
            max_worst_stop_loss_usdc=self.config.max_worst_stop_loss_usdc,
            paper_max_hold_hours=self.config.paper_max_hold_hours,
            paper_stale_price_hours=self.config.paper_stale_price_hours,
            max_risk_trim_per_run=self.config.max_risk_trim_per_run,
            risk_trim_enabled=self.config.risk_trim_enabled,
        )

    def _risk_config(self) -> RiskConfig:
        return RiskConfig(
            bankroll=self.config.bankroll,
            max_total_exposure_pct=self.config.max_total_exposure_pct,
            max_position_usdc=25.0,
            max_market_exposure_usdc=self.config.max_market_exposure_usdc,
            max_wallet_exposure_usdc=self.config.max_wallet_exposure_usdc,
            max_open_positions=self.config.max_open_positions,
            max_new_positions_per_run=self.config.max_new_positions_per_run,
            max_daily_realized_loss_usdc=self.config.max_daily_loss_usdc,
            max_worst_stop_loss_usdc=self.config.max_worst_stop_loss_usdc,
        )

    def _exit_config(self) -> ExitConfig:
        return ExitConfig(
            max_hold_hours=self.config.paper_max_hold_hours,
            stale_price_hours=self.config.paper_stale_price_hours,
            risk_trim_enabled=self.config.risk_trim_enabled,
            max_risk_trim_positions_per_run=self.config.max_risk_trim_per_run,
        )

    def _stream_flush_config(self) -> StreamConfig:
        return StreamConfig(
            assets=self.config.websocket_assets,
            asset_limit=self.config.websocket_asset_limit,
            market_ws_url=self.config.websocket_url,
            reconcile_trades=True,
            reconcile_min_notional=self.config.min_trade_usdc,
            reconcile_limit=30,
            reconcile_window_seconds=45,
            scan_every_events=0,
            wallet_limit=self.config.wallet_limit,
            bankroll=self.config.bankroll,
            min_wallet_score=self.config.min_wallet_score,
            min_trade_usdc=self.config.min_trade_usdc,
            max_signals=self.config.max_signals,
        )


def _state_position_row(position: PaperPosition) -> tuple[object, ...]:
    entry = float(position.entry_price or 0.0)
    tp_pct = (float(position.take_profit) - entry) / entry if entry > 0 else 0.0
    sl_pct = (entry - float(position.stop_loss)) / entry if entry > 0 else 0.0
    return (
        position.position_id,
        position.condition_id or position.asset,
        "BUY",
        round(float(position.size_usdc), 4),
        round(entry, 6),
        round(max(0.0, tp_pct), 6),
        round(max(0.0, sl_pct), 6),
        position.status or "OPEN",
        int(position.opened_at),
        position.closed_at,
        round(float(position.realized_pnl or 0.0), 4),
    )


def _signal_from_payload(payload: Any) -> Signal | None:
    if isinstance(payload, Signal):
        return payload
    data = _payload_dict(payload)
    if not data:
        return None
    if "signal_id" not in data and "signalId" not in data:
        return None
    if "action" not in data or "asset" not in data:
        return None
    now = int(time.time())
    return Signal(
        signal_id=str(data.get("signal_id") or data.get("signalId") or ""),
        generated_at=_as_int(data.get("generated_at") or data.get("generatedAt"), now),
        action=str(data.get("action") or "").upper(),
        wallet=str(data.get("wallet") or ""),
        wallet_score=_as_float(data.get("wallet_score") or data.get("walletScore")),
        condition_id=str(data.get("condition_id") or data.get("conditionId") or data.get("market_id") or ""),
        asset=str(data.get("asset") or ""),
        outcome=str(data.get("outcome") or ""),
        title=str(data.get("title") or ""),
        observed_price=_as_float(data.get("observed_price") or data.get("observedPrice")),
        suggested_price=_as_float(data.get("suggested_price") or data.get("suggestedPrice")),
        size_usdc=_as_float(data.get("size_usdc") or data.get("sizeUsdc") or data.get("size")),
        confidence=_as_float(data.get("confidence")),
        stop_loss=_as_float(data.get("stop_loss") or data.get("stopLoss")),
        take_profit=_as_float(data.get("take_profit") or data.get("takeProfit")),
        expires_at=_as_int(data.get("expires_at") or data.get("expiresAt"), now + 300),
        source_trade_id=str(data.get("source_trade_id") or data.get("sourceTradeId") or ""),
        reason=str(data.get("reason") or ""),
    )


def _stream_events_from_payload(payload: Any) -> list[dict[str, object]]:
    if isinstance(payload, Signal):
        return []
    if isinstance(payload, list):
        events: list[dict[str, object]] = []
        for item in payload:
            events.extend(_stream_events_from_payload(item))
        return events
    data = _payload_dict(payload)
    if data and _looks_like_signal_payload(data):
        return []
    if data and _looks_like_stream_event(data):
        return [data]
    try:
        return normalize_market_payload(payload)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []


def _payload_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _looks_like_signal_payload(data: dict[str, Any]) -> bool:
    return ("signal_id" in data or "signalId" in data) and "action" in data and "asset" in data


def _looks_like_stream_event(data: dict[str, Any]) -> bool:
    return "event_type" in data and "event_id" in data


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_websockets() -> Any:
    try:
        return importlib.import_module("websockets")
    except ImportError as exc:
        raise MissingWebsocketError(missing_websocket_message()) from exc


def configure_logging(log_path: str, *, level: int = logging.INFO) -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(console)
    root.addHandler(file_handler)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Async paper trading runner for PolySignal.")
    parser.add_argument("--db", default=os.environ.get("POLYSIGNAL_DB", str(DEFAULT_DB_PATH)))
    parser.add_argument("--poll-interval", type=int, default=int(os.environ.get("POLYSIGNAL_POLL_INTERVAL", "60")))
    parser.add_argument("--price-interval", type=int, default=int(os.environ.get("POLYSIGNAL_PRICE_INTERVAL", "15")))
    parser.add_argument("--monitor-standalone", action="store_true", default=_env_bool("MONITOR_STANDALONE", False))
    parser.add_argument("--external-signal-channel", default=os.environ.get("POLYSIGNAL_SIGNAL_CHANNEL", "polysignal:signals"))
    parser.add_argument("--use-websocket", action="store_true", default=_env_bool("POLYSIGNAL_USE_WEBSOCKET", False))
    parser.add_argument("--websocket-url", default=os.environ.get("POLYSIGNAL_WEBSOCKET_URL", MARKET_WS_URL))
    parser.add_argument("--websocket-asset", action="append", default=[], help="CLOB token id. Repeat as needed.")
    parser.add_argument("--websocket-asset-limit", type=int, default=40)
    parser.add_argument("--websocket-timeout", type=float, default=30.0)
    parser.add_argument("--websocket-reconnect", type=float, default=5.0)
    parser.add_argument("--use-stream-queue", action="store_true")
    parser.add_argument("--stream-queue-interval", type=float, default=1.0)
    parser.add_argument("--stream-batch-limit", type=int, default=500)
    parser.add_argument("--stream-min-events", type=int, default=1)
    parser.add_argument("--manual-confirm", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-close-on-stop", action="store_true")
    parser.add_argument("--state-db", default=os.environ.get("POLYSIGNAL_PAPER_STATE_DB", "data/paper_state.db"))
    parser.add_argument("--export-json-state", action="store_true", default=_env_bool("POLYSIGNAL_EXPORT_JSON_STATE", False))
    parser.add_argument("--state-path", default=os.environ.get("POLYSIGNAL_STATE_FILE", "data/live_paper_state.json"))
    parser.add_argument("--log-path", default=os.environ.get("POLYSIGNAL_LOG_FILE", "data/live_paper_runner.log"))
    parser.add_argument("--leaderboard-limit", type=int, default=0)
    parser.add_argument("--wallet-limit", type=int, default=100)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--per-wallet-limit", type=int, default=150)
    parser.add_argument("--bankroll", type=float, default=200.0)
    parser.add_argument("--min-wallet-score", type=float, default=0.55)
    parser.add_argument("--min-trade-usdc", type=float, default=50.0)
    parser.add_argument("--lookback-minutes", type=int, default=120)
    parser.add_argument("--max-signals", type=int, default=20)
    parser.add_argument("--book-asset-limit", type=int, default=40)
    parser.add_argument("--max-spread", type=float, default=0.08)
    parser.add_argument("--min-liquidity-score", type=float, default=0.10)
    parser.add_argument("--min-depth-usdc", type=float, default=25.0)
    parser.add_argument("--stop-loss-pct", type=float, default=0.28)
    parser.add_argument("--take-profit-pct", type=float, default=0.40)
    parser.add_argument("--no-cohort-policy", action="store_true")
    parser.add_argument("--no-learning-policy", action="store_true")
    return parser


def config_from_args(args: argparse.Namespace) -> LivePaperConfig:
    return LivePaperConfig(
        db_path=args.db,
        poll_interval_seconds=args.poll_interval,
        price_interval_seconds=args.price_interval,
        monitor_standalone=args.monitor_standalone,
        external_signal_channel=args.external_signal_channel,
        use_websocket=args.use_websocket,
        websocket_url=args.websocket_url,
        websocket_assets=tuple(args.websocket_asset or ()),
        websocket_asset_limit=args.websocket_asset_limit,
        websocket_timeout_seconds=args.websocket_timeout,
        websocket_reconnect_seconds=args.websocket_reconnect,
        use_stream_queue=args.use_stream_queue,
        stream_queue_interval_seconds=args.stream_queue_interval,
        stream_batch_limit=args.stream_batch_limit,
        stream_min_events=args.stream_min_events,
        manual_confirm=args.manual_confirm,
        dry_run=args.dry_run,
        close_on_stop=not args.no_close_on_stop,
        state_db_path=args.state_db,
        export_json_state=args.export_json_state,
        state_path=args.state_path,
        log_path=args.log_path,
        leaderboard_limit=args.leaderboard_limit,
        wallet_limit=args.wallet_limit,
        days=args.days,
        per_wallet_limit=args.per_wallet_limit,
        bankroll=args.bankroll,
        min_wallet_score=args.min_wallet_score,
        min_trade_usdc=args.min_trade_usdc,
        lookback_minutes=args.lookback_minutes,
        max_signals=args.max_signals,
        book_asset_limit=args.book_asset_limit,
        max_spread=args.max_spread,
        min_liquidity_score=args.min_liquidity_score,
        min_depth_usdc=args.min_depth_usdc,
        stop_loss_pct=args.stop_loss_pct,
        take_profit_pct=args.take_profit_pct,
        use_cohort_policy=not args.no_cohort_policy,
        use_learning_policy=not args.no_learning_policy,
    )


async def _sleep_until_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


def _is_active_signal(signal_item: Signal) -> bool:
    return signal_item.action == "BUY" and signal_item.expires_at > int(time.time())


def _auto_open_allowed(signal_item: Signal) -> bool:
    if _reason_value(signal_item.reason, "learning_auto_open") == "0":
        return False
    if "cohort=" not in signal_item.reason:
        return True
    return _reason_value(signal_item.reason, "auto_open") == "1"


def _reason_value(reason: str, key: str) -> str:
    prefix = f"{key}="
    for part in reason.split(";"):
        item = part.strip()
        if item.startswith(prefix):
            return item[len(prefix) :].strip()
    return ""


def _should_discover(iteration: int, discover_every: int) -> bool:
    if discover_every <= 0:
        return iteration == 1
    return iteration == 1 or iteration % discover_every == 0


def _position_close_event(position: PaperPosition, event_at: int, price: float, reason: str) -> PaperEvent:
    pnl = position.shares * price - position.size_usdc
    return PaperEvent(
        event_id=_event_id("LIVE_CLOSED", position.position_id, reason, event_at),
        event_at=event_at,
        event_type="CLOSED",
        signal_id=position.signal_id,
        position_id=position.position_id,
        wallet=position.wallet,
        condition_id=position.condition_id,
        asset=position.asset,
        outcome=position.outcome,
        title=position.title,
        reason=reason,
        price=round(float(price), 4),
        size_usdc=round(float(position.size_usdc), 2),
        pnl=round(pnl, 4),
        hold_seconds=max(0, event_at - position.opened_at),
        metadata_json=json.dumps(
            {
                "runner": "live_paper",
                "entry_price": position.entry_price,
                "stop_loss": position.stop_loss,
                "take_profit": position.take_profit,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def _event_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:32]


def _int_runtime(store: Store, key: str, default: int = 0) -> int:
    value = store.runtime_state().get(key, {}).get("value", default)
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return default


def _portfolio_snapshot(store: Store, bankroll: float) -> dict[str, Any]:
    paper_summary = store.paper_summary()
    unrealized = 0.0
    for position in store.fetch_open_positions():
        mark = store.latest_trade_mark(position.asset)
        price = float(mark["price"]) if mark else position.entry_price
        unrealized += position.shares * price - position.size_usdc
    closed = store.conn.execute(
        """
        SELECT
            COUNT(*) AS closed_trades,
            COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0) AS winning_trades,
            COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END), 0) AS losing_trades
        FROM paper_positions
        WHERE status = 'CLOSED'
        """
    ).fetchone()
    closed_trades = int(closed["closed_trades"] or 0)
    winning_trades = int(closed["winning_trades"] or 0)
    losing_trades = int(closed["losing_trades"] or 0)
    realized_pnl = float(paper_summary["realized_pnl"])
    total_pnl = realized_pnl + unrealized
    balance = bankroll + float(paper_summary["realized_pnl"]) + unrealized
    return {
        "seed": round(bankroll, 2),
        "balance": round(balance, 2),
        "total_pnl": round(total_pnl, 4),
        "realized_pnl": round(realized_pnl, 4),
        "unrealized_pnl": round(unrealized, 4),
        "open_positions": int(paper_summary["open_positions"]),
        "open_cost": round(float(paper_summary["open_cost"]), 2),
        "total_positions": int(paper_summary["total_positions"]),
        "trade_count": int(paper_summary["total_positions"]),
        "closed_trades": closed_trades,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "win_rate": round(winning_trades / closed_trades, 4) if closed_trades else 0.0,
    }


def _metrics_summary(summary: dict[str, Any]) -> str:
    return (
        f"total_pnl=${float(summary['total_pnl']):.2f} "
        f"trades={int(summary['trade_count'])} closed={int(summary['closed_trades'])} "
        f"win_rate={float(summary['win_rate']):.1%} open={int(summary['open_positions'])}"
    )


def _position_payload(position: PaperPosition, store: Store) -> dict[str, Any]:
    mark = store.latest_trade_mark(position.asset)
    price = float(mark["price"]) if mark else position.entry_price
    return {
        "positionId": position.position_id,
        "signalId": position.signal_id,
        "asset": position.asset,
        "outcome": position.outcome,
        "title": position.title,
        "wallet": position.wallet,
        "openedAt": position.opened_at,
        "entryPrice": position.entry_price,
        "currentPrice": round(price, 4),
        "sizeUsdc": position.size_usdc,
        "shares": position.shares,
        "stopLoss": position.stop_loss,
        "takeProfit": position.take_profit,
        "unrealizedPnl": round(position.shares * price - position.size_usdc, 4),
    }


def _load_secret_env() -> dict[str, str]:
    keys = {
        "api_key": os.environ.get("POLYMARKET_API_KEY", ""),
        "api_secret": os.environ.get("POLYMARKET_API_SECRET", ""),
        "api_passphrase": os.environ.get("POLYMARKET_API_PASSPHRASE", ""),
    }
    return {key: value for key, value in keys.items() if value}


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


async def async_main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    runner = LivePaperRunner(config_from_args(args))
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, signame, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, runner.request_stop)
            except NotImplementedError:
                pass
    try:
        await runner.run()
    except KeyboardInterrupt:
        runner.request_stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
