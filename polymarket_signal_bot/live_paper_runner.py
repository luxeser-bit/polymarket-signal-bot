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

Confidential values, if a future authenticated API client is added, should come
from environment variables such as ``POLYMARKET_API_KEY``,
``POLYMARKET_API_SECRET`` and ``POLYMARKET_API_PASSPHRASE``. The current runner
uses public market data only and never stores private keys.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .api import ApiError, PolymarketClient
from .cohorts import CohortConfig, wallet_cohort_report
from .learning import wallet_outcome_lookup, wallet_outcome_report
from .models import PaperEvent, PaperPosition, Signal, Wallet
from .monitor import Monitor, MonitorConfig
from .paper import ExitConfig, PaperBroker, RiskConfig
from .policy_optimizer import policy_settings_from_recommendation
from .scoring import score_wallets
from .signals import SignalConfig, generate_signals
from .storage import DEFAULT_DB_PATH, Store


LOGGER = logging.getLogger("polymarket_signal_bot.live_paper")


@dataclass(frozen=True)
class LivePaperConfig:
    db_path: str = str(DEFAULT_DB_PATH)
    poll_interval_seconds: int = 60
    price_interval_seconds: int = 15
    use_stream_queue: bool = False
    stream_queue_interval_seconds: float = 1.0
    stream_batch_limit: int = 500
    stream_min_events: int = 1
    manual_confirm: bool = False
    dry_run: bool = False
    close_on_stop: bool = True
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


class LivePaperRunner:
    """Combine periodic monitoring with real-time paper position management."""

    def __init__(
        self,
        config: LivePaperConfig | None = None,
        *,
        client: PolymarketClient | None = None,
    ) -> None:
        self.config = config or LivePaperConfig()
        self.client = client or PolymarketClient()
        self._stop = asyncio.Event()
        self._seen_signal_ids: set[str] = set()
        self._stream_cursor = 0
        self._last_policy_label = ""

    async def run(self) -> None:
        configure_logging(self.config.log_path)
        _load_secret_env()
        with Store(self.config.db_path) as store:
            store.init_schema()
            self._seen_signal_ids = {signal.signal_id for signal in store.fetch_recent_signals(limit=500)}
            self._stream_cursor = _int_runtime(store, "live_paper_stream_cursor", 0)
            store.set_runtime_state("live_paper_status", "starting")
            self._write_state(store)

        LOGGER.info(
            "live paper runner started dry_run=%s manual_confirm=%s stream_queue=%s",
            self.config.dry_run,
            self.config.manual_confirm,
            self.config.use_stream_queue,
        )
        signal_task = asyncio.create_task(
            self._stream_signal_loop() if self.config.use_stream_queue else self._signal_loop(),
            name="live-paper-signals",
        )
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
        return generate_signals(
            recent_trades,
            store.fetch_scores(min_score=self.config.min_wallet_score),
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
        return answer.strip().lower() in {"y", "yes", "д", "да"}

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
        path = Path(self.config.state_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        summary = _portfolio_snapshot(store, self.config.bankroll)
        payload = {
            "updatedAt": int(time.time()),
            "dbPath": str(store.path),
            "dryRun": self.config.dry_run,
            "manualConfirm": self.config.manual_confirm,
            "useStreamQueue": self.config.use_stream_queue,
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
    parser.add_argument("--use-stream-queue", action="store_true")
    parser.add_argument("--stream-queue-interval", type=float, default=1.0)
    parser.add_argument("--stream-batch-limit", type=int, default=500)
    parser.add_argument("--stream-min-events", type=int, default=1)
    parser.add_argument("--manual-confirm", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-close-on-stop", action="store_true")
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
        use_stream_queue=args.use_stream_queue,
        stream_queue_interval_seconds=args.stream_queue_interval,
        stream_batch_limit=args.stream_batch_limit,
        stream_min_events=args.stream_min_events,
        manual_confirm=args.manual_confirm,
        dry_run=args.dry_run,
        close_on_stop=not args.no_close_on_stop,
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
