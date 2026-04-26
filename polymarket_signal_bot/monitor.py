from __future__ import annotations

import json
import pickle
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .alerts import TelegramConfig, TelegramNotifier
from .analytics import DEFAULT_DUCKDB_PATH, refresh_analytics_snapshot
from .api import ApiError, PolymarketClient
from .auto_trainer import run_full_training_cycle
from .cohorts import CohortConfig, load_wallet_cohorts, wallet_cohort_report
from .market_flow import MarketFlowConfig, sync_market_flow
from .models import OrderBookSnapshot, Trade, Wallet, WalletScore
from .paper import ExitConfig, PaperBroker, RiskConfig
from .policy_optimizer import policy_settings_from_recommendation
from .scoring import score_wallets
from .signals import SignalConfig, generate_signals
from .storage import Store
from .learning import wallet_outcome_lookup, wallet_outcome_report


@dataclass(frozen=True)
class MonitorConfig:
    interval_seconds: int = 60
    iterations: int = 0
    leaderboard_limit: int = 50
    discover_every: int = 10
    wallet_limit: int = 50
    days: int = 7
    per_wallet_limit: int = 150
    bankroll: float = 200.0
    min_wallet_score: float = 0.55
    min_trade_usdc: float = 50.0
    lookback_minutes: int = 120
    max_signals: int = 20
    live_discover: bool = True
    live_sync: bool = True
    sync_books: bool = True
    book_asset_limit: int = 40
    max_spread: float = 0.08
    min_liquidity_score: float = 0.10
    min_depth_usdc: float = 25.0
    max_book_price_deviation: float = 0.12
    max_wallet_trades_per_day: float = 60.0
    min_cluster_wallets: int = 1
    min_cluster_notional: float = 0.0
    use_learning_policy: bool = True
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
    market_flow_every: int = 0
    market_flow_market_limit: int = 15
    market_flow_trades_per_market: int = 100
    market_flow_min_trade_cash: float = 25.0
    market_flow_min_wallet_notional: float = 100.0
    market_flow_min_wallet_trades: int = 2
    analytics_export_every: int = 0
    analytics_duckdb_path: str = str(DEFAULT_DUCKDB_PATH)
    analytics_chunk_size: int = 50000
    analytics_rebuild: bool = False
    auto_retrain: bool = True
    retrain_interval_hours: float = 24.0
    retrain_min_new_records: int = 0
    training_db_path: str = "data/indexer.db"
    exit_model_path: str = "data/exit_model.pkl"
    exit_stats_path: str = "data/exit_stats.json"
    policy_path: str = "data/best_policy.json"


class Monitor:
    def __init__(
        self,
        *,
        store: Store,
        client: PolymarketClient | None = None,
        config: MonitorConfig | None = None,
        telegram: TelegramConfig | None = None,
    ) -> None:
        self.store = store
        self.client = client or PolymarketClient()
        self.config = config or MonitorConfig()
        self.notifier = TelegramNotifier(telegram or TelegramConfig())
        self.cohorts: dict[str, dict[str, Any]] = {}
        self.exit_model: dict[str, Any] | None = None
        self._retrain_thread: threading.Thread | None = None
        self._last_retrain_started_at = 0.0
        self._last_retrain_summary: dict[str, Any] = {}
        self._refresh_training_artifacts()

    def run(self) -> None:
        self.store.init_schema()
        self.store.set_runtime_state("monitor_status", "starting")
        self._maybe_schedule_retrain()
        iteration = 0
        while True:
            iteration += 1
            started_at = int(time.time())
            try:
                summary = self.run_once(iteration)
                self.store.set_runtime_state("monitor_status", "sleeping")
                self.store.set_runtime_state("monitor_last_summary", summary)
                print(summary)
            except ApiError as exc:
                message = f"api_error iteration={iteration} error={str(exc)[:180]}"
                self.store.set_runtime_state("monitor_status", "api_error")
                self.store.set_runtime_state("monitor_last_error", message)
                print(message)
            except Exception as exc:  # noqa: BLE001 - keep monitor alive.
                message = f"error iteration={iteration} error={str(exc)[:180]}"
                self.store.set_runtime_state("monitor_status", "error")
                self.store.set_runtime_state("monitor_last_error", message)
                print(message)

            if self.config.iterations and iteration >= self.config.iterations:
                self.store.set_runtime_state("monitor_status", "stopped")
                return

            self._maybe_schedule_retrain()
            elapsed = int(time.time()) - started_at
            sleep_for = max(1, self.config.interval_seconds - elapsed)
            time.sleep(sleep_for)

    def run_once(self, iteration: int = 1) -> str:
        self.store.set_runtime_state("monitor_status", "scanning")
        wallets_added = 0
        if self.config.live_discover and _should_discover(iteration, self.config.discover_every):
            wallets_added = self.discover_wallets()
        market_flow_status = ""
        if self.config.market_flow_every > 0 and iteration % self.config.market_flow_every == 0:
            flow_summary = sync_market_flow(
                self.store,
                client=self.client,
                config=MarketFlowConfig(
                    market_limit=self.config.market_flow_market_limit,
                    trades_per_market=self.config.market_flow_trades_per_market,
                    min_trade_cash=self.config.market_flow_min_trade_cash,
                    min_wallet_notional=self.config.market_flow_min_wallet_notional,
                    min_wallet_trades=self.config.market_flow_min_wallet_trades,
                ),
            )
            market_flow_status = (
                f" market_flow_trades={flow_summary['trades_inserted']}"
                f" market_flow_wallets={flow_summary['wallets_promoted']}"
            )
        trades_added = self.sync_wallet_activity() if self.config.live_sync else 0
        books_synced = self.sync_order_books() if self.config.sync_books else 0
        signals_created, positions_opened, positions_closed = self.scan()
        risk_status = self.store.runtime_state().get("risk_status", {}).get("value", "unknown")
        alerts_sent = self.notifier.notify_new_signals(self.store)
        analytics_status = ""
        if self.config.analytics_export_every > 0 and iteration % self.config.analytics_export_every == 0:
            result = refresh_analytics_snapshot(
                self.store,
                duckdb_path=self.config.analytics_duckdb_path,
                chunk_size=self.config.analytics_chunk_size,
                rebuild=self.config.analytics_rebuild,
            )
            analytics_status = " analytics=ok" if result["ok"] else " analytics=error"
        heartbeat = int(time.time())
        self.store.set_runtime_state("monitor_last_seen", str(heartbeat))
        return (
            f"iteration={iteration} wallets={wallets_added} trades={trades_added} "
            f"books={books_synced} signals={signals_created} opened={positions_opened} "
            f"closed={positions_closed} risk={risk_status} alerts={alerts_sent}{market_flow_status}{analytics_status}"
        )

    def retrain(self, *, force: bool = True, wait: bool = False) -> dict[str, Any]:
        if self._retrain_thread and self._retrain_thread.is_alive():
            return {"started": False, "reason": "already_running"}
        self._last_retrain_started_at = time.time()
        self._last_retrain_summary = {"started": True, "status": "running"}
        self._set_runtime_state_safe("auto_trainer_status", "running")
        thread = threading.Thread(target=self._run_training_cycle, args=(force,), daemon=True)
        self._retrain_thread = thread
        thread.start()
        if wait:
            thread.join()
            return self._last_retrain_summary
        return {"started": True, "status": "running"}

    def _maybe_schedule_retrain(self) -> None:
        if not self.config.auto_retrain:
            return
        if self._retrain_thread and self._retrain_thread.is_alive():
            return
        if not self._training_db_has_data():
            return
        now = time.time()
        interval = max(1.0, self.config.retrain_interval_hours) * 3600
        last_started = max(self._last_retrain_started_at, float(self._last_training_at()))
        if last_started <= 0 or now - last_started >= interval:
            self.retrain(force=False, wait=False)

    def _run_training_cycle(self, force: bool) -> None:
        try:
            summary = run_full_training_cycle(
                db_path=self.config.training_db_path,
                model_path=self.config.exit_model_path,
                stats_path=self.config.exit_stats_path,
                policy_path=self.config.policy_path,
                min_new_records=self.config.retrain_min_new_records,
                force=force,
            )
            self._last_retrain_summary = summary
            self._refresh_training_artifacts()
            status = "ok" if summary.get("ok") else "error"
            if summary.get("skipped"):
                status = "skipped"
            self._set_runtime_state_safe("auto_trainer_status", status)
            self._set_runtime_state_safe(
                "auto_trainer_last_summary",
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
            )
        except Exception as exc:  # noqa: BLE001 - keep monitor signal loop alive.
            summary = {"ok": False, "error": str(exc)[:500]}
            self._last_retrain_summary = summary
            self._set_runtime_state_safe("auto_trainer_status", "error")
            self._set_runtime_state_safe(
                "auto_trainer_last_summary",
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
            )

    def _refresh_training_artifacts(self) -> None:
        self.cohorts = load_wallet_cohorts(
            self.config.training_db_path,
            statuses={"STABLE", "CANDIDATE"},
            limit=100_000,
        )
        self.exit_model = self._load_exit_model()

    def _load_exit_model(self) -> dict[str, Any] | None:
        path = Path(self.config.exit_model_path)
        if not path.exists():
            return None
        try:
            with path.open("rb") as handle:
                model = pickle.load(handle)
        except Exception:  # noqa: BLE001 - a bad model file must not stop monitoring.
            return None
        return model if isinstance(model, dict) else {"type": type(model).__name__, "model": model}

    def _indexed_score_map(self, *, min_score: float) -> dict[str, WalletScore]:
        path = Path(self.config.training_db_path)
        if not path.exists():
            return {}
        try:
            uri = f"{path.resolve().as_uri()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=30)
            try:
                conn.row_factory = sqlite3.Row
                if not _table_exists(conn, "scored_wallets"):
                    return {}
                rows = conn.execute(
                    """
                    SELECT wallet, score, computed_at, trade_count, buy_count, sell_count,
                           volume, avg_trade_size, active_days, market_count, pnl,
                           profit_factor, win_rate, max_drawdown, reason
                    FROM scored_wallets
                    WHERE score >= ?
                    """,
                    (min_score,),
                ).fetchall()
            finally:
                conn.close()
        except sqlite3.Error:
            return {}
        return {
            str(row["wallet"]): WalletScore(
                wallet=str(row["wallet"]),
                score=float(row["score"] or 0.0),
                computed_at=int(row["computed_at"] or 0),
                trade_count=int(row["trade_count"] or 0),
                buy_count=int(row["buy_count"] or 0),
                sell_count=int(row["sell_count"] or 0),
                total_notional=float(row["volume"] or 0.0),
                avg_notional=float(row["avg_trade_size"] or 0.0),
                active_days=int(row["active_days"] or 0),
                market_count=int(row["market_count"] or 0),
                leaderboard_pnl=float(row["pnl"] or 0.0),
                leaderboard_volume=float(row["volume"] or 0.0),
                pnl_efficiency=float(row["pnl"] or 0.0) / max(1.0, float(row["volume"] or 0.0)),
                reason=str(row["reason"] or ""),
                repeatability_score=float(row["win_rate"] or 0.0),
                drawdown_score=max(
                    0.0,
                    1.0 - float(row["max_drawdown"] or 0.0) / max(1.0, float(row["volume"] or 0.0)),
                ),
            )
            for row in rows
        }

    def _training_db_has_data(self) -> bool:
        path = Path(self.config.training_db_path)
        if not path.exists():
            return False
        try:
            uri = f"{path.resolve().as_uri()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=5)
            try:
                if not _table_exists(conn, "raw_transactions"):
                    return False
                row = conn.execute("SELECT 1 FROM raw_transactions LIMIT 1").fetchone()
                return row is not None
            finally:
                conn.close()
        except sqlite3.Error:
            return False

    def _last_training_at(self) -> int:
        path = Path(self.config.training_db_path)
        if not path.exists():
            return 0
        try:
            uri = f"{path.resolve().as_uri()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=5)
            try:
                if not _table_exists(conn, "training_runs"):
                    return 0
                row = conn.execute(
                    "SELECT started_at FROM training_runs WHERE ok = 1 ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                return int(row[0] or 0) if row else 0
            finally:
                conn.close()
        except sqlite3.Error:
            return 0

    def _set_runtime_state_safe(self, key: str, value: str) -> None:
        try:
            with Store(self.store.path) as store:
                store.init_schema()
                store.set_runtime_state(key, value)
        except Exception:  # noqa: BLE001 - runtime state is diagnostic only.
            return

    def discover_wallets(self) -> int:
        wallets: list[Wallet] = []
        remaining = max(0, self.config.leaderboard_limit)
        offset = 0
        while remaining > 0:
            page_limit = min(50, remaining)
            rows = self.client.leaderboard(
                limit=page_limit,
                offset=offset,
                category="OVERALL",
                time_period="WEEK",
                order_by="PNL",
            )
            if not rows:
                break
            wallets.extend(Wallet.from_leaderboard(row) for row in rows)
            fetched = len(rows)
            remaining -= fetched
            offset += fetched
            if fetched < page_limit:
                break
        return self.store.upsert_wallets(wallets)

    def sync_wallet_activity(self) -> int:
        wallets = self.store.fetch_wallets(limit=self.config.wallet_limit)
        if not wallets:
            return 0
        start = int(time.time()) - max(1, self.config.days) * 86400
        inserted = 0
        for wallet in wallets:
            rows = self.client.user_activity(
                wallet.address,
                limit=min(500, self.config.per_wallet_limit),
                start=start,
                types=["TRADE"],
            )
            trades = [Trade.from_api(row, source="monitor") for row in rows]
            inserted += self.store.insert_trades(trades)
        return inserted

    def sync_order_books(self) -> int:
        assets = self.store.recent_assets(
            limit=self.config.book_asset_limit,
            since_ts=int(time.time()) - self.config.lookback_minutes * 60,
        )
        if not assets:
            return 0
        synced = 0
        for chunk in _chunks(assets, 20):
            rows = self.client.order_books(chunk)
            books = [OrderBookSnapshot.from_api(row) for row in rows]
            raw_by_asset = {str(row.get("asset_id") or ""): row for row in rows}
            synced += self.store.upsert_order_books(books, raw_by_asset=raw_by_asset)
        return synced

    def scan(self) -> tuple[int, int, int]:
        wallets = self.store.fetch_wallets(limit=self.config.wallet_limit)
        since_score = int(time.time()) - max(1, self.config.days) * 86400
        trades_by_wallet = {
            wallet.address: self.store.fetch_trades_for_wallet(wallet.address, since_score)
            for wallet in wallets
        }
        scores = score_wallets(wallets, trades_by_wallet)
        self.store.upsert_scores(scores)

        score_map = self.store.fetch_scores(min_score=self.config.min_wallet_score)
        score_map.update(self._indexed_score_map(min_score=self.config.min_wallet_score))
        recent_trades = self.store.fetch_recent_trades(
            since_ts=int(time.time()) - self.config.lookback_minutes * 60,
            min_notional=self.config.min_trade_usdc,
            limit=2500,
        )
        order_books = self.store.fetch_order_books([trade.asset for trade in recent_trades])
        policy_enabled, policy_mode = self.active_signal_policy()
        wallet_cohorts = {}
        if policy_enabled:
            self._refresh_training_artifacts()
            wallet_cohorts = self.cohorts
            if not wallet_cohorts:
                cohort_report = wallet_cohort_report(
                    self.store,
                    CohortConfig(
                        history_days=max(1, self.config.days),
                        min_notional=1,
                        min_trades=1,
                        limit=max(100, len(wallets)),
                    ),
                )
                wallet_cohorts = {str(item["wallet"]): item for item in cohort_report["wallets"]}
        signals = generate_signals(
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
                use_cohort_policy=policy_enabled,
                cohort_policy_mode=policy_mode,
                use_learning_policy=self.config.use_learning_policy,
            ),
            order_books=order_books,
            wallet_cohorts=wallet_cohorts,
            wallet_outcomes=wallet_outcome_lookup(
                wallet_outcome_report(
                    self.store,
                    since_days=max(30, self.config.days),
                    min_events=1,
                    limit=50000,
                )
            )
            if self.config.use_learning_policy
            else {},
        )
        self.store.set_runtime_state(
            "signal_policy_active",
            f"enabled={int(policy_enabled)} mode={policy_mode} learning={int(self.config.use_learning_policy)}",
        )
        created = self.store.insert_signals(signals)
        broker = PaperBroker(self.store, risk_config=self.risk_config(), exit_config=self.exit_config())
        broker.record_signal_created(signals)
        closed = len(broker.mark_and_close())
        opened = len(broker.open_from_signals(signals))
        return created, opened, closed

    def active_signal_policy(self) -> tuple[bool, str]:
        runtime = self.store.runtime_state()
        recommended = runtime.get("policy_optimizer_recommended")
        if not recommended:
            return True, "strict"
        return policy_settings_from_recommendation(str(recommended.get("value") or ""), default_enabled=True)

    def risk_config(self) -> RiskConfig:
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

    def exit_config(self) -> ExitConfig:
        return ExitConfig(
            max_hold_hours=self.config.paper_max_hold_hours,
            stale_price_hours=self.config.paper_stale_price_hours,
            risk_trim_enabled=self.config.risk_trim_enabled,
            max_risk_trim_positions_per_run=self.config.max_risk_trim_per_run,
        )


def _should_discover(iteration: int, discover_every: int) -> bool:
    if discover_every <= 0:
        return iteration == 1
    return iteration == 1 or (iteration % discover_every == 0)


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None
