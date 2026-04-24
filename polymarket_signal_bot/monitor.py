from __future__ import annotations

import time
from dataclasses import dataclass

from .alerts import TelegramConfig, TelegramNotifier
from .analytics import DEFAULT_DUCKDB_PATH, refresh_analytics_snapshot
from .api import ApiError, PolymarketClient
from .cohorts import CohortConfig, wallet_cohort_report
from .market_flow import MarketFlowConfig, sync_market_flow
from .models import OrderBookSnapshot, Trade, Wallet
from .paper import PaperBroker, RiskConfig
from .policy_optimizer import policy_settings_from_recommendation
from .scoring import score_wallets
from .signals import SignalConfig, generate_signals
from .storage import Store


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
    max_total_exposure_pct: float = 0.65
    max_open_positions: int = 20
    max_new_positions_per_run: int = 6
    max_market_exposure_usdc: float = 40.0
    max_wallet_exposure_usdc: float = 60.0
    max_daily_loss_usdc: float = 20.0
    max_worst_stop_loss_usdc: float = 35.0
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

    def run(self) -> None:
        self.store.init_schema()
        self.store.set_runtime_state("monitor_status", "starting")
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
        recent_trades = self.store.fetch_recent_trades(
            since_ts=int(time.time()) - self.config.lookback_minutes * 60,
            min_notional=self.config.min_trade_usdc,
            limit=2500,
        )
        order_books = self.store.fetch_order_books([trade.asset for trade in recent_trades])
        policy_enabled, policy_mode = self.active_signal_policy()
        wallet_cohorts = {}
        if policy_enabled:
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
            ),
            order_books=order_books,
            wallet_cohorts=wallet_cohorts,
        )
        self.store.set_runtime_state("signal_policy_active", f"enabled={int(policy_enabled)} mode={policy_mode}")
        created = self.store.insert_signals(signals)
        broker = PaperBroker(self.store, risk_config=self.risk_config())
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


def _should_discover(iteration: int, discover_every: int) -> bool:
    if discover_every <= 0:
        return iteration == 1
    return iteration == 1 or (iteration % discover_every == 0)


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]
