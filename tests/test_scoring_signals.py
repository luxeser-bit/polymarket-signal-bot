from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.models import PaperPosition, Signal, Trade, WalletScore
from polymarket_signal_bot.paper import ExitConfig, PaperBroker, RiskConfig
from polymarket_signal_bot.scoring import score_wallets
from polymarket_signal_bot.signals import SignalConfig, generate_signals
from polymarket_signal_bot.storage import Store


class ScoringSignalTests(unittest.TestCase):
    def test_demo_pipeline_scores_and_generates_signals(self) -> None:
        now = int(time.time())
        wallets = demo_wallets()
        trades = demo_trades(now)
        trades_by_wallet = {
            wallet.address: [trade for trade in trades if trade.proxy_wallet == wallet.address]
            for wallet in wallets
        }

        scores = score_wallets(wallets, trades_by_wallet)
        score_map = {score.wallet: score for score in scores}

        self.assertGreater(score_map[wallets[0].address].score, score_map[wallets[2].address].score)

        signals = generate_signals(
            trades,
            score_map,
            SignalConfig(bankroll=200, min_wallet_score=0.5, min_trade_usdc=50),
            now=now,
        )

        self.assertTrue(signals)
        self.assertEqual(signals[0].action, "BUY")
        self.assertGreaterEqual(signals[0].confidence, 0.5)

    def test_paper_broker_does_not_duplicate_open_asset(self) -> None:
        now = int(time.time())
        wallets = demo_wallets()
        trades = demo_trades(now)

        with tempfile.TemporaryDirectory() as tmpdir:
            store = Store(f"{tmpdir}/test.db")
            try:
                store.init_schema()
                store.upsert_wallets(wallets)
                store.insert_trades(trades)
                scores = score_wallets(
                    wallets,
                    {
                        wallet.address: store.fetch_trades_for_wallet(wallet.address)
                        for wallet in wallets
                    },
                )
                store.upsert_scores(scores)
                signals = generate_signals(
                    trades,
                    store.fetch_scores(min_score=0.5),
                    SignalConfig(min_wallet_score=0.5, min_trade_usdc=50),
                    now=now,
                )
                broker = PaperBroker(store)
                created_events = broker.record_signal_created(signals)
                opened_first = broker.open_from_signals(signals)
                opened_second = broker.open_from_signals(signals)
                journal = store.paper_event_summary(limit=10)

                self.assertEqual(created_events, len(signals))
                self.assertTrue(opened_first)
                self.assertEqual(opened_second, [])
                self.assertGreaterEqual(journal["total_events"], len(signals) + len(opened_first))
            finally:
                store.close()

    def test_cohort_policy_scales_signals_and_blocks_watch_auto_open(self) -> None:
        now = int(time.time())
        stable_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        watch_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        trades = [
            Trade(
                trade_id="stable-signal",
                proxy_wallet=stable_wallet,
                side="BUY",
                asset="stable-asset",
                condition_id="stable-cond",
                size=400,
                price=0.4,
                timestamp=now,
                title="Stable signal",
                outcome="Yes",
            ),
            Trade(
                trade_id="watch-signal",
                proxy_wallet=watch_wallet,
                side="BUY",
                asset="watch-asset",
                condition_id="watch-cond",
                size=400,
                price=0.4,
                timestamp=now,
                title="Watch signal",
                outcome="Yes",
            ),
        ]
        scores = {
            stable_wallet: self.wallet_score(stable_wallet, 0.72, now),
            watch_wallet: self.wallet_score(watch_wallet, 0.72, now),
        }
        cohorts = {
            stable_wallet: {"status": "STABLE", "stabilityScore": 0.72},
            watch_wallet: {"status": "WATCH", "stabilityScore": 0.42},
        }

        signals = generate_signals(
            trades,
            scores,
            SignalConfig(bankroll=200, min_wallet_score=0.5, min_trade_usdc=50),
            wallet_cohorts=cohorts,
            now=now,
        )
        by_wallet = {signal.wallet: signal for signal in signals}

        self.assertGreater(by_wallet[stable_wallet].size_usdc, by_wallet[watch_wallet].size_usdc)
        self.assertIn("cohort=STABLE", by_wallet[stable_wallet].reason)
        self.assertIn("auto_open=0", by_wallet[watch_wallet].reason)

        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/policy.db") as store:
                store.init_schema()
                store.insert_signals(signals)
                pending = store.fetch_review_queue("PENDING", limit=10)
                opened = PaperBroker(store).open_from_signals(signals)
                store.set_signal_review_status(by_wallet[watch_wallet].signal_id, "APPROVED", "manual")
                approved_opened = PaperBroker(store).open_approved(limit=10)

        self.assertEqual(pending[0]["cohort_status"], "STABLE")
        self.assertLess(pending[0]["priority"], pending[-1]["priority"])
        self.assertEqual([position.wallet for position in opened], [stable_wallet])
        self.assertEqual([position.wallet for position in approved_opened], [watch_wallet])

    def test_paper_broker_blocks_new_positions_when_exposure_cap_is_hit(self) -> None:
        now = int(time.time())
        wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        signals = [
            self.signal("risk-a", wallet, "asset-a", "cond-a", now, size=8.0),
            self.signal("risk-b", wallet, "asset-b", "cond-b", now, size=8.0),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/risk.db") as store:
                store.init_schema()
                opened = PaperBroker(
                    store,
                    RiskConfig(
                        bankroll=100,
                        max_total_exposure_pct=0.10,
                        max_open_positions=10,
                        max_new_positions_per_run=10,
                        max_market_exposure_usdc=100,
                        max_wallet_exposure_usdc=100,
                        max_daily_realized_loss_usdc=100,
                        max_worst_stop_loss_usdc=100,
                    ),
                ).open_from_signals(signals)
                runtime = store.runtime_state()
                journal = store.paper_event_summary(limit=10)

        self.assertEqual(len(opened), 1)
        self.assertIn("total_exposure_cap", runtime["risk_last_blocks"]["value"])
        self.assertIn("blocked=1", runtime["risk_last_summary"]["value"])
        reasons = {(row["event_type"], row["reason"]): row["events"] for row in journal["by_reason"]}
        self.assertEqual(reasons[("OPENED", "opened")], 1)
        self.assertEqual(reasons[("BLOCKED", "total_exposure_cap")], 1)

    def test_paper_broker_closes_max_hold_positions_with_reason(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/exit.db") as store:
                store.init_schema()
                store.insert_paper_positions(
                    [
                        self.position(
                            "pos-max-hold",
                            "sig-max-hold",
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "exit-asset",
                            "exit-cond",
                            now - 3 * 3600,
                        )
                    ]
                )
                store.insert_trades(
                    [
                        Trade(
                            trade_id="exit-trade",
                            proxy_wallet="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                            side="BUY",
                            asset="exit-asset",
                            condition_id="exit-cond",
                            size=100,
                            price=0.45,
                            timestamp=now,
                            title="Exit test",
                            outcome="Yes",
                        )
                    ]
                )
                closed = PaperBroker(store).mark_and_close(
                    ExitConfig(max_hold_hours=1, stale_price_hours=24, risk_trim_enabled=False)
                )
                recent = store.fetch_recent_closed_positions(limit=1)
                events = store.fetch_recent_paper_events(limit=5)

        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0][2], "max_hold")
        self.assertEqual(recent[0].close_reason, "max_hold")
        self.assertGreater(recent[0].realized_pnl, 0)
        self.assertEqual(events[0].event_type, "CLOSED")
        self.assertEqual(events[0].reason, "max_hold")

    def test_paper_broker_risk_trims_locked_portfolio(self) -> None:
        now = int(time.time())
        wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        positions = [
            self.position(f"pos-trim-{index}", f"sig-trim-{index}", wallet, f"trim-asset-{index}", f"trim-cond-{index}", now - index)
            for index in range(3)
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/trim.db") as store:
                store.init_schema()
                store.insert_paper_positions(positions)
                closed = PaperBroker(
                    store,
                    RiskConfig(
                        bankroll=100,
                        max_total_exposure_pct=0.50,
                        max_open_positions=10,
                        max_market_exposure_usdc=100,
                        max_wallet_exposure_usdc=100,
                        max_daily_realized_loss_usdc=100,
                        max_worst_stop_loss_usdc=100,
                    ),
                ).mark_and_close(
                    ExitConfig(
                        max_hold_hours=24,
                        stale_price_hours=24,
                        risk_trim_enabled=True,
                        max_risk_trim_positions_per_run=2,
                        trim_to_total_exposure_pct=0.40,
                    )
                )
                open_positions = store.fetch_open_positions()
                recent = store.fetch_recent_closed_positions(limit=3)

        self.assertEqual(len(closed), 2)
        self.assertEqual(len(open_positions), 1)
        self.assertEqual({position.close_reason for position in recent[:2]}, {"risk_trim"})

    def test_paper_broker_closes_stale_price_positions_at_entry(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/stale.db") as store:
                store.init_schema()
                store.insert_paper_positions(
                    [
                        self.position(
                            "pos-stale",
                            "sig-stale",
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "stale-asset",
                            "stale-cond",
                            now - 3 * 3600,
                        )
                    ]
                )
                closed = PaperBroker(store).mark_and_close(
                    ExitConfig(max_hold_hours=24, stale_price_hours=1, risk_trim_enabled=False)
                )
                recent = store.fetch_recent_closed_positions(limit=1)

        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0][2], "stale_price")
        self.assertEqual(recent[0].close_reason, "stale_price")
        self.assertEqual(recent[0].realized_pnl, 0)

    @staticmethod
    def wallet_score(wallet: str, score: float, now: int) -> WalletScore:
        return WalletScore(
            wallet=wallet,
            score=score,
            computed_at=now,
            trade_count=8,
            buy_count=8,
            sell_count=0,
            total_notional=1200,
            avg_notional=150,
            active_days=4,
            market_count=4,
            leaderboard_pnl=0,
            leaderboard_volume=0,
            pnl_efficiency=0,
            reason="test",
            repeatability_score=0.5,
            drawdown_score=0.5,
        )

    @staticmethod
    def signal(signal_id: str, wallet: str, asset: str, condition_id: str, now: int, *, size: float) -> Signal:
        return Signal(
            signal_id=signal_id,
            generated_at=now,
            action="BUY",
            wallet=wallet,
            wallet_score=0.8,
            condition_id=condition_id,
            asset=asset,
            outcome="Yes",
            title="Risk test",
            observed_price=0.4,
            suggested_price=0.4,
            size_usdc=size,
            confidence=0.8,
            stop_loss=0.3,
            take_profit=0.6,
            expires_at=now + 3600,
            source_trade_id=signal_id,
            reason="test",
        )

    @staticmethod
    def position(
        position_id: str,
        signal_id: str,
        wallet: str,
        asset: str,
        condition_id: str,
        opened_at: int,
    ) -> PaperPosition:
        return PaperPosition(
            position_id=position_id,
            signal_id=signal_id,
            opened_at=opened_at,
            wallet=wallet,
            condition_id=condition_id,
            asset=asset,
            outcome="Yes",
            title="Paper exit test",
            entry_price=0.4,
            size_usdc=30,
            shares=75,
            stop_loss=0.3,
            take_profit=0.8,
        )


if __name__ == "__main__":
    unittest.main()
