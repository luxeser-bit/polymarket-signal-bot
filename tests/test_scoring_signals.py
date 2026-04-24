from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.models import Signal, Trade, WalletScore
from polymarket_signal_bot.paper import PaperBroker, RiskConfig
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
                opened_first = broker.open_from_signals(signals)
                opened_second = broker.open_from_signals(signals)

                self.assertTrue(opened_first)
                self.assertEqual(opened_second, [])
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

        self.assertEqual(len(opened), 1)
        self.assertIn("total_exposure_cap", runtime["risk_last_blocks"]["value"])
        self.assertIn("blocked=1", runtime["risk_last_summary"]["value"])

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


if __name__ == "__main__":
    unittest.main()
