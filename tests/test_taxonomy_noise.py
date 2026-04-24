from __future__ import annotations

import time
import unittest

from polymarket_signal_bot.backtest import BacktestConfig, run_backtest
from polymarket_signal_bot.models import Trade, WalletScore
from polymarket_signal_bot.signals import SignalConfig, generate_signals
from polymarket_signal_bot.taxonomy import market_category


class TaxonomyNoiseTests(unittest.TestCase):
    def test_market_category_detects_sports_and_crypto(self) -> None:
        self.assertEqual(market_category("Knicks vs. Hawks: O/U 216.5"), "sports")
        self.assertEqual(market_category("Will Bitcoin hit $100k?"), "crypto")

    def test_noisy_wallet_filter_blocks_high_frequency_wallet(self) -> None:
        now = int(time.time())
        wallet = "0x1111111111111111111111111111111111111111"
        trade = Trade.from_api(
            {
                "proxyWallet": wallet,
                "side": "BUY",
                "asset": "asset-1",
                "conditionId": "cond-1",
                "size": 1000,
                "price": 0.4,
                "timestamp": now,
                "title": "Will Bitcoin hit $100k?",
                "outcome": "Yes",
                "transactionHash": "0xnoise",
            }
        )
        scores = {
            wallet: WalletScore(
                wallet=wallet,
                score=0.9,
                computed_at=now,
                trade_count=1000,
                buy_count=900,
                sell_count=100,
                total_notional=100000,
                avg_notional=100,
                active_days=1,
                market_count=100,
                leaderboard_pnl=1000,
                leaderboard_volume=100000,
                pnl_efficiency=0.01,
                reason="test",
            )
        }

        signals = generate_signals(
            [trade],
            scores,
            SignalConfig(
                min_wallet_score=0.5,
                min_trade_usdc=20,
                max_wallet_trades_per_day=60,
            ),
            now=now,
        )

        self.assertEqual(signals, [])

    def test_backtest_includes_category_breakdown(self) -> None:
        now = int(time.time()) - 3600
        wallet = "0x1111111111111111111111111111111111111111"
        raw = []
        for index in range(5):
            raw.append(
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": f"warm-{index}",
                    "conditionId": f"warm-cond-{index}",
                    "size": 100,
                    "price": 0.3,
                    "timestamp": now + index * 60,
                    "title": "Warmup Bitcoin market",
                    "outcome": "Yes",
                    "transactionHash": f"0xwarmtax{index}",
                }
            )
        raw.extend(
            [
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": "btc-target",
                    "conditionId": "btc-cond",
                    "size": 500,
                    "price": 0.4,
                    "timestamp": now + 600,
                    "title": "Will Bitcoin hit $100k?",
                    "outcome": "Yes",
                    "transactionHash": "0xtax1",
                },
                {
                    "proxyWallet": "0x2222222222222222222222222222222222222222",
                    "side": "BUY",
                    "asset": "btc-target",
                    "conditionId": "btc-cond",
                    "size": 100,
                    "price": 0.7,
                    "timestamp": now + 800,
                    "title": "Will Bitcoin hit $100k?",
                    "outcome": "Yes",
                    "transactionHash": "0xtax2",
                },
            ]
        )
        result = run_backtest(
            [Trade.from_api(item) for item in raw],
            BacktestConfig(min_wallet_score=0.2, min_trade_usdc=20, copy_delay_seconds=0, warmup_trades=3),
        )

        self.assertTrue(result["by_category"])
        self.assertEqual(result["by_category"][0]["category"], "crypto")


if __name__ == "__main__":
    unittest.main()
