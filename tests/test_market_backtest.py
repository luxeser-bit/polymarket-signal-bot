from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.backtest import BacktestConfig, run_backtest
from polymarket_signal_bot.models import OrderBookSnapshot, Trade
from polymarket_signal_bot.storage import Store


class MarketBacktestTests(unittest.TestCase):
    def test_order_book_snapshot_metrics_are_stored(self) -> None:
        raw = {
            "market": "0xabc",
            "asset_id": "token-1",
            "timestamp": "1000",
            "bids": [{"price": "0.48", "size": "1000"}],
            "asks": [{"price": "0.50", "size": "900"}],
            "last_trade_price": "0.49",
        }
        book = OrderBookSnapshot.from_api(raw)

        self.assertEqual(book.asset, "token-1")
        self.assertAlmostEqual(book.spread, 0.02)
        self.assertGreater(book.liquidity_score, 0.2)

        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/books.db") as store:
                store.init_schema()
                store.upsert_order_books([book], raw_by_asset={"token-1": raw})
                fetched = store.latest_order_book("token-1")
                history = store.order_book_history_summary()

        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.asset, "token-1")
        self.assertEqual(int(history["snapshots"]), 1)
        self.assertEqual(int(history["assets"]), 1)

    def test_backtest_replays_copy_trade_with_delay(self) -> None:
        now = int(time.time()) - 3600
        wallet = "0x1111111111111111111111111111111111111111"
        raw = []
        for index in range(8):
            raw.append(
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": f"warm-{index}",
                    "conditionId": f"cond-{index}",
                    "size": 200,
                    "price": 0.35,
                    "timestamp": now + index * 60,
                    "title": "Warmup",
                    "outcome": "Yes",
                    "transactionHash": f"0xwarm{index}",
                }
            )
        raw.extend(
            [
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": "target",
                    "conditionId": "cond-target",
                    "size": 500,
                    "price": 0.40,
                    "timestamp": now + 600,
                    "title": "Target market",
                    "outcome": "Yes",
                    "transactionHash": "0xtarget1",
                },
                {
                    "proxyWallet": "0x2222222222222222222222222222222222222222",
                    "side": "BUY",
                    "asset": "target",
                    "conditionId": "cond-target",
                    "size": 100,
                    "price": 0.70,
                    "timestamp": now + 900,
                    "title": "Target market",
                    "outcome": "Yes",
                    "transactionHash": "0xtarget2",
                },
            ]
        )
        trades = [Trade.from_api(item, source="test") for item in raw]
        result = run_backtest(
            trades,
            BacktestConfig(
                min_wallet_score=0.20,
                min_trade_usdc=20,
                copy_delay_seconds=0,
                warmup_trades=4,
            ),
        )

        self.assertGreaterEqual(result["closed_trades"], 1)
        self.assertGreater(result["pnl"], 0)

    def test_backtest_reports_liquidity_buckets(self) -> None:
        now = int(time.time()) - 3600
        wallet = "0x1111111111111111111111111111111111111111"
        raw = []
        for index in range(8):
            raw.append(
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": f"warm-{index}",
                    "conditionId": f"cond-{index}",
                    "size": 200,
                    "price": 0.35,
                    "timestamp": now + index * 60,
                    "title": "Warmup",
                    "outcome": "Yes",
                    "transactionHash": f"0xwarm-liq{index}",
                }
            )
        raw.extend(
            [
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": "liquid-target",
                    "conditionId": "cond-liquid",
                    "size": 500,
                    "price": 0.40,
                    "timestamp": now + 600,
                    "title": "Liquid target",
                    "outcome": "Yes",
                    "transactionHash": "0xliquid1",
                },
                {
                    "proxyWallet": "0x2222222222222222222222222222222222222222",
                    "side": "BUY",
                    "asset": "liquid-target",
                    "conditionId": "cond-liquid",
                    "size": 100,
                    "price": 0.70,
                    "timestamp": now + 900,
                    "title": "Liquid target",
                    "outcome": "Yes",
                    "transactionHash": "0xliquid2",
                },
            ]
        )
        book = OrderBookSnapshot(
            asset="liquid-target",
            market="cond-liquid",
            timestamp=now + 590,
            best_bid=0.39,
            best_ask=0.40,
            mid=0.395,
            spread=0.01,
            bid_depth_usdc=3000,
            ask_depth_usdc=2800,
            liquidity_score=0.9,
            last_trade_price=0.4,
        )

        result = run_backtest(
            [Trade.from_api(item, source="test") for item in raw],
            BacktestConfig(min_wallet_score=0.20, min_trade_usdc=20, copy_delay_seconds=0, warmup_trades=4),
            order_books={"liquid-target": book},
        )

        self.assertEqual(result["by_liquidity"][0]["liquidity_bucket"], "deep")
        self.assertGreater(result["by_liquidity"][0]["pnl"], 0)
        self.assertTrue(any(item["liquidity_bucket"] == "deep" for item in result["sample"]))
        self.assertGreater(result["liquidity_coverage"], 0)

    def test_cohort_policy_backtest_blocks_one_day_burst(self) -> None:
        now = int(time.time()) - 3600
        wallet = "0x1111111111111111111111111111111111111111"
        raw = []
        for index in range(8):
            raw.append(
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": f"burst-{index}",
                    "conditionId": f"burst-cond-{index}",
                    "size": 250,
                    "price": 0.35,
                    "timestamp": now + index * 60,
                    "title": "Burst warmup",
                    "outcome": "Yes",
                    "transactionHash": f"0xburst{index}",
                }
            )
        raw.extend(
            [
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": "burst-target",
                    "conditionId": "burst-target-cond",
                    "size": 500,
                    "price": 0.40,
                    "timestamp": now + 600,
                    "title": "Burst target",
                    "outcome": "Yes",
                    "transactionHash": "0xbursttarget1",
                },
                {
                    "proxyWallet": "0x2222222222222222222222222222222222222222",
                    "side": "BUY",
                    "asset": "burst-target",
                    "conditionId": "burst-target-cond",
                    "size": 100,
                    "price": 0.72,
                    "timestamp": now + 900,
                    "title": "Burst target",
                    "outcome": "Yes",
                    "transactionHash": "0xbursttarget2",
                },
            ]
        )
        trades = [Trade.from_api(item, source="test") for item in raw]

        baseline = run_backtest(
            trades,
            BacktestConfig(min_wallet_score=0.20, min_trade_usdc=20, copy_delay_seconds=0, warmup_trades=4),
        )
        cohort = run_backtest(
            trades,
            BacktestConfig(
                min_wallet_score=0.20,
                min_trade_usdc=20,
                copy_delay_seconds=0,
                warmup_trades=4,
                use_cohort_policy=True,
            ),
        )

        self.assertGreater(baseline["closed_trades"], cohort["closed_trades"])
        self.assertGreater(cohort["skipped"], baseline["skipped"])
        self.assertTrue(cohort["cohort_policy"])

    def test_balanced_cohort_policy_can_auto_open_watch_wallets(self) -> None:
        now = int(time.time()) - 3600
        wallet = "0x1111111111111111111111111111111111111111"
        raw = []
        for index in range(8):
            raw.append(
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": f"watch-{index}",
                    "conditionId": f"watch-cond-{index}",
                    "size": 250,
                    "price": 0.35,
                    "timestamp": now + index * 60,
                    "title": "Watch warmup",
                    "outcome": "Yes",
                    "transactionHash": f"0xwatch{index}",
                }
            )
        raw.extend(
            [
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": "watch-target",
                    "conditionId": "watch-target-cond",
                    "size": 500,
                    "price": 0.40,
                    "timestamp": now + 600,
                    "title": "Watch target",
                    "outcome": "Yes",
                    "transactionHash": "0xwatchtarget1",
                },
                {
                    "proxyWallet": "0x2222222222222222222222222222222222222222",
                    "side": "BUY",
                    "asset": "watch-target",
                    "conditionId": "watch-target-cond",
                    "size": 100,
                    "price": 0.72,
                    "timestamp": now + 900,
                    "title": "Watch target",
                    "outcome": "Yes",
                    "transactionHash": "0xwatchtarget2",
                },
            ]
        )
        trades = [Trade.from_api(item, source="test") for item in raw]

        strict = run_backtest(
            trades,
            BacktestConfig(
                min_wallet_score=0.20,
                min_trade_usdc=20,
                copy_delay_seconds=0,
                warmup_trades=4,
                use_cohort_policy=True,
                cohort_policy_mode="strict",
            ),
        )
        balanced = run_backtest(
            trades,
            BacktestConfig(
                min_wallet_score=0.20,
                min_trade_usdc=20,
                copy_delay_seconds=0,
                warmup_trades=4,
                use_cohort_policy=True,
                cohort_policy_mode="balanced",
            ),
        )

        self.assertGreater(balanced["closed_trades"], strict["closed_trades"])
        self.assertEqual(balanced["cohort_policy_mode"], "balanced")


if __name__ == "__main__":
    unittest.main()
