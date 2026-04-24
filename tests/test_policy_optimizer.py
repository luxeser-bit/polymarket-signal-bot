from __future__ import annotations

import time
import unittest

from polymarket_signal_bot.backtest import BacktestConfig
from polymarket_signal_bot.models import Trade
from polymarket_signal_bot.policy_optimizer import run_policy_optimizer


class PolicyOptimizerTests(unittest.TestCase):
    def test_optimizer_ranks_candidate_policies(self) -> None:
        now = int(time.time()) - 3600
        wallet = "0x1111111111111111111111111111111111111111"
        raw = []
        for index in range(8):
            raw.append(
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": f"opt-warm-{index}",
                    "conditionId": f"opt-cond-{index}",
                    "size": 250,
                    "price": 0.35,
                    "timestamp": now + index * 60,
                    "title": "Optimizer warmup",
                    "outcome": "Yes",
                    "transactionHash": f"0xoptwarm{index}",
                }
            )
        raw.extend(
            [
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "asset": "opt-target",
                    "conditionId": "opt-target-cond",
                    "size": 500,
                    "price": 0.40,
                    "timestamp": now + 600,
                    "title": "Optimizer target",
                    "outcome": "Yes",
                    "transactionHash": "0xopttarget1",
                },
                {
                    "proxyWallet": "0x2222222222222222222222222222222222222222",
                    "side": "BUY",
                    "asset": "opt-target",
                    "conditionId": "opt-target-cond",
                    "size": 100,
                    "price": 0.72,
                    "timestamp": now + 900,
                    "title": "Optimizer target",
                    "outcome": "Yes",
                    "transactionHash": "0xopttarget2",
                },
            ]
        )

        result = run_policy_optimizer(
            [Trade.from_api(item, source="test") for item in raw],
            BacktestConfig(min_wallet_score=0.20, min_trade_usdc=20, copy_delay_seconds=0, warmup_trades=4),
        )

        policies = {row["policy"] for row in result["rows"]}
        scores = [row["optimizer_score"] for row in result["rows"]]

        self.assertIn("baseline", policies)
        self.assertIn("balanced_cohort", policies)
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertEqual(result["recommended"], result["rows"][0])


if __name__ == "__main__":
    unittest.main()
