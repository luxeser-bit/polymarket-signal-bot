from __future__ import annotations

import time
import unittest
import sqlite3
import tempfile
from pathlib import Path

from polymarket_signal_bot.backtest import BacktestConfig
from polymarket_signal_bot.models import Trade
from polymarket_signal_bot.policy_optimizer import (
    load_indexed_policy_trades,
    prepared_training_tables_available,
    run_policy_optimizer,
    run_policy_optimizer_from_db,
)


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

    def test_optimizer_can_load_prepared_indexed_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE scored_wallets (
                    wallet TEXT PRIMARY KEY,
                    score REAL NOT NULL
                );
                CREATE TABLE raw_transactions (
                    hash TEXT NOT NULL,
                    log_index INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    user_address TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    amount REAL NOT NULL
                );
                """
            )
            now = int(time.time()) - 1000
            wallet = "0x3333333333333333333333333333333333333333"
            conn.execute("INSERT INTO scored_wallets(wallet, score) VALUES (?, ?)", (wallet, 0.8))
            conn.executemany(
                """
                INSERT INTO raw_transactions(
                    hash, log_index, timestamp, user_address, market_id, side, price, amount
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("0xdb1", 1, now, wallet, "market-a", "BUY", 0.40, 100),
                    ("0xdb2", 2, now + 120, wallet, "market-a", "SELL", 0.48, 100),
                ],
            )
            conn.commit()
            conn.close()

            self.assertTrue(prepared_training_tables_available(db_path))
            trades = load_indexed_policy_trades(db_path, min_score=0.55, limit=10)
            result = run_policy_optimizer_from_db(
                db_path,
                BacktestConfig(min_wallet_score=0.55, min_trade_usdc=10, warmup_trades=1),
                history_days=1,
                limit=10,
            )

        self.assertEqual(len(trades), 2)
        self.assertEqual(result["source"]["trades"], 2)
        self.assertTrue(result["source"]["uses_scored_wallets"])


if __name__ == "__main__":
    unittest.main()
