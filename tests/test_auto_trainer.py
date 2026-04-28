from __future__ import annotations

import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from polymarket_signal_bot.auto_trainer import run_full_training_cycle
from polymarket_signal_bot.cohorts import load_wallet_cohorts, update_cohorts
from polymarket_signal_bot.scoring import calculate_all, save_scored_wallets


class AutoTrainerTests(unittest.TestCase):
    def test_sql_scoring_cohorts_and_training_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            model_path = Path(tmp) / "exit_model.pkl"
            stats_path = Path(tmp) / "exit_stats.json"
            examples_path = Path(tmp) / "exit_examples.json"
            _seed_raw_transactions(db_path)

            scores = calculate_all(db_path)
            self.assertEqual(set(scores["wallet"]), {"0xaaa", "0xbbb"})
            self.assertEqual(set(scores["address"]), {"0xaaa", "0xbbb"})
            self.assertIn("calculated_at", scores.columns)
            self.assertIn("sharpe", scores.columns)
            self.assertEqual(int(scores["trade_count"].sum()), 32)

            saved = save_scored_wallets(scores, db_path)
            self.assertEqual(saved, 2)
            conn = sqlite3.connect(db_path)
            try:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(scored_wallets)").fetchall()}
                self.assertIn("address", columns)
                self.assertIn("calculated_at", columns)
            finally:
                conn.close()
            cohort_result = update_cohorts(db_path)
            self.assertEqual(cohort_result["updated"], 2)
            self.assertIn("0xaaa", load_wallet_cohorts(db_path, statuses={"STABLE", "CANDIDATE", "WATCH"}))

            summary = run_full_training_cycle(
                db_path=db_path,
                model_path=model_path,
                stats_path=stats_path,
                examples_path=examples_path,
                test=True,
                limit=100,
            )

            self.assertTrue(summary["ok"])
            self.assertEqual(summary["raw_transactions"], 33)
            self.assertTrue(model_path.exists())
            self.assertTrue(stats_path.exists())
            self.assertTrue(examples_path.exists())
            conn = sqlite3.connect(db_path)
            try:
                self.assertIsNotNone(conn.execute("SELECT 1 FROM training_runs LIMIT 1").fetchone())
            finally:
                conn.close()


def _seed_raw_transactions(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE raw_transactions (
            hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            block_number INTEGER NOT NULL,
            block_hash TEXT NOT NULL DEFAULT '',
            timestamp INTEGER NOT NULL,
            user_address TEXT NOT NULL DEFAULT '',
            market_id TEXT NOT NULL DEFAULT '',
            side TEXT NOT NULL DEFAULT '',
            price REAL NOT NULL DEFAULT 0,
            amount REAL NOT NULL DEFAULT 0,
            event_type TEXT NOT NULL,
            contract TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '',
            inserted_at INTEGER NOT NULL,
            PRIMARY KEY(hash, log_index)
        );
        """
    )
    now = int(time.time()) - 200_000
    rows = []
    for index in range(32):
        wallet = "0xaaa" if index < 24 else "0xbbb"
        side = "SELL" if index % 3 == 0 else "BUY"
        rows.append(
            (
                f"0x{index}",
                index,
                index,
                now + index * 3600,
                wallet,
                f"market-{index % 5}",
                side,
                0.42 + index * 0.002,
                100 + index,
                "OrderFilled",
                now,
            )
        )
    conn.executemany(
        """
        INSERT INTO raw_transactions(
            hash, log_index, block_number, timestamp, user_address, market_id,
            side, price, amount, event_type, inserted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.execute(
        """
        INSERT INTO raw_transactions(
            hash, log_index, block_number, timestamp, user_address, market_id,
            side, price, amount, event_type, inserted_at
        )
        VALUES ('0xnoise', 999, 999, ?, '0xnoise', 'market-noise', 'BUY', 0.50, 500, 'TransferSingle', ?)
        """,
        (now, now),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
