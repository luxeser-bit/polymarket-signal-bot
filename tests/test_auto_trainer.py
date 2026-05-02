from __future__ import annotations

import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from polymarket_signal_bot.auto_trainer import _exit_examples, _fit_exit_model, run_full_training_cycle
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
            _promote_test_scores_for_new_cohorts(db_path)
            cohort_result = update_cohorts(db_path)
            self.assertEqual(cohort_result["updated"], 2)
            self.assertEqual(cohort_result["counts"]["STABLE"], 1)
            self.assertEqual(cohort_result["counts"]["CANDIDATE"], 1)
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

    def test_exit_examples_use_only_buy_to_nearest_sell_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            _seed_strict_exit_pairs(db_path)

            examples = _exit_examples(db_path, stable_wallets={"0xAAA"})

        self.assertEqual(len(examples), 2)
        self.assertTrue(all(example["entry_side"] == "BUY" for example in examples))
        self.assertTrue(all(example["exit_side"] == "SELL" for example in examples))
        self.assertEqual({example["entry_hash"] for example in examples}, {"0xbuy-1", "0xbuy-2"})
        self.assertEqual({example["exit_hash"] for example in examples}, {"0xsell-close"})
        hold_by_entry = {example["entry_hash"]: example["hold_seconds"] for example in examples}
        self.assertEqual(hold_by_entry["0xbuy-1"], 70)
        self.assertEqual(hold_by_entry["0xbuy-2"], 60)

    def test_exit_model_reports_train_test_metrics(self) -> None:
        examples = [
            {
                "entry_notional": float(index * 10),
                "volatility_proxy": float(index % 3),
                "hour_of_day": float(index % 24),
                "hold_seconds": float(index * 60),
            }
            for index in range(1, 11)
        ]

        model = _fit_exit_model(examples)

        self.assertIn(model["type"], {"ridge", "dummy_median"})
        self.assertEqual(model["train_examples"], 8)
        self.assertEqual(model["test_examples"], 2)
        self.assertIn("mae_seconds", model)
        self.assertIn("r2", model)

    def test_sql_scoring_uses_strict_pair_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            _seed_strict_scoring_metrics(db_path)

            scores = calculate_all(db_path)

        row = scores.set_index("wallet").loc["0xaaa"]
        self.assertEqual(row["user_address"], "0xaaa")
        self.assertEqual(int(row["trade_count"]), 5)
        self.assertAlmostEqual(float(row["win_rate"]), 0.5)
        self.assertAlmostEqual(float(row["profit_factor"]), 1.0)
        self.assertAlmostEqual(float(row["avg_hold_time"]), 75.0)
        self.assertAlmostEqual(float(row["consistency"]), 0.5)
        self.assertAlmostEqual(float(row["sharpe"]), 0.0)


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


def _seed_strict_scoring_metrics(db_path: Path) -> None:
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
    jan = 1704067200
    feb = 1706745600
    rows = [
        ("0xjan-buy", 1, 1, jan, "0xaaa", "market-a", "BUY", 0.40, 100, "OrderFilled", jan),
        ("0xjan-sell", 2, 2, jan + 100, "0xaaa", "market-a", "SELL", 0.60, 100, "OrderFilled", jan),
        ("0xfeb-buy-ignored", 3, 3, feb, "0xaaa", "market-a", "BUY", 0.70, 100, "OrderFilled", feb),
        ("0xfeb-buy", 4, 4, feb + 10, "0xaaa", "market-a", "BUY", 0.70, 100, "OrderFilled", feb),
        ("0xfeb-sell", 5, 5, feb + 60, "0xaaa", "market-a", "SELL", 0.50, 100, "OrderFilled", feb),
        ("0xtransfer-noise", 6, 6, feb + 120, "0xaaa", "market-a", "BUY", 0.99, 100, "TransferSingle", feb),
    ]
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
    conn.commit()
    conn.close()


def _seed_strict_exit_pairs(db_path: Path) -> None:
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
    now = int(time.time()) - 10_000
    rows = [
        ("0xsell-before", 1, 1, now + 1, "0xaaa", "market-a", "SELL", 0.30, 10, "OrderFilled", now),
        ("0xbuy-1", 2, 2, now + 10, "0xaaa", "market-a", "BUY", 0.40, 100, "OrderFilled", now),
        ("0xbuy-2", 3, 3, now + 20, "0xaaa", "market-a", "BUY", 0.42, 120, "OrderFilled", now),
        ("0xsell-close", 4, 4, now + 80, "0xaaa", "market-a", "SELL", 0.55, 100, "OrderFilled", now),
        ("0xsell-later", 5, 5, now + 110, "0xaaa", "market-a", "SELL", 0.65, 100, "OrderFilled", now),
        ("0xbuy-open", 6, 6, now + 120, "0xaaa", "market-open", "BUY", 0.20, 100, "OrderFilled", now),
        ("0xother-buy", 7, 7, now + 130, "0xbbb", "market-a", "BUY", 0.10, 100, "OrderFilled", now),
        ("0xother-sell", 8, 8, now + 170, "0xbbb", "market-a", "SELL", 0.15, 100, "OrderFilled", now),
        ("0xnoise-transfer", 9, 9, now + 100, "0xaaa", "market-a", "BUY", 0.99, 100, "TransferSingle", now),
    ]
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
    conn.commit()
    conn.close()


def _promote_test_scores_for_new_cohorts(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        UPDATE scored_wallets
        SET sharpe = 1.8,
            win_rate = 0.62,
            trade_count = 60,
            max_drawdown = 10,
            avg_hold_time = 600
        WHERE wallet = '0xaaa'
        """
    )
    conn.execute(
        """
        UPDATE scored_wallets
        SET sharpe = 1.1,
            win_rate = 0.52,
            trade_count = 25,
            max_drawdown = 25,
            avg_hold_time = 120
        WHERE wallet = '0xbbb'
        """
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
