from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from server import api


class ServerApiTests(unittest.TestCase):
    def test_component_specs_use_existing_entrypoints_and_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = api.ServerSettings(
                main_db=root / "polysignal.db",
                indexer_db=root / "indexer.db",
                paper_state_db=root / "paper_state.db",
                dry_run=True,
            )

            specs = api.component_specs(settings)

        self.assertEqual(specs["indexer"].command[:3], (api.sys.executable, "-m", "src.indexer"))
        self.assertIn("--sync", specs["indexer"].command)
        self.assertIn("monitor", specs["monitor"].command)
        self.assertEqual(specs["live_paper"].command[:3], (api.sys.executable, "-m", "polymarket_signal_bot.live_paper_runner"))
        self.assertIn("--dry-run", specs["live_paper"].command)

    def test_indexer_metrics_reads_raw_events_and_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE raw_transactions (hash TEXT PRIMARY KEY);
                CREATE TABLE indexer_state (
                    name TEXT PRIMARY KEY,
                    last_block INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                INSERT INTO raw_transactions(hash) VALUES ('0x1'), ('0x2');
                INSERT INTO indexer_state(name, last_block, updated_at)
                    VALUES ('polygon', 123, 1700000000);
                """
            )
            conn.close()
            settings = api.ServerSettings(indexer_db=db_path)

            with patch.object(api, "component_status", return_value={"running": False}):
                metrics = api.indexer_metrics(settings)

        self.assertEqual(metrics["raw_events"], 2)
        self.assertEqual(metrics["last_block"], 123)
        self.assertGreater(metrics["progress"], 0)

    def test_positions_snapshot_reads_open_positions_and_balance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper_state.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE positions (
                    id TEXT PRIMARY KEY,
                    market_id TEXT,
                    side TEXT,
                    size REAL,
                    entry_price REAL,
                    tp_pct REAL,
                    sl_pct REAL,
                    status TEXT,
                    opened_at INTEGER,
                    closed_at INTEGER,
                    pnl REAL
                );
                CREATE TABLE balance_history (
                    timestamp INTEGER,
                    balance REAL,
                    pnl REAL
                );
                INSERT INTO positions VALUES ('p1', 'm1', 'BUY', 10, 0.5, 0.4, 0.2, 'OPEN', 100, NULL, 0);
                INSERT INTO positions VALUES ('p2', 'm2', 'BUY', 10, 0.5, 0.4, 0.2, 'CLOSED', 100, 200, 1);
                INSERT INTO balance_history VALUES (100, 200, 0), (200, 207, 7);
                """
            )
            conn.close()
            settings = api.ServerSettings(paper_state_db=db_path)

            snapshot = api.positions_snapshot(settings)

        self.assertEqual(snapshot["balance"], 207)
        self.assertEqual(snapshot["pnl"], 7)
        self.assertEqual(snapshot["open_positions_count"], 1)
        self.assertEqual(snapshot["total_positions"], 2)

    def test_wallet_metrics_reads_scores_and_cohorts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE scored_wallets (
                    wallet TEXT PRIMARY KEY,
                    computed_at INTEGER,
                    score REAL,
                    pnl REAL,
                    sharpe REAL,
                    max_drawdown REAL,
                    profit_factor REAL,
                    win_rate REAL,
                    avg_hold_time REAL,
                    volume REAL,
                    trade_count INTEGER,
                    consistency REAL
                );
                CREATE TABLE wallet_cohorts (
                    wallet TEXT PRIMARY KEY,
                    status TEXT,
                    stability_score REAL,
                    score REAL,
                    pnl REAL,
                    volume REAL,
                    trade_count INTEGER,
                    win_rate REAL,
                    profit_factor REAL,
                    max_drawdown REAL,
                    updated_at INTEGER
                );
                INSERT INTO scored_wallets VALUES ('0xaaa', 100, 0.9, 10, 1, 0, 2, 0.7, 60, 1000, 12, 0.8);
                INSERT INTO wallet_cohorts VALUES ('0xaaa', 'STABLE', 0.9, 0.9, 10, 1000, 12, 0.7, 2, 0, 100);
                """
            )
            conn.close()
            settings = api.ServerSettings(indexer_db=db_path)

            result = api.wallet_metrics(settings)

        self.assertEqual(result["counts"]["STABLE"], 1)
        self.assertEqual(result["scored_wallets"], 1)
        self.assertEqual(result["top_wallets"][0]["wallet"], "0xaaa")
        self.assertEqual(result["top_wallets"][0]["status"], "STABLE")


if __name__ == "__main__":
    unittest.main()
