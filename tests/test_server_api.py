from __future__ import annotations

import sqlite3
import tempfile
import unittest
import json
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

    def test_component_spec_accepts_live_paper_aliases(self) -> None:
        settings = api.ServerSettings()

        self.assertEqual(api.component_spec(settings, "indexer").key, "indexer")
        self.assertEqual(api.component_spec(settings, "paper").key, "live_paper")
        self.assertEqual(api.component_spec(settings, "live-paper").key, "live_paper")

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

    def test_block_speed_preserves_recent_nonzero_speed_between_checkpoints(self) -> None:
        api.METRICS_CURSOR.update(
            {"last_block": 0.0, "seen_at": 0.0, "current_speed": 0.0, "speed_at": 0.0}
        )

        with patch.object(api.time, "time", return_value=1000.0):
            self.assertEqual(api.block_speed(100), 0.0)
        with patch.object(api.time, "time", return_value=1010.0):
            self.assertEqual(api.block_speed(200), 10.0)
        with patch.object(api.time, "time", return_value=1011.0):
            self.assertEqual(api.block_speed(200), 10.0)
        with patch.object(api.time, "time", return_value=1041.1):
            self.assertEqual(api.block_speed(200), 0.0)

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
            examples_path = Path(tmp) / "exit_examples.json"
            stats_path = Path(tmp) / "exit_stats.json"
            examples_path.write_text(
                json.dumps(
                    [
                        {
                            "wallet": "0xaaa",
                            "market_id": "market-a",
                            "entry_ts": 100,
                            "exit_ts": 160,
                            "entry_price": 0.4,
                            "exit_price": 0.5,
                            "predicted_time": 75,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            stats_path.write_text(
                json.dumps({"model_type": "ridge", "median_hold_seconds": 300, "mae_seconds": 25, "r2": 0.42}),
                encoding="utf-8",
            )
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
                INSERT INTO scored_wallets VALUES ('0xbbb', 101, 0.4, 5, 2.5, 0, 1, 0.5, 30, 500, 5, 0.2);
                INSERT INTO wallet_cohorts VALUES ('0xaaa', 'STABLE', 0.9, 0.9, 10, 1000, 12, 0.7, 2, 0, 100);
                INSERT INTO wallet_cohorts VALUES ('0xbbb', 'CANDIDATE', 0.5, 0.4, 5, 500, 5, 0.5, 1, 0, 101);
                """
            )
            conn.close()
            settings = api.ServerSettings(indexer_db=db_path, exit_examples_path=examples_path, exit_stats_path=stats_path)

            result = api.wallet_metrics(settings)

        self.assertEqual(result["counts"]["STABLE"], 1)
        self.assertEqual(result["counts"]["CANDIDATE"], 1)
        self.assertEqual(result["scored_wallets"], 2)
        self.assertEqual(result["top_wallets"][0]["wallet"], "0xbbb")
        self.assertEqual(result["top_wallets"][0]["cohort"], "CANDIDATE")
        self.assertEqual(result["top_wallets"][0]["sharpe"], 2.5)
        self.assertEqual(result["scored_wallet_rows"][0]["wallet"], "0xbbb")
        self.assertEqual(result["exit_examples"]["examples"][0]["whale_address"], "0xaaa")
        self.assertAlmostEqual(result["exit_examples"]["examples"][0]["pnl_percent"], 25.0)
        self.assertEqual(result["model_metrics"]["model_type"], "ridge")
        self.assertEqual(result["model_metrics"]["median_hold_time"], 300)
        self.assertEqual(result["model_metrics"]["mae"], 25)
        self.assertEqual(result["model_metrics"]["r2"], 0.42)

    def test_training_status_reads_last_training_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            summary = {
                "ok": True,
                "raw_transactions": 25,
                "scored_wallets": 2,
                "cohorts": {"STABLE": 1, "CANDIDATE": 1},
            }
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE training_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO training_state VALUES ('last_training_summary', ?, 1700000000)",
                (json.dumps(summary),),
            )
            conn.commit()
            conn.close()
            settings = api.ServerSettings(indexer_db=db_path)

            result = api.training_status_snapshot(settings)

        self.assertFalse(result["running"])
        self.assertEqual(result["last_run"]["raw_transactions"], 25)
        self.assertEqual(result["last_run"]["updated_at"], 1700000000)

    def test_training_command_uses_auto_trainer_entrypoint(self) -> None:
        settings = api.ServerSettings(indexer_db=Path("data/indexer.db"))

        command = api.training_command(settings, test=True, limit=1000, force=False)

        self.assertEqual(command[:3], (api.sys.executable, "-m", "polymarket_signal_bot.auto_trainer"))
        self.assertIn("--db", command)
        self.assertIn("--test", command)
        self.assertIn("--limit", command)
        self.assertIn("--no-force", command)


if __name__ == "__main__":
    unittest.main()
