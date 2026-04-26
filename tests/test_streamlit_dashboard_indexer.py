from __future__ import annotations

import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import dashboard


class StreamlitDashboardIndexerTests(unittest.TestCase):
    def test_indexer_snapshot_reads_records_and_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
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
                CREATE TABLE indexer_state (
                    name TEXT PRIMARY KEY,
                    last_block INTEGER NOT NULL,
                    last_block_hash TEXT NOT NULL DEFAULT '',
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE training_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at INTEGER NOT NULL,
                    ok INTEGER NOT NULL,
                    raw_transactions INTEGER NOT NULL,
                    scored_wallets INTEGER NOT NULL DEFAULT 0,
                    stable_wallets INTEGER NOT NULL DEFAULT 0,
                    candidate_wallets INTEGER NOT NULL DEFAULT 0,
                    watch_wallets INTEGER NOT NULL DEFAULT 0,
                    noise_wallets INTEGER NOT NULL DEFAULT 0,
                    exit_examples INTEGER NOT NULL DEFAULT 0,
                    summary_json TEXT NOT NULL
                );
                CREATE TABLE wallet_cohorts (
                    wallet TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    stability_score REAL NOT NULL,
                    score REAL NOT NULL,
                    pnl REAL NOT NULL,
                    volume REAL NOT NULL,
                    trade_count INTEGER NOT NULL,
                    win_rate REAL NOT NULL,
                    profit_factor REAL NOT NULL,
                    max_drawdown REAL NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                """
            )
            conn.execute(
                """
                INSERT INTO raw_transactions(
                    hash, log_index, block_number, timestamp, event_type, inserted_at
                )
                VALUES ('0x1', 1, 100, 1700000000, 'OrderFilled', 1700000000)
                """
            )
            conn.execute(
                "INSERT INTO indexer_state(name, last_block, updated_at) VALUES ('polygon', 12345, ?)",
                (int(time.time()),),
            )
            conn.execute(
                """
                INSERT INTO training_runs(
                    started_at, ok, raw_transactions, scored_wallets, stable_wallets,
                    candidate_wallets, watch_wallets, noise_wallets, exit_examples, summary_json
                )
                VALUES (?, 1, 1, 2, 1, 1, 0, 0, 7, '{}')
                """,
                (int(time.time()),),
            )
            conn.executemany(
                """
                INSERT INTO wallet_cohorts(
                    wallet, status, stability_score, score, pnl, volume, trade_count,
                    win_rate, profit_factor, max_drawdown, updated_at
                )
                VALUES (?, ?, 0.7, 0.7, 1, 100, 10, 0.6, 1.2, 0.1, ?)
                """,
                [("0xaaa", "STABLE", int(time.time())), ("0xbbb", "CANDIDATE", int(time.time()))],
            )
            conn.commit()
            conn.close()

            with patch.object(dashboard, "_indexer_process_running", return_value=False):
                snapshot = dashboard._indexer_snapshot(db_path)

        self.assertTrue(snapshot["db_exists"])
        self.assertTrue(snapshot["schema_ready"])
        self.assertEqual(snapshot["records"], 1)
        self.assertEqual(snapshot["last_block"], 12345)
        self.assertFalse(snapshot["running"])
        self.assertTrue(snapshot["recent_checkpoint"])
        self.assertTrue(snapshot["last_training_ok"])
        self.assertEqual(snapshot["training_scored_wallets"], 2)
        self.assertEqual(snapshot["exit_examples"], 7)
        self.assertEqual(snapshot["cohort_counts"]["STABLE"], 1)

    def test_indexer_db_path_uses_env(self) -> None:
        with patch.dict("os.environ", {"INDEXER_DB_PATH": "data/custom-indexer.db"}):
            self.assertEqual(
                dashboard._indexer_db_path(),
                dashboard.ROOT / "data" / "custom-indexer.db",
            )

    def test_indexer_speed_uses_block_delta(self) -> None:
        speed = dashboard._indexer_speed(
            {"last_block": 130},
            {"last_block": 100, "at": time.time() - 10},
        )

        self.assertGreater(speed, 2.0)

    def test_indexer_live_label_is_conditional(self) -> None:
        live_labels = dashboard._dashboard_tab_labels(indexer_live=True)
        idle_labels = dashboard._dashboard_tab_labels(indexer_live=False)

        self.assertIn("● Live", live_labels["indexer"])
        self.assertNotIn("● Live", idle_labels["indexer"])

    def test_indexer_update_active_follows_process_state(self) -> None:
        with patch.object(dashboard, "_indexer_process_running", return_value=False):
            self.assertFalse(dashboard._indexer_update_active(Path("missing.db")))
        with patch.object(dashboard, "_indexer_process_running", return_value=True):
            self.assertTrue(dashboard._indexer_update_active(Path("missing.db")))

    def test_system_component_specs_use_absolute_paths_and_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict("os.environ", {"DRY_RUN": "true"}, clear=False), patch.object(
                dashboard, "load_dotenv", None
            ):
                specs = dashboard._system_component_specs(
                    root / "polysignal.db",
                    root / "paper_state.db",
                    root / "indexer.db",
                )

        self.assertEqual(specs["indexer"]["command"][0], sys.executable)
        self.assertIn("--sync", specs["indexer"]["command"])
        self.assertIn("--db", specs["indexer"]["command"])
        self.assertIn("--dry-run", specs["live_paper"]["command"])
        self.assertIn("monitor", specs["monitor"]["command"])
        self.assertTrue(Path(specs["indexer"]["command"][-1]).is_absolute())

    def test_system_status_html_shows_running_and_stopped(self) -> None:
        html = dashboard._system_status_html(
            {
                "indexer": {"label": "Indexer", "running": True, "pid": 11},
                "monitor": {"label": "Monitor", "running": False, "pid": 0},
                "live_paper": {"label": "Live Paper", "running": False, "pid": 0},
            }
        )

        self.assertIn("system-dot on", html)
        self.assertIn("PID 11", html)
        self.assertIn("STOPPED", html)

    def test_dashboard_autostart_accepts_cli_flag(self) -> None:
        with patch.object(sys, "argv", ["streamlit", "--autostart"]):
            self.assertTrue(dashboard._dashboard_autostart_requested())


if __name__ == "__main__":
    unittest.main()
