from __future__ import annotations

import sqlite3
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
            conn.commit()
            conn.close()

            with patch.object(dashboard, "_indexer_process_running", return_value=False):
                snapshot = dashboard._indexer_snapshot(db_path)

        self.assertTrue(snapshot["db_exists"])
        self.assertTrue(snapshot["schema_ready"])
        self.assertEqual(snapshot["records"], 1)
        self.assertEqual(snapshot["last_block"], 12345)
        self.assertTrue(snapshot["running"])

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


if __name__ == "__main__":
    unittest.main()
