from __future__ import annotations

import tempfile
import unittest

from pathlib import Path

from polymarket_signal_bot.analytics import (
    duckdb_available,
    format_bytes,
    missing_duckdb_message,
    refresh_analytics_snapshot,
    sqlite_counts,
)
from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.storage import Store


class AnalyticsTests(unittest.TestCase):
    def test_sqlite_counts_report_exportable_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/analytics.db") as store:
                store.init_schema()
                store.upsert_wallets(demo_wallets())
                store.insert_trades(demo_trades())

            counts = sqlite_counts(f"{tmpdir}/analytics.db")

        self.assertEqual(counts["wallets"], 3)
        self.assertEqual(counts["trades"], 5)

    def test_format_bytes_and_missing_message(self) -> None:
        self.assertEqual(format_bytes(1024), "1.0 KB")
        self.assertIn("duckdb", missing_duckdb_message().lower())

    @unittest.skipUnless(duckdb_available(), "duckdb not installed")
    def test_refresh_analytics_snapshot_updates_runtime_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/analytics.db"
            duck_path = Path(tmpdir) / "analytics.duckdb"
            with Store(db_path) as store:
                store.init_schema()
                store.upsert_wallets(demo_wallets())
                store.insert_trades(demo_trades())
                result = refresh_analytics_snapshot(store, duckdb_path=duck_path, rebuild=True)
                runtime = store.runtime_state()
                exists = duck_path.exists()
            from polymarket_signal_bot.analytics import analytics_report

            report = analytics_report(duck_path, limit=5)

        self.assertTrue(result["ok"])
        self.assertTrue(exists)
        self.assertEqual(runtime["analytics_status"]["value"], "ready")
        self.assertIn("cohorts", report)
        self.assertTrue(report["cohorts"])


if __name__ == "__main__":
    unittest.main()
