from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path

from polymarket_signal_bot.dune_fetcher import (
    DUNE_SOURCE_COUNTER,
    DuneTradeStore,
    build_market_trades_sql,
    dedupe_trades,
)


class DuneFetcherTests(unittest.TestCase):
    def test_build_market_trades_sql_filters_dates_and_action(self) -> None:
        sql = build_market_trades_sql(
            start_date=date(2023, 1, 1),
            end_date=date(2023, 1, 31),
            full=False,
            include_action=True,
        )

        self.assertIn("polymarket_polygon.market_trades", sql)
        self.assertIn("CAST(action AS VARCHAR) AS action", sql)
        self.assertIn("block_time >= DATE '2023-01-01'", sql)
        self.assertIn("block_time < DATE '2023-01-31' + INTERVAL '1' DAY", sql)

    def test_dedupe_trades_groups_maker_taker_duplicates(self) -> None:
        rows = [
            {
                "block_time": "2024-01-01 00:00:00",
                "block_number": 100,
                "tx_hash": "0xaaa",
                "maker": "0x1111111111111111111111111111111111111111",
                "taker": "0x2222222222222222222222222222222222222222",
                "token_id": "1",
                "price": 0.5,
                "amount": 10,
                "side": "BUY",
                "market_id": "condition",
                "action": "maker",
            },
            {
                "block_time": "2024-01-01 00:00:00",
                "block_number": 100,
                "tx_hash": "0xaaa",
                "maker": "0x3333333333333333333333333333333333333333",
                "taker": "0x4444444444444444444444444444444444444444",
                "token_id": "1",
                "price": 0.5,
                "amount": 10,
                "side": "BUY",
                "market_id": "condition",
                "action": "taker",
            },
        ]

        result = dedupe_trades(rows)

        self.assertEqual(result.raw_rows, 2)
        self.assertEqual(result.deduped_rows, 1)
        self.assertEqual(result.duplicate_rows, 1)
        self.assertEqual(result.trades[0].action, "taker")

    def test_store_skips_existing_tx_block_and_updates_counters(self) -> None:
        rows = [
            {
                "block_time": "2024-01-01T00:00:00Z",
                "block_number": 100,
                "tx_hash": "0xaaa",
                "maker": "0x1111111111111111111111111111111111111111",
                "taker": "0x2222222222222222222222222222222222222222",
                "token_id": "1",
                "price": 0.5,
                "amount": 10,
                "side": "BUY",
                "market_id": "condition",
            },
            {
                "block_time": "2024-01-01T00:01:00Z",
                "block_number": 100,
                "tx_hash": "0xaaa",
                "maker": "0x3333333333333333333333333333333333333333",
                "taker": "0x4444444444444444444444444444444444444444",
                "token_id": "2",
                "price": 0.7,
                "amount": 20,
                "side": "SELL",
                "market_id": "condition-2",
            },
        ]
        trades = dedupe_trades(rows).trades
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            with DuneTradeStore(db_path) as store:
                result = store.insert_trades(trades)
                again = store.insert_trades(trades)

            conn = sqlite3.connect(db_path)
            counter = conn.execute(
                "SELECT value FROM indexer_counters WHERE name = ?",
                (DUNE_SOURCE_COUNTER,),
            ).fetchone()[0]
            raw_counter = conn.execute(
                "SELECT value FROM indexer_counters WHERE name = 'raw_events'"
            ).fetchone()[0]
            rows_count = conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0]
            conn.close()

        self.assertEqual(result.inserted, 1)
        self.assertEqual(result.skipped_existing, 1)
        self.assertEqual(again.inserted, 0)
        self.assertEqual(counter, 1)
        self.assertEqual(raw_counter, 1)
        self.assertEqual(rows_count, 1)


if __name__ == "__main__":
    unittest.main()
