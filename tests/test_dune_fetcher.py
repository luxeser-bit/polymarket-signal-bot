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
    normalize_dune_trade,
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
        self.assertIn("CAST(asset_id AS VARCHAR) AS token_id", sql)
        self.assertIn("CAST(evt_index AS BIGINT) AS evt_index", sql)
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

    def test_normalize_dune_trade_accepts_real_dune_asset_id(self) -> None:
        trade = normalize_dune_trade(
            {
                "block_time": "2024-01-01 00:00:00.000 UTC",
                "block_number": 100,
                "tx_hash": "0xaaa",
                "maker": "0x1111111111111111111111111111111111111111",
                "taker": "0x2222222222222222222222222222222222222222",
                "asset_id": "asset",
                "condition_id": "condition",
                "price": 0.5,
                "amount": 10,
                "side": "BUY",
                "evt_index": 229,
                "unique_key": "unique",
                "token_outcome": "Yes",
                "shares": 20,
                "contract_address": "0x3333333333333333333333333333333333333333",
            }
        )

        self.assertEqual(trade.token_id, "asset")
        self.assertEqual(trade.market_id, "condition")
        self.assertEqual(trade.evt_index, 229)
        self.assertEqual(trade.token_outcome, "Yes")

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

        self.assertEqual(result.inserted, 2)
        self.assertEqual(result.skipped_existing, 0)
        self.assertEqual(again.inserted, 0)
        self.assertEqual(again.skipped_existing, 2)
        self.assertEqual(counter, 2)
        self.assertEqual(raw_counter, 2)
        self.assertEqual(rows_count, 2)

    def test_store_allows_dune_trade_when_only_technical_logs_exist(self) -> None:
        rows = [
            {
                "block_time": "2024-01-01T00:00:00Z",
                "block_number": 101,
                "tx_hash": "0xbbb",
                "maker": "0x1111111111111111111111111111111111111111",
                "taker": "0x2222222222222222222222222222222222222222",
                "token_id": "1",
                "price": 0.5,
                "amount": 10,
                "side": "BUY",
                "market_id": "condition",
            }
        ]
        trades = dedupe_trades(rows).trades
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "indexer.db"
            with DuneTradeStore(db_path) as store:
                store.conn.execute(
                    """
                    INSERT INTO raw_transactions(
                        hash, log_index, block_number, block_hash, timestamp,
                        user_address, market_id, side, price, amount,
                        event_type, contract, raw_json, inserted_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "0xbbb",
                        12,
                        101,
                        "",
                        1704067200,
                        "0x1111111111111111111111111111111111111111",
                        "condition",
                        "",
                        0,
                        0,
                        "TransferSingle",
                        "conditional_tokens",
                        "{}",
                        1,
                    ),
                )
                store.conn.commit()
                result = store.insert_trades(trades)

            conn = sqlite3.connect(db_path)
            orderfilled_count = conn.execute(
                "SELECT COUNT(*) FROM raw_transactions WHERE event_type = 'OrderFilled'"
            ).fetchone()[0]
            rows_count = conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0]
            conn.close()

        self.assertEqual(result.inserted, 1)
        self.assertEqual(result.skipped_existing, 0)
        self.assertEqual(orderfilled_count, 1)
        self.assertEqual(rows_count, 2)


if __name__ == "__main__":
    unittest.main()
