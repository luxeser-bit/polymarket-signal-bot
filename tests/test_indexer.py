from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from polymarket_signal_bot.indexer import (
    BlockInfo,
    IndexerStore,
    ORDER_FILLED_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    chunk_ranges,
    decode_log,
    topic_to_address,
)


def _topic_address(address: str) -> str:
    return "0x" + ("0" * 24) + address.lower().replace("0x", "")


def _word(value: int) -> str:
    return f"{value:064x}"


class IndexerTests(unittest.TestCase):
    def test_store_schema_creates_raw_tables_and_wal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "polysignal.db"
            with IndexerStore(db_path) as store:
                store.init_schema()
                journal_mode = store.conn.execute("PRAGMA journal_mode").fetchone()[0]
                tables = {
                    row[0]
                    for row in store.conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }

            self.assertEqual(journal_mode.lower(), "wal")
            self.assertIn("raw_transactions", tables)
            self.assertIn("indexer_state", tables)
            self.assertIn("indexer_blocks", tables)

    def test_decode_order_filled_buy(self) -> None:
        maker = "0x1111111111111111111111111111111111111111"
        taker = "0x2222222222222222222222222222222222222222"
        token_id = 12345
        maker_amount = 450_000
        taker_amount = 1_000_000
        log = {
            "address": "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
            "blockNumber": "0x64",
            "blockHash": "0xabc",
            "transactionHash": "0xtx",
            "logIndex": "0x7",
            "topics": [
                ORDER_FILLED_TOPIC,
                "0x" + ("a" * 64),
                _topic_address(maker),
                _topic_address(taker),
            ],
            "data": "0x"
            + _word(0)
            + _word(token_id)
            + _word(maker_amount)
            + _word(taker_amount)
            + _word(0),
        }

        rows = decode_log(log, block_map={100: BlockInfo(100, "0xabc", 1710000000)})

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "OrderFilled")
        self.assertEqual(rows[0].user_address, maker)
        self.assertEqual(rows[0].market_id, str(token_id))
        self.assertEqual(rows[0].side, "BUY")
        self.assertAlmostEqual(rows[0].price, 0.45)
        self.assertAlmostEqual(rows[0].amount, 1.0)

    def test_decode_transfer_single_mint(self) -> None:
        to_address = "0x3333333333333333333333333333333333333333"
        log = {
            "address": "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
            "blockNumber": "0x65",
            "blockHash": "0xdef",
            "transactionHash": "0xtx2",
            "logIndex": "0x8",
            "topics": [
                TRANSFER_SINGLE_TOPIC,
                _topic_address("0x4444444444444444444444444444444444444444"),
                _topic_address("0x0000000000000000000000000000000000000000"),
                _topic_address(to_address),
            ],
            "data": "0x" + _word(999) + _word(2_500_000),
        }

        rows = decode_log(log, block_map={101: BlockInfo(101, "0xdef", 1710000001)})

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "TransferSingle")
        self.assertEqual(rows[0].user_address, to_address)
        self.assertEqual(rows[0].market_id, "999")
        self.assertEqual(rows[0].side, "MINT")
        self.assertAlmostEqual(rows[0].amount, 2.5)

    def test_chunk_ranges(self) -> None:
        self.assertEqual(chunk_ranges(1, 5, 2), [(1, 2), (3, 4), (5, 5)])
        self.assertEqual(chunk_ranges(5, 1, 2), [])

    def test_topic_to_address(self) -> None:
        address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        self.assertEqual(topic_to_address(_topic_address(address)), address)

    def test_src_indexer_import(self) -> None:
        import src.indexer as indexer

        self.assertTrue(callable(indexer.main))


if __name__ == "__main__":
    unittest.main()
