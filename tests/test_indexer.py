from __future__ import annotations

import tempfile
import unittest
import os
from pathlib import Path
from unittest.mock import patch

from polymarket_signal_bot.indexer import (
    BlockInfo,
    CTF_EXCHANGE_ADDRESS,
    ContractSpec,
    IndexerConfig,
    IndexerStore,
    ORDER_FILLED_TOPIC,
    PolygonEventIndexer,
    RawTransaction,
    RpcLogRangeTooLarge,
    TRANSFER_SINGLE_TOPIC,
    build_arg_parser,
    chunk_ranges,
    condition_id_from_tx_input,
    decode_log,
    default_contract_specs,
    topics_for_contract,
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

        rows = decode_log(
            log,
            block_map={100: BlockInfo(100, "0xabc", 1710000000)},
            topic_to_event={ORDER_FILLED_TOPIC: "OrderFilledV1"},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "OrderFilled")
        self.assertEqual(rows[0].user_address, maker)
        self.assertEqual(rows[0].market_id, str(token_id))
        self.assertEqual(rows[0].side, "BUY")
        self.assertAlmostEqual(rows[0].price, 0.45)
        self.assertAlmostEqual(rows[0].amount, 1.0)

    def test_decode_order_filled_v2_uses_condition_id_market(self) -> None:
        maker = "0x1111111111111111111111111111111111111111"
        taker = "0x2222222222222222222222222222222222222222"
        token_id = 12345
        condition_id = "0x" + ("c" * 64)
        v2_topic = "0x" + ("9" * 64)
        log = {
            "address": CTF_EXCHANGE_ADDRESS,
            "blockNumber": "0x64",
            "blockHash": "0xabc",
            "transactionHash": "0xtxv2",
            "logIndex": "0x9",
            "topics": [
                v2_topic,
                "0x" + ("a" * 64),
                _topic_address(maker),
                _topic_address(taker),
            ],
            "data": "0x"
            + _word(1)
            + _word(token_id)
            + _word(1_000_000)
            + _word(450_000)
            + _word(0)
            + _word(0)
            + _word(0),
        }

        rows = decode_log(
            log,
            block_map={100: BlockInfo(100, "0xabc", 1710000000)},
            topic_to_event={v2_topic: "OrderFilledV2"},
            tx_condition_ids={"0xtxv2": condition_id},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "OrderFilled")
        self.assertEqual(rows[0].user_address, maker)
        self.assertEqual(rows[0].market_id, condition_id)
        self.assertEqual(rows[0].side, "SELL")
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

    def test_condition_id_from_tx_input(self) -> None:
        condition_id = "0x" + ("b" * 64)
        self.assertEqual(condition_id_from_tx_input("0x12345678" + condition_id[2:]), condition_id)

    def test_default_contracts_use_current_exchange_addresses(self) -> None:
        addresses = {spec.normalized_address for spec in default_contract_specs()}
        self.assertIn("0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", addresses)
        self.assertIn("0xc5d563a36ae78145c45a50134d48a1215220f80a", addresses)
        self.assertIn("0xe111180000d2663c0091e4f400237545b87b996b", addresses)
        self.assertIn("0xe2222d279d744050d28e00520010520000310f59", addresses)

    def test_contract_topics_are_explicit(self) -> None:
        for contract in default_contract_specs():
            topics = topics_for_contract(contract)
            self.assertGreaterEqual(len(topics), len(contract.event_names))
            self.assertTrue(all(topic.startswith("0x") and len(topic) == 66 for topic in topics))

    def test_cli_defaults_are_rpc_friendly(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            args = build_arg_parser().parse_args([])
        self.assertEqual(args.chunk_size, 100)
        self.assertEqual(args.max_workers, 2)
        self.assertEqual(args.min_log_chunk_size, 10)

    def test_topic_to_address(self) -> None:
        address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        self.assertEqual(topic_to_address(_topic_address(address)), address)

    def test_src_indexer_import(self) -> None:
        import src.indexer as indexer

        self.assertTrue(callable(indexer.main))

    def test_historical_store_does_not_move_checkpoint_backwards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with IndexerStore(Path(tmp) / "state.db") as store:
                store.init_schema()
                store.set_last_block(500, "0xcurrent")
                indexer = PolygonEventIndexer(
                    IndexerConfig(rpc_url="http://rpc"),
                    store=store,
                    contracts=[],
                )

                inserted = indexer._store_chunk(
                    100,
                    110,
                    [
                        RawTransaction(
                            hash="0xtx",
                            log_index=1,
                            block_number=101,
                            block_hash="0xold",
                            timestamp=1700000000,
                            user_address="0x1111111111111111111111111111111111111111",
                            market_id="market",
                            side="BUY",
                            price=0.5,
                            amount=1.0,
                            event_type="OrderFilled",
                            contract=CTF_EXCHANGE_ADDRESS,
                            raw_json="{}",
                        )
                    ],
                    [BlockInfo(110, "0xold", 1700000010)],
                )

                self.assertEqual(inserted, 1)
                self.assertEqual(store.last_block(), 500)


class AdaptiveLogFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_logs_splits_rejected_ranges(self) -> None:
        class FakeRpc:
            def __init__(self) -> None:
                self.calls: list[tuple[int, int, tuple[str, ...]]] = []

            async def get_logs(
                self,
                *,
                address: str,
                from_block: int,
                to_block: int,
                topics: list[str] | None = None,
            ) -> list[dict[str, object]]:
                self.calls.append((from_block, to_block, tuple(topics or [])))
                if to_block - from_block + 1 > 10:
                    raise RpcLogRangeTooLarge("too many logs")
                return [{"address": address, "blockNumber": hex(from_block), "topics": topics or []}]

        with tempfile.TemporaryDirectory() as tmp:
            rpc = FakeRpc()
            contract = ContractSpec("Test", CTF_EXCHANGE_ADDRESS, ("OrderFilledV2",))
            with IndexerStore(Path(tmp) / "state.db") as store:
                indexer = PolygonEventIndexer(
                    IndexerConfig(rpc_url="http://rpc", min_log_chunk_size=10),
                    store=store,
                    contracts=[contract],
                )

                with self.assertLogs("polymarket_signal_bot.indexer", level="WARNING"):
                    logs = await indexer._fetch_logs_for_contract(
                        rpc, contract, 1, 25, topics_for_contract(contract)
                    )

        self.assertEqual(len(logs), 4)
        self.assertIn((1, 25, tuple(topics_for_contract(contract))), rpc.calls)
        self.assertTrue(all(call[2] for call in rpc.calls))

    async def test_legacy_exchange_contract_can_verify_with_bytecode_only(self) -> None:
        class FakeRpc:
            async def get_code(self, address: str) -> str:
                return "0x6000"

            async def eth_call(self, address: str, data: str) -> str:
                return "0x"

        with tempfile.TemporaryDirectory() as tmp:
            with IndexerStore(Path(tmp) / "state.db") as store:
                indexer = PolygonEventIndexer(
                    IndexerConfig(rpc_url="http://rpc"),
                    store=store,
                    contracts=[
                        ContractSpec(
                            "LegacyNegRiskCLOBExchange",
                            "0xc5d563a36ae78145c45a50134d48a1215220f80a",
                            ("OrderFilledV1",),
                        )
                    ],
                )

                await indexer._verify_contracts(FakeRpc())


if __name__ == "__main__":
    unittest.main()
