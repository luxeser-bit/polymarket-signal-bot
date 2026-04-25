from __future__ import annotations

import json
import tempfile
import time
import unittest

from polymarket_signal_bot.streaming import StreamConfig, StreamProcessor, normalize_market_payload
from polymarket_signal_bot.storage import Store


class FakeStreamClient:
    def trades(self, **kwargs):
        return [
            {
                "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "asset": "asset-1",
                "conditionId": "market-1",
                "side": "BUY",
                "size": "100",
                "price": "0.55",
                "timestamp": kwargs.get("timestamp", int(time.time())),
                "title": "Knicks vs Celtics",
                "outcome": "Knicks",
                "transactionHash": "0xabc",
            }
        ]


class StreamingTests(unittest.TestCase):
    def test_normalize_market_payload_splits_price_changes(self) -> None:
        raw = json.dumps(
            {
                "event_type": "price_change",
                "market": "market-1",
                "timestamp": "1766789469958",
                "price_changes": [
                    {"asset_id": "asset-1", "side": "BUY", "price": "0.5", "size": "20"},
                    {"asset_id": "asset-2", "side": "SELL", "price": "0.4", "size": "10"},
                ],
            }
        )

        events = normalize_market_payload(raw)

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["event_type"], "price_change")
        self.assertEqual(events[0]["asset"], "asset-1")
        self.assertEqual(events[0]["event_ts"], 1766789469)
        self.assertEqual(events[0]["notional"], 10.0)

    def test_flush_records_stream_book_and_reconciled_trade(self) -> None:
        now_ms = int(time.time()) * 1000
        book_event = normalize_market_payload(
            {
                "event_type": "book",
                "asset_id": "asset-1",
                "market": "market-1",
                "bids": [{"price": "0.54", "size": "100"}],
                "asks": [{"price": "0.56", "size": "100"}],
                "timestamp": str(now_ms),
            }
        )[0]
        trade_event = normalize_market_payload(
            {
                "event_type": "last_trade_price",
                "asset_id": "asset-1",
                "market": "market-1",
                "side": "BUY",
                "price": "0.55",
                "size": "100",
                "timestamp": str(now_ms),
            }
        )[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/stream.db") as store:
                store.init_schema()
                processor = StreamProcessor(
                    store,
                    client=FakeStreamClient(),
                    config=StreamConfig(reconcile_min_notional=10),
                )
                result = processor._flush([book_event, trade_event])
                summary = store.stream_event_summary()
                book = store.latest_order_book("asset-1")
                trades = store.fetch_recent_trades(limit=5)

        self.assertEqual(result.stream_events, 2)
        self.assertEqual(summary["events"], 2)
        self.assertIsNotNone(book)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].proxy_wallet, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(trades[0].source, "clob-stream-reconcile")


if __name__ == "__main__":
    unittest.main()
