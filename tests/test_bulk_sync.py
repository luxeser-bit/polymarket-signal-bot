from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.bulk_sync import BulkSync, BulkSyncConfig
from polymarket_signal_bot.models import Wallet
from polymarket_signal_bot.storage import Store


class FakeClient:
    def __init__(self, now: int) -> None:
        self.now = now

    def user_activity(self, user: str, *, limit: int, offset: int, start: int, types: list[str]):
        del start, types
        rows = []
        for index in range(offset, min(offset + limit, 3)):
            rows.append(
                {
                    "proxyWallet": user,
                    "side": "BUY",
                    "asset": f"asset-{index}",
                    "conditionId": f"cond-{index}",
                    "size": 100 + index,
                    "price": 0.4,
                    "timestamp": self.now + index,
                    "title": "Bulk sync test",
                    "outcome": "Yes",
                    "transactionHash": f"0xbulk{index}",
                }
            )
        return rows


class BulkSyncTests(unittest.TestCase):
    def test_bulk_sync_updates_checkpoint(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/bulk.db") as store:
                store.init_schema()
                store.upsert_wallets([Wallet(address="0x1111111111111111111111111111111111111111")])
                summary = BulkSync(
                    store=store,
                    client=FakeClient(now),
                    config=BulkSyncConfig(wallet_limit=1, page_size=2, max_pages_per_wallet=3),
                ).run_once()
                state = store.wallet_sync_state("0x1111111111111111111111111111111111111111")

        self.assertEqual(summary["seen"], 3)
        self.assertEqual(summary["inserted"], 3)
        self.assertEqual(state["total_trades_seen"], 3)
        self.assertGreaterEqual(state["last_trade_ts"], now)


if __name__ == "__main__":
    unittest.main()
