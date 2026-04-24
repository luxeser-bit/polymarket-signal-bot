from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from polymarket_signal_bot.cli import read_wallet_file
from polymarket_signal_bot.models import Trade, Wallet
from polymarket_signal_bot.storage import Store


class WatchlistTests(unittest.TestCase):
    def test_discovers_new_wallets_from_trade_flow_without_overwriting_existing(self) -> None:
        now = int(time.time())
        existing = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        strong = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        weak = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        small = "0xcccccccccccccccccccccccccccccccccccccccc"
        old = "0xdddddddddddddddddddddddddddddddddddddddd"

        trades = [
            self.trade("existing-1", existing, 1000, 0.5, now),
            self.trade("strong-1", strong.upper(), 500, 0.5, now),
            self.trade("strong-2", strong, 500, 0.5, now - 60),
            self.trade("weak-1", weak, 600, 0.5, now),
            self.trade("small-1", small, 20, 0.5, now),
            self.trade("small-2", small, 20, 0.5, now),
            self.trade("old-1", old, 500, 0.5, now - 10 * 86400),
            self.trade("old-2", old, 500, 0.5, now - 10 * 86400 + 1),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/watchlist.db") as store:
                store.init_schema()
                store.upsert_wallets([Wallet(address=existing, source="leaderboard", pnl=123.0, volume=999.0)])
                store.insert_trades(trades)

                discovered = store.discover_wallets_from_trades(
                    limit=10,
                    min_notional=100,
                    min_trades=2,
                    since_ts=now - 86400,
                )
                store.insert_wallets_ignore_existing(discovered)
                wallets = {wallet.address: wallet for wallet in store.fetch_wallets()}

        self.assertEqual([wallet.address for wallet in discovered], [strong])
        self.assertEqual(discovered[0].source, "trade-flow")
        self.assertEqual(discovered[0].volume, 500.0)
        self.assertEqual(wallets[existing].source, "leaderboard")
        self.assertEqual(wallets[existing].pnl, 123.0)
        self.assertIn(strong, wallets)
        self.assertNotIn(weak, wallets)
        self.assertNotIn(small, wallets)
        self.assertNotIn(old, wallets)

    def test_insert_wallets_ignore_existing_preserves_existing_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/watchlist.db") as store:
                store.init_schema()
                store.upsert_wallets(
                    [
                        Wallet(
                            address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                            source="leaderboard",
                            pnl=42.0,
                            volume=1000.0,
                        )
                    ]
                )
                inserted = store.insert_wallets_ignore_existing(
                    [
                        Wallet(address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", source="file"),
                        Wallet(address="0xffffffffffffffffffffffffffffffffffffffff", source="file"),
                    ]
                )
                wallets = {wallet.address: wallet for wallet in store.fetch_wallets()}

        self.assertEqual(inserted, 1)
        self.assertEqual(wallets["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"].source, "leaderboard")
        self.assertEqual(wallets["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"].pnl, 42.0)
        self.assertEqual(wallets["0xffffffffffffffffffffffffffffffffffffffff"].source, "file")

    def test_wallet_file_reader_skips_comments_and_blank_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "wallets.txt"
            path.write_text(
                "\n# seed wallets\n  0xabc  \n\n0xdef\n   # ignored\n",
                encoding="utf-8",
            )

            wallets = read_wallet_file(path)

        self.assertEqual(wallets, ["0xabc", "0xdef"])

    @staticmethod
    def trade(trade_id: str, wallet: str, size: float, price: float, timestamp: int) -> Trade:
        return Trade(
            trade_id=trade_id,
            proxy_wallet=wallet,
            side="BUY",
            asset=f"asset-{trade_id}",
            condition_id=f"cond-{trade_id}",
            size=size,
            price=price,
            timestamp=timestamp,
            title="Watchlist test",
            outcome="Yes",
        )


if __name__ == "__main__":
    unittest.main()
