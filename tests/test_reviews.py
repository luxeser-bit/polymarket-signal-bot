from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.scoring import score_wallets
from polymarket_signal_bot.signals import SignalConfig, generate_signals
from polymarket_signal_bot.storage import Store


class ReviewTests(unittest.TestCase):
    def test_inserted_signals_create_review_queue_items(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/reviews.db") as store:
                store.init_schema()
                store.upsert_wallets(demo_wallets())
                store.insert_trades(demo_trades(now))
                wallets = store.fetch_wallets()
                scores = score_wallets(
                    wallets,
                    {
                        wallet.address: store.fetch_trades_for_wallet(wallet.address)
                        for wallet in wallets
                    },
                )
                store.upsert_scores(scores)
                signals = generate_signals(
                    store.fetch_recent_trades(limit=100),
                    store.fetch_scores(min_score=0.5),
                    SignalConfig(min_wallet_score=0.5, min_trade_usdc=20, lookback_minutes=24 * 60),
                    now=now,
                )
                store.insert_signals(signals)

                pending = store.fetch_review_queue("PENDING", limit=10)
                self.assertEqual(len(pending), len(signals))
                self.assertTrue(store.set_signal_review_status(signals[0].signal_id, "APPROVED", "ok"))
                approved = store.fetch_approved_unopened_signals(limit=10)

        self.assertEqual(len(approved), 1)


if __name__ == "__main__":
    unittest.main()
