from __future__ import annotations

import tempfile
import unittest

from polymarket_signal_bot.dashboard import build_dashboard_state
from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.scoring import score_wallets
from polymarket_signal_bot.storage import Store


class DashboardTests(unittest.TestCase):
    def test_dashboard_state_has_terminal_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/dash.db") as store:
                store.init_schema()
                store.upsert_wallets(demo_wallets())
                store.insert_trades(demo_trades())
                wallets = store.fetch_wallets()
                scores = score_wallets(
                    wallets,
                    {
                        wallet.address: store.fetch_trades_for_wallet(wallet.address)
                        for wallet in wallets
                    },
                )
                store.upsert_scores(scores)

                state = build_dashboard_state(store, bankroll=200)

        self.assertEqual(state["meta"]["mode"], "PAPER")
        self.assertGreaterEqual(state["stats"]["wallets"], 3)
        self.assertIn("scanner", state)
        self.assertIn("risk", state)
        self.assertIn("orderBook", state)
        self.assertIn("cohorts", state)
        self.assertGreaterEqual(state["cohorts"]["totalWallets"], 1)


if __name__ == "__main__":
    unittest.main()
