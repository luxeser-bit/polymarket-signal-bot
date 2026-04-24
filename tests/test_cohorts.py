from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.cohorts import CohortConfig, format_cohort_summary, wallet_cohort_report
from polymarket_signal_bot.models import Trade, Wallet
from polymarket_signal_bot.scoring import score_wallets
from polymarket_signal_bot.storage import Store


class CohortTests(unittest.TestCase):
    def test_cohort_report_ranks_stable_wallet_above_one_day_burst(self) -> None:
        now = int(time.time())
        stable = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        burst = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        tiny = "0xcccccccccccccccccccccccccccccccccccccccc"
        trades = []
        for day in range(4):
            trades.append(self.trade(f"stable-buy-{day}", stable, f"stable-{day}", 180, 0.5, now - day * 86400))
            trades.append(self.trade(f"stable-sell-{day}", stable, f"stable-{day}", 120, 0.56, now - day * 86400 + 120, side="SELL"))
        trades.extend(
            [
                self.trade("burst-1", burst, "burst", 1000, 0.5, now),
                self.trade("burst-2", burst, "burst", 900, 0.5, now + 60),
                self.trade("tiny-1", tiny, "tiny", 10, 0.5, now),
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/cohorts.db") as store:
                store.init_schema()
                wallets = [
                    Wallet(address=stable, source="leaderboard", pnl=500, volume=4000),
                    Wallet(address=burst, source="market-flow", pnl=0, volume=0),
                    Wallet(address=tiny, source="market-flow", pnl=0, volume=0),
                ]
                store.upsert_wallets(wallets)
                store.insert_trades(trades)
                store.upsert_scores(
                    score_wallets(
                        wallets,
                        {
                            wallet.address: store.fetch_trades_for_wallet(wallet.address)
                            for wallet in wallets
                        },
                    )
                )
                report = wallet_cohort_report(store, CohortConfig(history_days=10, min_notional=20, min_trades=1, limit=10))

        rows = {row["wallet"]: row for row in report["wallets"]}
        self.assertEqual(report["wallets"][0]["wallet"], stable)
        self.assertEqual(rows[stable]["status"], "STABLE")
        self.assertIn(rows[burst]["status"], {"WATCH", "NOISE"})
        self.assertNotIn(tiny, rows)
        self.assertGreater(rows[stable]["dayBalance"], rows[burst]["dayBalance"])
        self.assertIn("cohorts wallets=2", format_cohort_summary(report))

    @staticmethod
    def trade(
        trade_id: str,
        wallet: str,
        asset: str,
        size: float,
        price: float,
        timestamp: int,
        *,
        side: str = "BUY",
    ) -> Trade:
        return Trade(
            trade_id=trade_id,
            proxy_wallet=wallet,
            side=side,
            asset=asset,
            condition_id=f"cond-{asset}",
            size=size,
            price=price,
            timestamp=timestamp,
            title="Cohort test",
            outcome="Yes",
        )


if __name__ == "__main__":
    unittest.main()
