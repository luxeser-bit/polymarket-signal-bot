from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.learning import wallet_outcome_report
from polymarket_signal_bot.models import PaperEvent
from polymarket_signal_bot.storage import Store


class LearningTests(unittest.TestCase):
    def test_wallet_outcome_report_groups_paper_events_by_wallet_and_category(self) -> None:
        now = int(time.time())
        wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/learning.db") as store:
                store.init_schema()
                store.insert_paper_events(
                    [
                        PaperEvent(
                            event_id="created-1",
                            event_at=now,
                            event_type="SIGNAL_CREATED",
                            signal_id="sig-1",
                            wallet=wallet,
                            asset="asset-1",
                            title="Knicks vs Celtics",
                            outcome="Knicks",
                            reason="created",
                            confidence=0.75,
                            wallet_score=0.8,
                        ),
                        PaperEvent(
                            event_id="opened-1",
                            event_at=now,
                            event_type="OPENED",
                            signal_id="sig-1",
                            position_id="pos-1",
                            wallet=wallet,
                            asset="asset-1",
                            title="Knicks vs Celtics",
                            outcome="Knicks",
                            reason="opened",
                            size_usdc=10,
                            confidence=0.75,
                            wallet_score=0.8,
                        ),
                        PaperEvent(
                            event_id="closed-1",
                            event_at=now + 3600,
                            event_type="CLOSED",
                            signal_id="sig-1",
                            position_id="pos-1",
                            wallet=wallet,
                            asset="asset-1",
                            title="Knicks vs Celtics",
                            outcome="Knicks",
                            reason="take_profit",
                            size_usdc=10,
                            pnl=2.5,
                            hold_seconds=3600,
                        ),
                    ]
                )
                report = wallet_outcome_report(store, since_days=1, min_events=1, limit=5)

        self.assertEqual(report["totalEvents"], 3)
        self.assertEqual(report["wallets"][0]["wallet"], wallet)
        self.assertEqual(report["wallets"][0]["category"], "sports")
        self.assertEqual(report["wallets"][0]["closed"], 1)
        self.assertGreater(report["wallets"][0]["learningScore"], 0.5)


if __name__ == "__main__":
    unittest.main()
