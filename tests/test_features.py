from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.features import build_decision_features
from polymarket_signal_bot.models import OrderBookSnapshot, PaperEvent, Signal
from polymarket_signal_bot.storage import Store


class DecisionFeatureTests(unittest.TestCase):
    def test_build_decision_features_labels_paper_outcomes(self) -> None:
        now = int(time.time())
        wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/features.db") as store:
                store.init_schema()
                store.upsert_order_books(
                    [
                        OrderBookSnapshot(
                            asset="asset-1",
                            market="market-1",
                            timestamp=now,
                            best_bid=0.49,
                            best_ask=0.51,
                            mid=0.50,
                            spread=0.02,
                            bid_depth_usdc=1200,
                            ask_depth_usdc=1300,
                            liquidity_score=0.72,
                            last_trade_price=0.50,
                        )
                    ]
                )
                store.insert_signals(
                    [
                        Signal(
                            signal_id="sig-1",
                            generated_at=now + 1,
                            action="BUY",
                            wallet=wallet,
                            wallet_score=0.81,
                            condition_id="cond-1",
                            asset="asset-1",
                            outcome="Knicks",
                            title="Knicks vs Celtics",
                            observed_price=0.50,
                            suggested_price=0.51,
                            size_usdc=12.0,
                            confidence=0.76,
                            stop_loss=0.40,
                            take_profit=0.62,
                            expires_at=now + 3600,
                            source_trade_id="trade-1",
                            reason=(
                                "cohort=STABLE; learning_score=0.82; learning_events=9; "
                                "learning_delta=0.04; learning_size_mult=1.12; "
                                "learning_auto_open=1"
                            ),
                        )
                    ]
                )
                store.insert_paper_events(
                    [
                        PaperEvent(
                            event_id="created-1",
                            event_at=now + 2,
                            event_type="SIGNAL_CREATED",
                            signal_id="sig-1",
                            wallet=wallet,
                            asset="asset-1",
                            title="Knicks vs Celtics",
                            outcome="Knicks",
                            reason="created",
                        ),
                        PaperEvent(
                            event_id="closed-1",
                            event_at=now + 3,
                            event_type="CLOSED",
                            signal_id="sig-1",
                            position_id="pos-1",
                            wallet=wallet,
                            asset="asset-1",
                            title="Knicks vs Celtics",
                            outcome="Knicks",
                            reason="take_profit",
                            pnl=2.25,
                            hold_seconds=1800,
                        ),
                    ]
                )
                result = build_decision_features(store)
                closed = store.conn.execute(
                    "SELECT * FROM decision_features WHERE event_id = ?",
                    ("closed-1",),
                ).fetchone()

        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["summary"]["features"], 2)
        self.assertEqual(closed["label"], "win")
        self.assertEqual(closed["category"], "sports")
        self.assertAlmostEqual(closed["learning_score"], 0.82)
        self.assertAlmostEqual(closed["liquidity_score"], 0.72)
        self.assertEqual(closed["label_win"], 1)
        self.assertEqual(closed["close_reason"], "take_profit")


if __name__ == "__main__":
    unittest.main()
