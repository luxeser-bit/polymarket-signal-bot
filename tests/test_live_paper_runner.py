from __future__ import annotations

import asyncio
import tempfile
import time
import unittest

from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.live_paper_runner import LivePaperConfig, LivePaperRunner, _portfolio_snapshot
from polymarket_signal_bot.models import PaperPosition, Signal
from polymarket_signal_bot.storage import Store


class FakeLiveClient:
    def leaderboard(self, **kwargs):
        return []

    def user_activity(self, *args, **kwargs):
        return []

    def order_books(self, token_ids):
        return [
            {
                "asset_id": token_id,
                "market": f"market-{index}",
                "timestamp": str(int(time.time())),
                "bids": [{"price": "0.49", "size": "200"}],
                "asks": [{"price": "0.51", "size": "200"}],
                "last_trade_price": "0.50",
            }
            for index, token_id in enumerate(token_ids)
        ]

    def midpoint(self, token_id):
        return 0.62

    def market_price(self, token_id, side):
        return 0.62


class LivePaperRunnerTests(unittest.TestCase):
    def test_tick_creates_signals_without_opening_positions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/live.db"
            state_path = f"{tmpdir}/state.json"
            with Store(db_path) as store:
                store.init_schema()
                store.upsert_wallets(demo_wallets())
                store.insert_trades(demo_trades())

            runner = LivePaperRunner(
                LivePaperConfig(
                    db_path=db_path,
                    state_path=state_path,
                    log_path=f"{tmpdir}/live.log",
                    wallet_limit=3,
                    min_wallet_score=0.2,
                    min_trade_usdc=5,
                    lookback_minutes=24 * 60,
                    max_book_price_deviation=10.0,
                    use_cohort_policy=False,
                    use_learning_policy=False,
                ),
                client=FakeLiveClient(),
            )
            signals, summary = runner.tick()

            with Store(db_path) as store:
                store.init_schema()
                positions = store.fetch_open_positions()

        self.assertTrue(signals)
        self.assertIn("signals=", summary)
        self.assertEqual(positions, [])

    def test_signal_price_is_refreshed_from_midpoint(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = LivePaperRunner(
                LivePaperConfig(db_path=f"{tmpdir}/live.db", stop_loss_pct=0.25, take_profit_pct=0.30),
                client=FakeLiveClient(),
            )
            signal = Signal(
                signal_id="sig-1",
                generated_at=now,
                action="BUY",
                wallet="0xabc",
                wallet_score=0.8,
                condition_id="cond-1",
                asset="asset-1",
                outcome="Yes",
                title="Market",
                observed_price=0.50,
                suggested_price=0.51,
                size_usdc=10,
                confidence=0.75,
                stop_loss=0.40,
                take_profit=0.70,
                expires_at=now + 3600,
                source_trade_id="trade-1",
                reason="",
            )

            priced = asyncio.run(runner._signal_with_current_price(signal))

        self.assertEqual(priced.suggested_price, 0.62)
        self.assertEqual(priced.stop_loss, 0.465)
        self.assertEqual(priced.take_profit, 0.806)

    def test_position_loop_closes_take_profit(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/live.db"
            runner = LivePaperRunner(
                LivePaperConfig(db_path=db_path, state_path=f"{tmpdir}/state.json"),
                client=FakeLiveClient(),
            )
            with Store(db_path) as store:
                store.init_schema()
                store.insert_paper_positions(
                    [
                        PaperPosition(
                            position_id="pos-1",
                            signal_id="sig-1",
                            opened_at=now - 60,
                            wallet="0xabc",
                            condition_id="cond-1",
                            asset="asset-1",
                            outcome="Yes",
                            title="Market",
                            entry_price=0.50,
                            size_usdc=10,
                            shares=20,
                            stop_loss=0.40,
                            take_profit=0.60,
                        )
                    ]
                )
                closed = asyncio.run(runner._check_open_positions(store))
                open_positions = store.fetch_open_positions()
                events = store.paper_event_summary()

        self.assertEqual(len(closed), 1)
        self.assertEqual(open_positions, [])
        self.assertEqual(events["total_events"], 1)
        self.assertEqual(events["recent"][0].reason, "take_profit")

    def test_portfolio_metrics_include_pnl_trades_and_win_rate(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/metrics.db") as store:
                store.init_schema()
                store.insert_paper_positions(
                    [
                        PaperPosition(
                            position_id="pos-win",
                            signal_id="sig-win",
                            opened_at=now - 60,
                            wallet="0xabc",
                            condition_id="cond-1",
                            asset="asset-1",
                            outcome="Yes",
                            title="Market",
                            entry_price=0.50,
                            size_usdc=10,
                            shares=20,
                            stop_loss=0.40,
                            take_profit=0.60,
                            status="CLOSED",
                            closed_at=now,
                            exit_price=0.60,
                            realized_pnl=2.0,
                            close_reason="take_profit",
                        ),
                        PaperPosition(
                            position_id="pos-loss",
                            signal_id="sig-loss",
                            opened_at=now - 60,
                            wallet="0xdef",
                            condition_id="cond-2",
                            asset="asset-2",
                            outcome="No",
                            title="Market 2",
                            entry_price=0.50,
                            size_usdc=10,
                            shares=20,
                            stop_loss=0.40,
                            take_profit=0.60,
                            status="CLOSED",
                            closed_at=now,
                            exit_price=0.45,
                            realized_pnl=-1.0,
                            close_reason="stop_loss",
                        ),
                    ]
                )
                metrics = _portfolio_snapshot(store, 200)

        self.assertEqual(metrics["trade_count"], 2)
        self.assertEqual(metrics["closed_trades"], 2)
        self.assertEqual(metrics["winning_trades"], 1)
        self.assertEqual(metrics["win_rate"], 0.5)
        self.assertEqual(metrics["total_pnl"], 1.0)

    def test_stream_tick_consumes_stream_queue_without_api_polling(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/stream-live.db"
            with Store(db_path) as store:
                store.init_schema()
                store.upsert_wallets(demo_wallets())
                store.insert_trades(demo_trades())
                store.insert_stream_events(
                    [
                        {
                            "event_id": "stream-1",
                            "received_at": int(time.time()),
                            "event_ts": int(time.time()),
                            "event_type": "last_trade_price",
                            "market": "market-1",
                            "asset": "asset-1",
                            "side": "BUY",
                            "price": 0.5,
                            "size": 100,
                            "notional": 50,
                            "raw_json": "{}",
                        }
                    ]
                )

            runner = LivePaperRunner(
                LivePaperConfig(
                    db_path=db_path,
                    use_stream_queue=True,
                    wallet_limit=3,
                    min_wallet_score=0.2,
                    min_trade_usdc=5,
                    lookback_minutes=24 * 60,
                    max_book_price_deviation=10.0,
                    use_cohort_policy=False,
                    use_learning_policy=False,
                ),
                client=FakeLiveClient(),
            )
            signals, summary = runner.stream_tick()

            with Store(db_path) as store:
                store.init_schema()
                cursor = store.runtime_state()["live_paper_stream_cursor"]["value"]

        self.assertIn("stream_events=1", summary)
        self.assertTrue(signals)
        self.assertEqual(cursor, "1")

    def test_policy_optimizer_recommendation_is_reloaded_each_tick(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/policy.db"
            runner = LivePaperRunner(LivePaperConfig(db_path=db_path))
            with Store(db_path) as store:
                store.init_schema()
                store.set_runtime_state("policy_optimizer_recommended", "balanced_cohort")
                self.assertEqual(runner._active_signal_policy(store), (True, "balanced"))
                store.set_runtime_state("policy_optimizer_recommended", "baseline")
                self.assertEqual(runner._active_signal_policy(store), (False, "strict"))


if __name__ == "__main__":
    unittest.main()
