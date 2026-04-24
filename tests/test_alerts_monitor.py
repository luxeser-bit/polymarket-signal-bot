from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.alerts import TelegramConfig, TelegramNotifier, format_signal_message
from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.monitor import Monitor, MonitorConfig
from polymarket_signal_bot.signals import SignalConfig, generate_signals
from polymarket_signal_bot.scoring import score_wallets
from polymarket_signal_bot.storage import Store


class AlertMonitorTests(unittest.TestCase):
    def test_alerts_are_deduplicated_when_telegram_is_disabled(self) -> None:
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/alerts.db") as store:
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

                notifier = TelegramNotifier(TelegramConfig())
                first = notifier.notify_new_signals(store)
                second = notifier.notify_new_signals(store)

                self.assertEqual(first, 0)
                self.assertEqual(second, 0)
                self.assertEqual(len(store.fetch_recent_alerts(limit=20)), len(signals))
                self.assertIn("POLYSIGNAL PAPER ALERT", format_signal_message(signals[0]))

    def test_monitor_once_scans_existing_demo_data_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/monitor.db") as store:
                store.init_schema()
                store.upsert_wallets(demo_wallets())
                store.insert_trades(demo_trades())
                monitor = Monitor(
                    store=store,
                    config=MonitorConfig(
                        iterations=1,
                        live_discover=False,
                        wallet_limit=3,
                        min_wallet_score=0.5,
                        min_trade_usdc=20,
                        lookback_minutes=24 * 60,
                        live_sync=False,
                    ),
                    telegram=TelegramConfig(),
                )
                summary = monitor.run_once()

                self.assertIn("signals=", summary)
                self.assertEqual(store.runtime_state()["monitor_last_seen"]["value"].isdigit(), True)


if __name__ == "__main__":
    unittest.main()
