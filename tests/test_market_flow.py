from __future__ import annotations

import tempfile
import time
import unittest

from polymarket_signal_bot.market_flow import (
    MarketFlowConfig,
    format_market_flow_summary,
    market_condition_id,
    sync_market_flow,
)
from polymarket_signal_bot.storage import Store


class FakeMarketClient:
    def __init__(self, now: int) -> None:
        self.now = now
        self.market_calls: list[dict[str, object]] = []
        self.trade_calls: list[dict[str, object]] = []

    def markets(self, **kwargs):
        self.market_calls.append(kwargs)
        return [
            {"conditionId": "cond-1", "question": "Will market flow work?", "slug": "flow"},
            {"question": "Missing condition"},
            {"conditionIds": '["cond-2"]', "question": "Second market"},
        ]

    def trades(self, **kwargs):
        self.trade_calls.append(kwargs)
        market = kwargs["market"]
        if market == "cond-1":
            return [
                self.trade("a1", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "asset-a", 100, 0.5),
                self.trade("a2", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "asset-a", 100, 0.5),
            ]
        return [
            self.trade("b1", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "asset-b", 20, 0.5),
            self.trade("b2", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "asset-b", 20, 0.5),
        ]

    def trade(self, trade_id: str, wallet: str, asset: str, size: float, price: float) -> dict[str, object]:
        return {
            "transactionHash": trade_id,
            "proxyWallet": wallet,
            "side": "BUY",
            "asset": asset,
            "conditionId": "",
            "size": size,
            "price": price,
            "timestamp": self.now,
            "outcome": "Yes",
        }


class MarketFlowTests(unittest.TestCase):
    def test_market_flow_inserts_trades_and_promotes_filtered_wallets(self) -> None:
        now = int(time.time())
        client = FakeMarketClient(now)
        with tempfile.TemporaryDirectory() as tmpdir:
            with Store(f"{tmpdir}/flow.db") as store:
                store.init_schema()
                summary = sync_market_flow(
                    store,
                    client=client,
                    config=MarketFlowConfig(
                        market_limit=3,
                        trades_per_market=10,
                        min_trade_cash=25,
                        min_wallet_notional=100,
                        min_wallet_trades=2,
                    ),
                )
                wallets = {wallet.address: wallet for wallet in store.fetch_wallets()}
                runtime = store.runtime_state()

        self.assertEqual(summary["market_rows"], 3)
        self.assertEqual(summary["markets_queried"], 2)
        self.assertEqual(summary["markets_skipped"], 1)
        self.assertEqual(summary["trades_seen"], 4)
        self.assertEqual(summary["trades_inserted"], 4)
        self.assertEqual(summary["wallets_promoted"], 1)
        self.assertIn("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wallets)
        self.assertNotIn("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", wallets)
        self.assertEqual(client.trade_calls[0]["filter_type"], "CASH")
        self.assertEqual(client.trade_calls[0]["filter_amount"], 25)
        self.assertIn("market_flow", runtime["market_flow_last_summary"]["value"])

    def test_market_condition_id_accepts_json_list(self) -> None:
        self.assertEqual(market_condition_id({"conditionIds": '["cond-json"]'}), "cond-json")

    def test_market_flow_summary_is_compact(self) -> None:
        text = format_market_flow_summary(
            {
                "markets_queried": 2,
                "trades_inserted": 3,
                "trades_seen": 4,
                "wallets_promoted": 1,
                "errors": 0,
            }
        )
        self.assertEqual(text, "market_flow markets=2 trades=3/4 wallets=1 errors=0")


if __name__ == "__main__":
    unittest.main()
