from __future__ import annotations

import time

from .models import Trade, Wallet


def demo_wallets() -> list[Wallet]:
    return [
        Wallet(
            address="0x1111111111111111111111111111111111111111",
            username="sharp_alpha",
            source="demo",
            pnl=8300,
            volume=41000,
        ),
        Wallet(
            address="0x2222222222222222222222222222222222222222",
            username="macro_scout",
            source="demo",
            pnl=2600,
            volume=18000,
        ),
        Wallet(
            address="0x3333333333333333333333333333333333333333",
            username="noisy_big",
            source="demo",
            pnl=-1200,
            volume=55000,
        ),
    ]


def demo_trades(now: int | None = None) -> list[Trade]:
    now = now or int(time.time())
    raw = [
        {
            "proxyWallet": "0x1111111111111111111111111111111111111111",
            "side": "BUY",
            "asset": "1001",
            "conditionId": "0x" + "a" * 64,
            "size": 900,
            "price": 0.42,
            "timestamp": now - 300,
            "title": "Demo market: policy outcome",
            "slug": "demo-policy-outcome",
            "outcome": "Yes",
            "transactionHash": "0xaaa1",
        },
        {
            "proxyWallet": "0x2222222222222222222222222222222222222222",
            "side": "BUY",
            "asset": "1001",
            "conditionId": "0x" + "a" * 64,
            "size": 350,
            "price": 0.43,
            "timestamp": now - 240,
            "title": "Demo market: policy outcome",
            "slug": "demo-policy-outcome",
            "outcome": "Yes",
            "transactionHash": "0xaaa2",
        },
        {
            "proxyWallet": "0x1111111111111111111111111111111111111111",
            "side": "BUY",
            "asset": "1002",
            "conditionId": "0x" + "b" * 64,
            "size": 500,
            "price": 0.31,
            "timestamp": now - 1800,
            "title": "Demo market: crypto close",
            "slug": "demo-crypto-close",
            "outcome": "Above",
            "transactionHash": "0xbbb1",
        },
        {
            "proxyWallet": "0x3333333333333333333333333333333333333333",
            "side": "BUY",
            "asset": "1003",
            "conditionId": "0x" + "c" * 64,
            "size": 3000,
            "price": 0.78,
            "timestamp": now - 600,
            "title": "Demo market: noisy trade",
            "slug": "demo-noisy-trade",
            "outcome": "Yes",
            "transactionHash": "0xccc1",
        },
        {
            "proxyWallet": "0x1111111111111111111111111111111111111111",
            "side": "SELL",
            "asset": "1004",
            "conditionId": "0x" + "d" * 64,
            "size": 120,
            "price": 0.62,
            "timestamp": now - 3600,
            "title": "Demo market: exit only",
            "slug": "demo-exit-only",
            "outcome": "Yes",
            "transactionHash": "0xddd1",
        },
    ]
    return [Trade.from_api(item, source="demo") for item in raw]
