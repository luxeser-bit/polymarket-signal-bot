from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Wallet:
    address: str
    username: str = ""
    source: str = "manual"
    pnl: float = 0.0
    volume: float = 0.0

    @classmethod
    def from_leaderboard(cls, item: dict[str, Any]) -> "Wallet":
        return cls(
            address=_clean_wallet(item.get("proxyWallet")),
            username=str(item.get("userName") or ""),
            source="leaderboard",
            pnl=_float(item.get("pnl")),
            volume=_float(item.get("vol")),
        )


@dataclass(frozen=True)
class Trade:
    trade_id: str
    proxy_wallet: str
    side: str
    asset: str
    condition_id: str
    size: float
    price: float
    timestamp: int
    title: str = ""
    slug: str = ""
    event_slug: str = ""
    outcome: str = ""
    outcome_index: int = 0
    transaction_hash: str = ""
    source: str = "data-api"

    @property
    def notional(self) -> float:
        return max(0.0, self.size * self.price)

    @classmethod
    def from_api(cls, item: dict[str, Any], *, source: str = "data-api") -> "Trade":
        wallet = _clean_wallet(item.get("proxyWallet"))
        asset = str(item.get("asset") or item.get("asset_id") or "")
        condition_id = str(item.get("conditionId") or item.get("market") or "")
        side = str(item.get("side") or "").upper()
        timestamp = _int(item.get("timestamp") or item.get("match_time") or item.get("last_update"))
        transaction_hash = str(item.get("transactionHash") or item.get("transaction_hash") or "")
        size = _float(item.get("size"))
        price = _float(item.get("price"))
        trade_id = _trade_id(wallet, asset, side, timestamp, price, size, transaction_hash)
        return cls(
            trade_id=trade_id,
            proxy_wallet=wallet,
            side=side,
            asset=asset,
            condition_id=condition_id,
            size=size,
            price=price,
            timestamp=timestamp,
            title=str(item.get("title") or ""),
            slug=str(item.get("slug") or ""),
            event_slug=str(item.get("eventSlug") or ""),
            outcome=str(item.get("outcome") or ""),
            outcome_index=_int(item.get("outcomeIndex") or item.get("bucket_index")),
            transaction_hash=transaction_hash,
            source=source,
        )


@dataclass(frozen=True)
class WalletScore:
    wallet: str
    score: float
    computed_at: int
    trade_count: int
    buy_count: int
    sell_count: int
    total_notional: float
    avg_notional: float
    active_days: int
    market_count: int
    leaderboard_pnl: float
    leaderboard_volume: float
    pnl_efficiency: float
    reason: str
    repeatability_score: float = 0.0
    drawdown_score: float = 0.0


@dataclass(frozen=True)
class Signal:
    signal_id: str
    generated_at: int
    action: str
    wallet: str
    wallet_score: float
    condition_id: str
    asset: str
    outcome: str
    title: str
    observed_price: float
    suggested_price: float
    size_usdc: float
    confidence: float
    stop_loss: float
    take_profit: float
    expires_at: int
    source_trade_id: str
    reason: str


@dataclass(frozen=True)
class PaperPosition:
    position_id: str
    signal_id: str
    opened_at: int
    wallet: str
    condition_id: str
    asset: str
    outcome: str
    title: str
    entry_price: float
    size_usdc: float
    shares: float
    stop_loss: float
    take_profit: float
    status: str = "OPEN"
    closed_at: int | None = None
    exit_price: float | None = None
    realized_pnl: float = 0.0


@dataclass(frozen=True)
class OrderBookSnapshot:
    asset: str
    market: str
    timestamp: int
    best_bid: float
    best_ask: float
    mid: float
    spread: float
    bid_depth_usdc: float
    ask_depth_usdc: float
    liquidity_score: float
    last_trade_price: float
    raw_json: str = ""

    @classmethod
    def from_api(cls, item: dict[str, Any]) -> "OrderBookSnapshot":
        bids = _levels(item.get("bids"))
        asks = _levels(item.get("asks"))
        best_bid = max([price for price, _ in bids] or [0.0])
        best_ask = min([price for price, _ in asks] or [0.0])
        mid = _midpoint(best_bid, best_ask, _float(item.get("last_trade_price")))
        spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 1.0
        bid_depth = _depth_usdc(bids, mid, is_bid=True)
        ask_depth = _depth_usdc(asks, mid, is_bid=False)
        liquidity_score = _liquidity_score(spread, bid_depth, ask_depth)
        return cls(
            asset=str(item.get("asset_id") or item.get("token_id") or ""),
            market=str(item.get("market") or ""),
            timestamp=_int(item.get("timestamp")),
            best_bid=round(best_bid, 6),
            best_ask=round(best_ask, 6),
            mid=round(mid, 6),
            spread=round(max(0.0, spread), 6),
            bid_depth_usdc=round(bid_depth, 4),
            ask_depth_usdc=round(ask_depth, 4),
            liquidity_score=round(liquidity_score, 4),
            last_trade_price=_float(item.get("last_trade_price")),
            raw_json="",
        )


def _clean_wallet(value: Any) -> str:
    return str(value or "").strip().lower()


def _float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _trade_id(
    wallet: str,
    asset: str,
    side: str,
    timestamp: int,
    price: float,
    size: float,
    transaction_hash: str,
) -> str:
    raw = "|".join(
        [
            transaction_hash,
            wallet,
            asset,
            side,
            str(timestamp),
            f"{price:.8f}",
            f"{size:.8f}",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _levels(value: Any) -> list[tuple[float, float]]:
    if not isinstance(value, list):
        return []
    levels: list[tuple[float, float]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        price = _float(item.get("price"))
        size = _float(item.get("size"))
        if price > 0 and size > 0:
            levels.append((price, size))
    return levels


def _midpoint(best_bid: float, best_ask: float, last_trade_price: float) -> float:
    if best_bid > 0 and best_ask > 0:
        return (best_bid + best_ask) / 2
    if last_trade_price > 0:
        return last_trade_price
    return best_bid or best_ask or 0.0


def _depth_usdc(levels: list[tuple[float, float]], mid: float, *, is_bid: bool) -> float:
    if not levels:
        return 0.0
    window = 0.05
    total = 0.0
    sorted_levels = sorted(levels, key=lambda level: level[0], reverse=is_bid)
    for price, size in sorted_levels[:20]:
        if mid > 0:
            if is_bid and price < mid - window:
                continue
            if not is_bid and price > mid + window:
                continue
        total += price * size
    return total


def _liquidity_score(spread: float, bid_depth: float, ask_depth: float) -> float:
    depth = min(bid_depth, ask_depth)
    depth_score = min(1.0, depth / 2500)
    spread_score = max(0.0, min(1.0, 1.0 - spread / 0.12))
    return 0.62 * depth_score + 0.38 * spread_score
