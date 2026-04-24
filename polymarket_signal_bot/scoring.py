from __future__ import annotations

import math
import time
from collections import Counter

from .models import Trade, Wallet, WalletScore


def score_wallet(wallet: Wallet, trades: list[Trade], *, now: int | None = None) -> WalletScore | None:
    if not trades:
        return None

    now = now or int(time.time())
    trade_count = len(trades)
    buy_count = sum(1 for trade in trades if trade.side == "BUY")
    sell_count = sum(1 for trade in trades if trade.side == "SELL")
    total_notional = sum(trade.notional for trade in trades)
    avg_notional = total_notional / max(1, trade_count)
    active_days = len({time.gmtime(trade.timestamp).tm_yday for trade in trades if trade.timestamp > 0})
    market_count = len({trade.condition_id for trade in trades if trade.condition_id})

    pnl_efficiency = _safe_ratio(wallet.pnl, wallet.volume or total_notional)
    pnl_score = _pnl_component(pnl_efficiency, has_leaderboard_volume=wallet.volume > 0)
    size_score = _clamp(math.log1p(avg_notional) / math.log1p(2500))
    volume_score = _clamp(math.log1p(total_notional) / math.log1p(50000))
    activity_score = _clamp(active_days / 7)
    diversity_score = _clamp(market_count / 12)
    recency_score = _recency_component(trades, now)
    discipline_score = _discipline_component(trades)
    repeatability_score, drawdown_score = _round_trip_components(trades)

    score = (
        0.24 * pnl_score
        + 0.16 * size_score
        + 0.14 * volume_score
        + 0.12 * activity_score
        + 0.08 * diversity_score
        + 0.06 * recency_score
        + 0.05 * discipline_score
        + 0.09 * repeatability_score
        + 0.06 * drawdown_score
    )
    score = round(_clamp(score), 4)

    reason = (
        f"pnl_eff={pnl_efficiency:.4f}; avg_trade=${avg_notional:.2f}; "
        f"active_days={active_days}; markets={market_count}; trades={trade_count}; "
        f"repeat={repeatability_score:.2f}; drawdown={drawdown_score:.2f}"
    )
    return WalletScore(
        wallet=wallet.address,
        score=score,
        computed_at=now,
        trade_count=trade_count,
        buy_count=buy_count,
        sell_count=sell_count,
        total_notional=round(total_notional, 4),
        avg_notional=round(avg_notional, 4),
        active_days=active_days,
        market_count=market_count,
        leaderboard_pnl=wallet.pnl,
        leaderboard_volume=wallet.volume,
        pnl_efficiency=round(pnl_efficiency, 6),
        reason=reason,
        repeatability_score=round(repeatability_score, 4),
        drawdown_score=round(drawdown_score, 4),
    )


def score_wallets(wallets: list[Wallet], trades_by_wallet: dict[str, list[Trade]]) -> list[WalletScore]:
    scores: list[WalletScore] = []
    now = int(time.time())
    for wallet in wallets:
        score = score_wallet(wallet, trades_by_wallet.get(wallet.address, []), now=now)
        if score:
            scores.append(score)
    scores.sort(key=lambda item: item.score, reverse=True)
    return scores


def _pnl_component(pnl_efficiency: float, *, has_leaderboard_volume: bool) -> float:
    if not has_leaderboard_volume:
        return 0.45
    return _clamp((pnl_efficiency + 0.03) / 0.15)


def _recency_component(trades: list[Trade], now: int) -> float:
    timestamps = [trade.timestamp for trade in trades if trade.timestamp > 0]
    if not timestamps:
        return 0.0
    age_hours = max(0.0, (now - max(timestamps)) / 3600)
    return _clamp(1.0 - age_hours / 72)


def _discipline_component(trades: list[Trade]) -> float:
    by_day: Counter[int] = Counter()
    for trade in trades:
        if trade.timestamp > 0:
            by_day[int(trade.timestamp // 86400)] += 1
    if not by_day:
        return 0.5
    avg_trades_per_day = sum(by_day.values()) / len(by_day)
    if avg_trades_per_day <= 12:
        return 1.0
    if avg_trades_per_day >= 60:
        return 0.1
    return _clamp(1.0 - ((avg_trades_per_day - 12) / 48))


def _round_trip_components(trades: list[Trade]) -> tuple[float, float]:
    ordered = sorted((trade for trade in trades if trade.price > 0 and trade.size > 0), key=lambda item: item.timestamp)
    inventory: dict[str, tuple[float, float]] = {}
    realized: list[float] = []
    for trade in ordered:
        shares, cost = inventory.get(trade.asset, (0.0, 0.0))
        if trade.side == "BUY":
            inventory[trade.asset] = (shares + trade.size, cost + trade.size * trade.price)
            continue
        if trade.side != "SELL" or shares <= 0:
            continue
        closed_size = min(shares, trade.size)
        avg_cost = cost / shares if shares else 0.0
        pnl = closed_size * (trade.price - avg_cost)
        realized.append(pnl)
        remaining_shares = shares - closed_size
        remaining_cost = avg_cost * remaining_shares
        inventory[trade.asset] = (remaining_shares, remaining_cost)

    if not realized:
        return 0.5, 0.5

    wins = sum(1 for pnl in realized if pnl > 0)
    repeatability = wins / len(realized)
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in realized:
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    gross = sum(abs(pnl) for pnl in realized) or 1.0
    drawdown = max_dd / gross
    drawdown_score = _clamp(1.0 - drawdown / 0.35)
    return _clamp(repeatability), drawdown_score


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))
