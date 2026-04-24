from __future__ import annotations

import bisect
import time
from dataclasses import dataclass
from typing import Any

from .models import OrderBookSnapshot, Trade, Wallet
from .scoring import score_wallet
from .taxonomy import market_category


@dataclass(frozen=True)
class BacktestConfig:
    bankroll: float = 200.0
    risk_per_signal: float = 0.04
    max_position_usdc: float = 25.0
    min_wallet_score: float = 0.55
    min_trade_usdc: float = 50.0
    min_entry_price: float = 0.02
    max_entry_price: float = 0.92
    slippage_bps: float = 75.0
    copy_delay_seconds: int = 45
    stop_loss_pct: float = 0.28
    take_profit_pct: float = 0.40
    max_hold_hours: int = 36
    warmup_trades: int = 8
    max_open_positions: int = 8
    use_cohort_policy: bool = False
    cohort_policy_mode: str = "strict"


@dataclass
class SimPosition:
    asset: str
    wallet: str
    title: str
    outcome: str
    opened_at: int
    entry_price: float
    size_usdc: float
    shares: float
    stop_loss: float
    take_profit: float
    category: str
    liquidity_bucket: str
    liquidity_score: float
    spread: float
    depth_usdc: float
    cohort_status: str
    cohort_stability: float


def run_backtest(
    trades: list[Trade],
    config: BacktestConfig,
    *,
    order_books: dict[str, OrderBookSnapshot] | None = None,
) -> dict[str, Any]:
    trades = [trade for trade in trades if trade.timestamp > 0 and trade.asset]
    trades.sort(key=lambda item: item.timestamp)
    if not trades:
        return _empty_result(config.bankroll)

    order_books = order_books or {}
    asset_series = _asset_series(trades)
    wallet_history: dict[str, list[Trade]] = {}
    latest_price: dict[str, float] = {}
    open_positions: list[SimPosition] = []
    closed: list[dict[str, Any]] = []
    equity_curve: list[float] = [config.bankroll]
    skipped = 0
    max_exposure = 0.0

    for trade in trades:
        latest_price[trade.asset] = trade.price
        _mark_exits(open_positions, closed, latest_price, trade.timestamp, config)
        realized = sum(item["pnl"] for item in closed)
        exposure = sum(position.size_usdc for position in open_positions)
        cash = config.bankroll + realized - exposure

        history = wallet_history.setdefault(trade.proxy_wallet, [])
        score = score_wallet(
            Wallet(address=trade.proxy_wallet, source="backtest"),
            history,
            now=trade.timestamp,
        )

        if score and len(history) >= config.warmup_trades and _can_open(trade, score.score, open_positions, config):
            delayed_price = _delayed_price(asset_series, trade.asset, trade.timestamp + config.copy_delay_seconds)
            entry_price = min(
                config.max_entry_price,
                delayed_price * (1 + config.slippage_bps / 10000),
            )
            if entry_price > 0 and config.min_entry_price <= entry_price <= config.max_entry_price:
                book = order_books.get(trade.asset)
                liquidity = _liquidity_context(book)
                cohort = _cohort_context(history, score.score, config, liquidity["bucket"])
                if not cohort["auto_open"]:
                    skipped += 1
                    history.append(trade)
                    continue
                size_usdc = min(
                    config.max_position_usdc,
                    config.bankroll * config.risk_per_signal * (0.55 + cohort["effective_score"] / 2),
                ) * cohort["size_multiplier"]
                if len(open_positions) < config.max_open_positions and size_usdc <= max(0.0, cash):
                    open_positions.append(
                        SimPosition(
                            asset=trade.asset,
                            wallet=trade.proxy_wallet,
                            title=trade.title,
                            outcome=trade.outcome,
                            opened_at=trade.timestamp + config.copy_delay_seconds,
                            entry_price=entry_price,
                            size_usdc=size_usdc,
                            shares=size_usdc / entry_price,
                            stop_loss=max(0.01, entry_price * (1 - config.stop_loss_pct)),
                            take_profit=min(0.99, entry_price * (1 + config.take_profit_pct)),
                            category=market_category(trade),
                            liquidity_bucket=liquidity["bucket"],
                            liquidity_score=liquidity["score"],
                            spread=liquidity["spread"],
                            depth_usdc=liquidity["depth"],
                            cohort_status=cohort["status"],
                            cohort_stability=cohort["stability"],
                        )
                    )
                else:
                    skipped += 1
            else:
                skipped += 1

        history.append(trade)
        exposure = sum(position.size_usdc for position in open_positions)
        max_exposure = max(max_exposure, exposure)
        unrealized = sum(
            position.shares * latest_price.get(position.asset, position.entry_price) - position.size_usdc
            for position in open_positions
        )
        equity_curve.append(config.bankroll + sum(item["pnl"] for item in closed) + unrealized)

    final_ts = trades[-1].timestamp
    for position in list(open_positions):
        price = latest_price.get(position.asset, position.entry_price)
        _close_position(open_positions, closed, position, final_ts, price, "end_of_data")

    pnl = sum(item["pnl"] for item in closed)
    wins = sum(1 for item in closed if item["pnl"] > 0)
    losses = sum(1 for item in closed if item["pnl"] <= 0)
    liquidity_known = sum(1 for item in closed if item.get("liquidity_bucket") != "unknown")
    return {
        "start_bankroll": round(config.bankroll, 2),
        "end_bankroll": round(config.bankroll + pnl, 2),
        "pnl": round(pnl, 4),
        "closed_trades": len(closed),
        "wins": wins,
        "losses": losses,
        "hit_rate": round(wins / max(1, len(closed)), 4),
        "max_drawdown": round(_max_drawdown(equity_curve), 4),
        "max_exposure": round(max_exposure, 4),
        "liquidity_coverage": round(liquidity_known / max(1, len(closed)), 4),
        "skipped": skipped,
        "avg_pnl": round(pnl / max(1, len(closed)), 4),
        "first_trade_at": trades[0].timestamp,
        "last_trade_at": trades[-1].timestamp,
        "sample": closed[-10:],
        "by_category": _breakdown(closed, "category"),
        "by_wallet": _breakdown(closed, "wallet", limit=12),
        "by_liquidity": _breakdown(closed, "liquidity_bucket"),
        "by_cohort": _breakdown(closed, "cohort_status"),
        "cohort_policy": config.use_cohort_policy,
        "cohort_policy_mode": config.cohort_policy_mode if config.use_cohort_policy else "baseline",
    }


def _can_open(
    trade: Trade,
    wallet_score: float,
    open_positions: list[SimPosition],
    config: BacktestConfig,
) -> bool:
    if trade.side != "BUY":
        return False
    if len([item for item in open_positions if item.asset == trade.asset]) > 0:
        return False
    if wallet_score < config.min_wallet_score:
        return False
    if trade.notional < config.min_trade_usdc:
        return False
    if len(open_positions) >= config.max_open_positions:
        return False
    if not (config.min_entry_price <= trade.price <= config.max_entry_price):
        return False
    return True


def _mark_exits(
    open_positions: list[SimPosition],
    closed: list[dict[str, Any]],
    latest_price: dict[str, float],
    timestamp: int,
    config: BacktestConfig,
) -> None:
    for position in list(open_positions):
        price = latest_price.get(position.asset)
        if price is None:
            continue
        reason = ""
        if price <= position.stop_loss:
            reason = "stop_loss"
        elif price >= position.take_profit:
            reason = "take_profit"
        elif timestamp - position.opened_at >= config.max_hold_hours * 3600:
            reason = "max_hold"
        if reason:
            _close_position(open_positions, closed, position, timestamp, price, reason)


def _close_position(
    open_positions: list[SimPosition],
    closed: list[dict[str, Any]],
    position: SimPosition,
    timestamp: int,
    price: float,
    reason: str,
) -> None:
    if position not in open_positions:
        return
    pnl = position.shares * price - position.size_usdc
    closed.append(
        {
            "asset": position.asset,
            "wallet": position.wallet,
            "market": position.title or position.asset,
            "outcome": position.outcome,
            "category": position.category,
            "liquidity_bucket": position.liquidity_bucket,
            "liquidity_score": round(position.liquidity_score, 4),
            "spread": round(position.spread, 4),
            "depth_usdc": round(position.depth_usdc, 2),
            "cohort_status": position.cohort_status,
            "cohort_stability": round(position.cohort_stability, 4),
            "entry": round(position.entry_price, 4),
            "exit": round(price, 4),
            "pnl": round(pnl, 4),
            "reason": reason,
            "opened_at": position.opened_at,
            "closed_at": timestamp,
        }
    )
    open_positions.remove(position)


def _asset_series(trades: list[Trade]) -> dict[str, tuple[list[int], list[float]]]:
    grouped: dict[str, list[tuple[int, float]]] = {}
    for trade in trades:
        grouped.setdefault(trade.asset, []).append((trade.timestamp, trade.price))
    return {
        asset: ([timestamp for timestamp, _ in rows], [price for _, price in rows])
        for asset, rows in grouped.items()
    }


def _delayed_price(series: dict[str, tuple[list[int], list[float]]], asset: str, timestamp: int) -> float:
    timestamps, prices = series.get(asset, ([], []))
    if not prices:
        return 0.0
    index = bisect.bisect_left(timestamps, timestamp)
    if index >= len(prices):
        return prices[-1]
    return prices[index]


def _max_drawdown(equity_curve: list[float]) -> float:
    peak = equity_curve[0] if equity_curve else 0.0
    max_dd = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    return max_dd


def _breakdown(closed: list[dict[str, Any]], key: str, limit: int | None = None) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in closed:
        grouped.setdefault(str(item.get(key) or "unknown"), []).append(item)
    rows = []
    for name, items in grouped.items():
        pnl = sum(float(item["pnl"]) for item in items)
        wins = sum(1 for item in items if float(item["pnl"]) > 0)
        rows.append(
            {
                key: name,
                "trades": len(items),
                "pnl": round(pnl, 4),
                "hit_rate": round(wins / max(1, len(items)), 4),
                "avg_pnl": round(pnl / max(1, len(items)), 4),
            }
        )
    rows.sort(key=lambda item: (item["pnl"], item["trades"]), reverse=True)
    if limit is not None:
        return rows[:limit]
    return rows


def _liquidity_context(book: OrderBookSnapshot | None) -> dict[str, Any]:
    if book is None:
        return {"bucket": "unknown", "score": 0.0, "spread": 0.0, "depth": 0.0}
    depth = min(book.bid_depth_usdc, book.ask_depth_usdc)
    if book.liquidity_score >= 0.60 and book.spread <= 0.03 and depth >= 1000:
        bucket = "deep"
    elif book.liquidity_score >= 0.30 and book.spread <= 0.08 and depth >= 250:
        bucket = "liquid"
    elif book.liquidity_score >= 0.10 and book.spread <= 0.14 and depth >= 50:
        bucket = "thin"
    else:
        bucket = "illiquid"
    return {
        "bucket": bucket,
        "score": float(book.liquidity_score),
        "spread": float(book.spread),
        "depth": float(depth),
    }


def _cohort_context(
    history: list[Trade],
    wallet_score: float,
    config: BacktestConfig,
    liquidity_bucket: str = "unknown",
) -> dict[str, Any]:
    if not config.use_cohort_policy:
        return {
            "status": "BASELINE",
            "stability": 0.0,
            "effective_score": wallet_score,
            "size_multiplier": 1.0,
            "auto_open": True,
        }
    trades = [trade for trade in history if trade.timestamp > 0 and trade.notional > 0]
    if not trades:
        return _cohort_policy("UNKNOWN", 0.0, wallet_score, config, liquidity_bucket)
    active_days = len({int(trade.timestamp // 86400) for trade in trades})
    market_count = len({trade.condition_id for trade in trades if trade.condition_id})
    total_notional = sum(trade.notional for trade in trades)
    daily: dict[int, float] = {}
    for trade in trades:
        daily[int(trade.timestamp // 86400)] = daily.get(int(trade.timestamp // 86400), 0.0) + trade.notional
    day_balance = 0.0
    if total_notional > 0 and len(daily) > 1:
        day_balance = _clamp(1.0 - max(daily.values()) / total_notional)
    maturity = _clamp(active_days / 4)
    diversity = _clamp(market_count / 8)
    discipline = _discipline(len(trades), active_days)
    stability = _clamp(
        0.32 * wallet_score
        + 0.18 * day_balance
        + 0.18 * maturity
        + 0.14 * diversity
        + 0.08 * discipline
    )
    if active_days >= 4 and market_count >= 3 and stability >= 0.58:
        status = "STABLE"
    elif active_days >= 2 and market_count >= 2 and stability >= 0.46:
        status = "CANDIDATE"
    elif stability < 0.25 or len(trades) <= 1:
        status = "NOISE"
    else:
        status = "WATCH"
    return _cohort_policy(status, stability, wallet_score, config, liquidity_bucket)


def _cohort_policy(
    status: str,
    stability: float,
    wallet_score: float,
    config: BacktestConfig,
    liquidity_bucket: str,
) -> dict[str, Any]:
    mode = _normalize_policy_mode(config.cohort_policy_mode)
    is_good_liquidity = liquidity_bucket in {"deep", "liquid"}

    if status == "STABLE":
        size_multiplier = 1.25
        score_boost = 0.06
        if mode == "balanced":
            size_multiplier = 1.15
            score_boost = 0.04
        elif mode == "liquidity_watch":
            size_multiplier = 1.18
            score_boost = 0.05
        return {
            "status": status,
            "stability": stability,
            "effective_score": _clamp(wallet_score + score_boost),
            "size_multiplier": size_multiplier,
            "auto_open": True,
        }
    if status == "CANDIDATE":
        auto_open = mode != "stable_only"
        size_multiplier = 0.85
        score_delta = 0.0
        if mode == "balanced":
            size_multiplier = 1.0
        elif mode == "liquidity_watch":
            size_multiplier = 0.90
        elif mode == "stable_only":
            size_multiplier = 0.40
            score_delta = -0.08
        return {
            "status": status,
            "stability": stability,
            "effective_score": _clamp(wallet_score + score_delta),
            "size_multiplier": size_multiplier,
            "auto_open": auto_open,
        }
    if status == "WATCH":
        auto_open = False
        size_multiplier = 0.55
        score_delta = -0.08
        if mode == "balanced":
            auto_open = stability >= 0.32 and wallet_score >= 0.50
            size_multiplier = 0.45
            score_delta = -0.05
        elif mode == "liquidity_watch":
            auto_open = is_good_liquidity and stability >= 0.32 and wallet_score >= 0.50
            size_multiplier = 0.45
            score_delta = -0.06
        return {
            "status": status,
            "stability": stability,
            "effective_score": _clamp(wallet_score + score_delta),
            "size_multiplier": size_multiplier,
            "auto_open": auto_open,
        }
    return {
        "status": status,
        "stability": stability,
        "effective_score": _clamp(wallet_score - 0.16),
        "size_multiplier": 0.30,
        "auto_open": False,
    }


def _normalize_policy_mode(value: str) -> str:
    mode = value.lower().replace("-", "_")
    if mode in {"strict", "strict_cohort"}:
        return "strict"
    if mode in {"balanced", "balanced_cohort"}:
        return "balanced"
    if mode in {"stable_only", "stable"}:
        return "stable_only"
    if mode in {"liquidity_watch", "liquidity"}:
        return "liquidity_watch"
    return "strict"


def _discipline(trades: int, active_days: int) -> float:
    if active_days <= 0:
        return 0.0
    trades_per_day = trades / active_days
    if trades_per_day <= 12:
        return 1.0
    if trades_per_day >= 60:
        return 0.1
    return _clamp(1.0 - ((trades_per_day - 12) / 48))


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _empty_result(bankroll: float) -> dict[str, Any]:
    now = int(time.time())
    return {
        "start_bankroll": bankroll,
        "end_bankroll": bankroll,
        "pnl": 0.0,
        "closed_trades": 0,
        "wins": 0,
        "losses": 0,
        "hit_rate": 0.0,
        "max_drawdown": 0.0,
        "max_exposure": 0.0,
        "liquidity_coverage": 0.0,
        "skipped": 0,
        "avg_pnl": 0.0,
        "first_trade_at": now,
        "last_trade_at": now,
        "sample": [],
        "by_category": [],
        "by_wallet": [],
        "by_liquidity": [],
        "by_cohort": [],
        "cohort_policy": False,
        "cohort_policy_mode": "baseline",
    }
