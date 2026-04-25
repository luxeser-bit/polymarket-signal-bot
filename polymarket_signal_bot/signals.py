from __future__ import annotations

import hashlib
import math
import time
from collections import defaultdict
from dataclasses import dataclass

from .models import OrderBookSnapshot, Signal, Trade, WalletScore
from .taxonomy import market_category


@dataclass(frozen=True)
class SignalConfig:
    bankroll: float = 200.0
    risk_per_signal: float = 0.04
    max_position_usdc: float = 25.0
    min_position_usdc: float = 2.0
    min_wallet_score: float = 0.55
    min_trade_usdc: float = 50.0
    min_entry_price: float = 0.02
    max_entry_price: float = 0.92
    lookback_minutes: int = 90
    slippage_bps: float = 75.0
    stop_loss_pct: float = 0.28
    take_profit_pct: float = 0.40
    max_hold_hours: int = 36
    max_signals: int = 20
    max_spread: float = 0.08
    min_liquidity_score: float = 0.0
    min_depth_usdc: float = 0.0
    max_book_price_deviation: float = 0.12
    max_wallet_trades_per_day: float = 60.0
    min_cluster_wallets: int = 1
    min_cluster_notional: float = 0.0
    use_cohort_policy: bool = True
    cohort_policy_mode: str = "strict"
    use_learning_policy: bool = True


def generate_signals(
    recent_trades: list[Trade],
    scores: dict[str, WalletScore],
    config: SignalConfig,
    *,
    order_books: dict[str, OrderBookSnapshot] | None = None,
    wallet_cohorts: dict[str, dict[str, object]] | None = None,
    wallet_outcomes: dict[tuple[str, str], dict[str, object]] | None = None,
    now: int | None = None,
) -> list[Signal]:
    now = now or int(time.time())
    order_books = order_books or {}
    wallet_cohorts = wallet_cohorts or {}
    wallet_outcomes = wallet_outcomes or {}
    cutoff = now - config.lookback_minutes * 60
    candidates = []
    for trade in recent_trades:
        ok, _note = _candidate_check(trade, scores, config, cutoff, order_books.get(trade.asset))
        if ok:
            candidates.append(trade)

    cluster_wallets: dict[str, set[str]] = defaultdict(set)
    cluster_notional: dict[str, float] = defaultdict(float)
    for trade in candidates:
        cluster_wallets[trade.asset].add(trade.proxy_wallet)
        cluster_notional[trade.asset] += trade.notional

    best_by_asset: dict[str, Signal] = {}
    for trade in candidates:
        wallet_score = scores[trade.proxy_wallet]
        wallet_count = len(cluster_wallets[trade.asset])
        total_notional = cluster_notional[trade.asset]
        if wallet_count < config.min_cluster_wallets:
            continue
        if total_notional < config.min_cluster_notional:
            continue
        category = market_category(trade)
        book = order_books.get(trade.asset)
        liquidity_score = book.liquidity_score if book else 0.5
        reference_price = book.best_ask if book and book.best_ask > 0 else trade.price
        book_depth = min(book.bid_depth_usdc, book.ask_depth_usdc) if book else 0.0
        confidence = _confidence(
            wallet_score.score,
            trade.notional,
            wallet_count,
            trade.timestamp,
            now,
            liquidity_score,
        )
        cohort_policy = _cohort_policy(
            wallet_cohorts.get(trade.proxy_wallet),
            enabled=config.use_cohort_policy and bool(wallet_cohorts),
            mode=config.cohort_policy_mode,
            book=book,
        )
        learning_policy = _learning_policy(
            wallet_outcomes.get((trade.proxy_wallet, category)),
            enabled=config.use_learning_policy and bool(wallet_outcomes),
        )
        confidence = max(
            0.0,
            min(1.0, confidence + cohort_policy["confidence_delta"] + learning_policy["confidence_delta"]),
        )
        suggested_price = min(
            config.max_entry_price,
            reference_price * (1 + config.slippage_bps / 10000),
        )
        if suggested_price <= 0:
            continue

        size_usdc = min(
            config.max_position_usdc,
            config.bankroll * config.risk_per_signal * (0.55 + confidence / 2),
        )
        size_usdc *= cohort_policy["size_multiplier"]
        size_usdc *= learning_policy["size_multiplier"]
        if size_usdc < config.min_position_usdc:
            continue

        stop_loss = max(0.01, suggested_price * (1 - config.stop_loss_pct))
        take_profit = min(0.99, suggested_price * (1 + config.take_profit_pct))
        reason = (
            f"wallet_score={wallet_score.score:.2f}; trade=${trade.notional:.2f}; "
            f"cluster_wallets={wallet_count}; cluster_notional=${total_notional:.2f}; "
            f"liquidity={liquidity_score:.2f}; depth=${book_depth:.0f}; "
            f"policy={cohort_policy['mode']}; "
            f"cohort={cohort_policy['status']}; stability={cohort_policy['stability']:.3f}; "
            f"priority={cohort_policy['priority']}; auto_open={cohort_policy['auto_open']}; "
            f"size_mult={cohort_policy['size_multiplier']:.2f}; "
            f"learning_category={category}; learning_score={learning_policy['score']:.3f}; "
            f"learning_events={learning_policy['events']}; learning_delta={learning_policy['confidence_delta']:.3f}; "
            f"learning_size_mult={learning_policy['size_multiplier']:.2f}; "
            f"learning_auto_open={learning_policy['auto_open']}"
        )
        signal = Signal(
            signal_id=_signal_id(trade.trade_id, now, suggested_price),
            generated_at=now,
            action="BUY",
            wallet=trade.proxy_wallet,
            wallet_score=wallet_score.score,
            condition_id=trade.condition_id,
            asset=trade.asset,
            outcome=trade.outcome,
            title=trade.title,
            observed_price=round(trade.price, 4),
            suggested_price=round(suggested_price, 4),
            size_usdc=round(size_usdc, 2),
            confidence=round(confidence, 4),
            stop_loss=round(stop_loss, 4),
            take_profit=round(take_profit, 4),
            expires_at=now + config.max_hold_hours * 3600,
            source_trade_id=trade.trade_id,
            reason=reason,
        )
        previous = best_by_asset.get(trade.asset)
        if previous is None or signal.confidence > previous.confidence:
            best_by_asset[trade.asset] = signal

    signals = list(best_by_asset.values())
    signals.sort(key=lambda item: item.confidence, reverse=True)
    return signals[: config.max_signals]


def _candidate_check(
    trade: Trade,
    scores: dict[str, WalletScore],
    config: SignalConfig,
    cutoff: int,
    book: OrderBookSnapshot | None,
) -> tuple[bool, str]:
    if trade.timestamp < cutoff:
        return False, "stale_trade"
    if trade.side != "BUY":
        return False, "not_buy"
    if trade.notional < config.min_trade_usdc:
        return False, "small_trade"
    if not trade.asset or not trade.proxy_wallet:
        return False, "missing_asset_or_wallet"
    if not (config.min_entry_price <= trade.price <= config.max_entry_price):
        return False, "price_out_of_range"
    if book:
        if book.spread > config.max_spread:
            return False, "wide_spread"
        if book.liquidity_score < config.min_liquidity_score:
            return False, "low_liquidity_score"
        if min(book.bid_depth_usdc, book.ask_depth_usdc) < config.min_depth_usdc:
            return False, "low_depth"
        if book.best_ask > 0 and trade.price > 0:
            deviation = (book.best_ask - trade.price) / trade.price
            if deviation > config.max_book_price_deviation:
                return False, "late_entry"
    score = scores.get(trade.proxy_wallet)
    if not score:
        return False, "unscored_wallet"
    if score.score < config.min_wallet_score:
        return False, "weak_wallet"
    avg_trades_per_day = score.trade_count / max(1, score.active_days)
    if avg_trades_per_day > config.max_wallet_trades_per_day:
        return False, "noisy_wallet"
    return True, ""


def _confidence(
    wallet_score: float,
    notional: float,
    wallet_count: int,
    timestamp: int,
    now: int,
    liquidity_score: float,
) -> float:
    notional_score = min(1.0, math.log1p(notional) / math.log1p(10000))
    cluster_score = min(1.0, wallet_count / 4)
    age_minutes = max(0.0, (now - timestamp) / 60)
    freshness = max(0.0, 1.0 - age_minutes / 180)
    return max(
        0.0,
        min(
            1.0,
            0.46 * wallet_score
            + 0.16 * notional_score
            + 0.18 * cluster_score
            + 0.10 * freshness
            + 0.10 * liquidity_score,
        ),
    )


def _cohort_policy(
    cohort: dict[str, object] | None,
    *,
    enabled: bool,
    mode: str = "strict",
    book: OrderBookSnapshot | None = None,
) -> dict[str, object]:
    mode = _normalize_policy_mode(mode)
    if not enabled:
        return {
            "mode": "baseline",
            "status": "OFF",
            "stability": 0.0,
            "size_multiplier": 1.0,
            "confidence_delta": 0.0,
            "priority": 50,
            "auto_open": 1,
        }
    if not cohort:
        return {
            "mode": mode,
            "status": "UNKNOWN",
            "stability": 0.0,
            "size_multiplier": 0.70,
            "confidence_delta": -0.04,
            "priority": 65,
            "auto_open": 0,
        }
    status = str(cohort.get("status") or "UNKNOWN").upper()
    try:
        stability = float(cohort.get("stabilityScore") or cohort.get("stability_score") or 0.0)
    except (TypeError, ValueError):
        stability = 0.0
    good_liquidity = _has_good_liquidity(book)
    if status == "STABLE":
        size_multiplier = 1.25
        confidence_delta = 0.06
        if mode == "balanced":
            size_multiplier = 1.15
            confidence_delta = 0.04
        elif mode == "liquidity_watch":
            size_multiplier = 1.18
            confidence_delta = 0.05
        return {
            "mode": mode,
            "status": status,
            "stability": stability,
            "size_multiplier": size_multiplier,
            "confidence_delta": confidence_delta,
            "priority": 10,
            "auto_open": 1,
        }
    if status == "CANDIDATE":
        size_multiplier = 0.85
        confidence_delta = 0.0
        priority = 30
        auto_open = 1
        if mode == "balanced":
            size_multiplier = 1.0
        elif mode == "liquidity_watch":
            size_multiplier = 0.90
        elif mode == "stable_only":
            size_multiplier = 0.40
            confidence_delta = -0.08
            priority = 45
            auto_open = 0
        return {
            "mode": mode,
            "status": status,
            "stability": stability,
            "size_multiplier": size_multiplier,
            "confidence_delta": confidence_delta,
            "priority": priority,
            "auto_open": auto_open,
        }
    if status == "WATCH":
        size_multiplier = 0.55
        confidence_delta = -0.08
        priority = 70
        auto_open = 0
        if mode == "balanced":
            size_multiplier = 0.45
            confidence_delta = -0.05
            priority = 55
            auto_open = 1 if stability >= 0.32 else 0
        elif mode == "liquidity_watch":
            size_multiplier = 0.45
            confidence_delta = -0.06
            priority = 55 if good_liquidity else 70
            auto_open = 1 if good_liquidity and stability >= 0.32 else 0
        return {
            "mode": mode,
            "status": status,
            "stability": stability,
            "size_multiplier": size_multiplier,
            "confidence_delta": confidence_delta,
            "priority": priority,
            "auto_open": auto_open,
        }
    return {
        "mode": mode,
        "status": status,
        "stability": stability,
        "size_multiplier": 0.30,
        "confidence_delta": -0.16,
        "priority": 90,
        "auto_open": 0,
    }


def _learning_policy(outcome: dict[str, object] | None, *, enabled: bool) -> dict[str, object]:
    if not enabled or not outcome:
        return {
            "score": 0.5,
            "events": 0,
            "size_multiplier": 1.0,
            "confidence_delta": 0.0,
            "auto_open": 1,
        }
    score = _float_value(outcome.get("learningScore"), 0.5)
    events = int(_float_value(outcome.get("events"), 0.0))
    closed = int(_float_value(outcome.get("closed"), 0.0))
    pnl = _float_value(outcome.get("pnl"), 0.0)
    blocked_rate = _float_value(outcome.get("blockedRate"), 0.0)
    risk_exit_rate = _float_value(outcome.get("riskExitRate"), 0.0)
    credibility = min(1.0, max(0.0, events / 8.0))
    confidence_delta = (score - 0.5) * 0.18 * credibility
    size_multiplier = 1.0 + (score - 0.5) * 0.55 * credibility
    auto_open = 1
    if events >= 4 and blocked_rate >= 0.80:
        confidence_delta -= 0.03
        size_multiplier *= 0.92
    if closed >= 2 and risk_exit_rate >= 0.65:
        confidence_delta -= 0.05
        size_multiplier *= 0.82
        auto_open = 0
    if closed >= 2 and pnl < 0:
        confidence_delta -= 0.03
        size_multiplier *= 0.90
    if events >= 6 and score < 0.20:
        auto_open = 0
    return {
        "score": max(0.0, min(1.0, score)),
        "events": events,
        "size_multiplier": max(0.55, min(1.20, size_multiplier)),
        "confidence_delta": max(-0.12, min(0.09, confidence_delta)),
        "auto_open": auto_open,
    }


def _float_value(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


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


def _has_good_liquidity(book: OrderBookSnapshot | None) -> bool:
    if not book:
        return False
    depth = min(book.bid_depth_usdc, book.ask_depth_usdc)
    return book.liquidity_score >= 0.30 and book.spread <= 0.08 and depth >= 250


def _signal_id(trade_id: str, now: int, suggested_price: float) -> str:
    raw = f"{trade_id}|{now // 300}|{suggested_price:.4f}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
