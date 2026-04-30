from __future__ import annotations

"""Two-agent consensus layer for trading signals.

The strategist agent is the existing signal generator from ``signals.py``. The
skeptic agent is a deterministic reviewer that rejects signals when liquidity,
volatility, or wallet behavior looks unsafe. Every review is persisted so the
dashboard can show why a signal was allowed or rejected.
"""

import json
import logging
import sqlite3
import statistics
import time
from dataclasses import dataclass
from typing import Any, Iterable

from .models import OrderBookSnapshot, Signal, Trade, WalletScore
from .signals import SignalConfig, generate_signals


LOGGER = logging.getLogger("polymarket_signal_bot.consensus")


@dataclass(frozen=True)
class ConsensusConfig:
    enabled: bool = True
    min_strategy_confidence: float = 0.55
    require_order_book: bool = False
    max_spread: float = 0.08
    min_liquidity_score: float = 0.10
    min_depth_usdc: float = 25.0
    max_relative_volatility: float = 0.22
    volatility_min_trades: int = 5
    max_wallet_trades_per_day: float = 60.0
    max_wallet_asset_share: float = 0.92
    min_asset_flow_wallets: int = 2


@dataclass(frozen=True)
class AgentVote:
    agent: str
    verdict: str
    confidence: float
    reason: str
    details: dict[str, object]

    @property
    def agrees(self) -> bool:
        return self.verdict.lower() == "agree"


@dataclass(frozen=True)
class ConsensusDecision:
    signal: Signal
    strategist: AgentVote
    skeptic: AgentVote
    final_decision: str
    decided_at: int
    reason: str

    @property
    def approved(self) -> bool:
        return self.final_decision == "EXECUTE"


def generate_strategy_signals(*args: Any, **kwargs: Any) -> list[Signal]:
    """Agent #1: delegate primary signal generation to the existing strategy."""

    return generate_signals(*args, **kwargs)


def consensus_filter_signals(
    signals: Iterable[Signal],
    *,
    recent_trades: list[Trade],
    scores: dict[str, WalletScore],
    order_books: dict[str, OrderBookSnapshot] | None = None,
    config: ConsensusConfig | None = None,
    now: int | None = None,
) -> tuple[list[Signal], list[ConsensusDecision]]:
    config = config or ConsensusConfig()
    now = now or int(time.time())
    order_books = order_books or {}
    approved: list[Signal] = []
    decisions: list[ConsensusDecision] = []
    for signal in signals:
        strategist = strategist_vote(signal, config=config)
        skeptic = skeptic_vote(
            signal,
            recent_trades=recent_trades,
            scores=scores,
            order_books=order_books,
            config=config,
            now=now,
        )
        execute = (not config.enabled) or (strategist.agrees and skeptic.agrees)
        final_decision = "EXECUTE" if execute else "REJECT"
        reason = "consensus_approved" if execute else f"rejected_by_consensus: {skeptic.reason}"
        decision = ConsensusDecision(
            signal=signal,
            strategist=strategist,
            skeptic=skeptic,
            final_decision=final_decision,
            decided_at=now,
            reason=reason,
        )
        decisions.append(decision)
        LOGGER.info(
            "consensus signal=%s strategist=%s skeptic=%s final=%s confidence=%.3f reason=%s",
            signal.signal_id,
            strategist.verdict,
            skeptic.verdict,
            final_decision,
            signal.confidence,
            reason,
        )
        if execute:
            approved.append(signal)
    return approved, decisions


def strategist_vote(signal: Signal, *, config: ConsensusConfig) -> AgentVote:
    confidence = max(0.0, min(1.0, float(signal.confidence or 0.0)))
    verdict = "agree"
    reason = f"strategy_signal confidence={confidence:.3f}"
    if confidence < config.min_strategy_confidence:
        reason += f" below_review_floor={config.min_strategy_confidence:.3f}"
    return AgentVote(
        agent="strategist",
        verdict=verdict,
        confidence=confidence,
        reason=reason,
        details={
            "signal_confidence": confidence,
            "wallet_score": float(signal.wallet_score or 0.0),
            "action": signal.action,
        },
    )


def skeptic_vote(
    signal: Signal,
    *,
    recent_trades: list[Trade],
    scores: dict[str, WalletScore],
    order_books: dict[str, OrderBookSnapshot],
    config: ConsensusConfig,
    now: int,
) -> AgentVote:
    hard_reasons: list[str] = []
    soft_reasons: list[str] = []
    details: dict[str, object] = {}

    confidence = float(signal.confidence or 0.0)
    if confidence < config.min_strategy_confidence:
        hard_reasons.append(f"low_confidence={confidence:.3f}")

    book = order_books.get(signal.asset)
    if book is None:
        details["order_book"] = "missing"
        if config.require_order_book:
            hard_reasons.append("missing_order_book")
        else:
            soft_reasons.append("liquidity_unknown")
    else:
        depth = min(float(book.bid_depth_usdc or 0.0), float(book.ask_depth_usdc or 0.0))
        details.update(
            {
                "spread": round(float(book.spread or 0.0), 6),
                "liquidity_score": round(float(book.liquidity_score or 0.0), 4),
                "depth_usdc": round(depth, 4),
                "best_bid": round(float(book.best_bid or 0.0), 6),
                "best_ask": round(float(book.best_ask or 0.0), 6),
            }
        )
        if float(book.spread or 1.0) > config.max_spread:
            hard_reasons.append(f"wide_spread={float(book.spread):.4f}")
        if float(book.liquidity_score or 0.0) < config.min_liquidity_score:
            hard_reasons.append(f"low_liquidity={float(book.liquidity_score or 0.0):.3f}")
        if depth < config.min_depth_usdc:
            hard_reasons.append(f"thin_depth=${depth:.0f}")

    asset_trades = [trade for trade in recent_trades if trade.asset == signal.asset]
    volatility = relative_price_volatility(asset_trades)
    details["asset_trade_count"] = len(asset_trades)
    details["relative_volatility"] = round(volatility, 6)
    if len(asset_trades) >= config.volatility_min_trades and volatility > config.max_relative_volatility:
        hard_reasons.append(f"high_volatility={volatility:.3f}")
    elif len(asset_trades) < config.volatility_min_trades:
        soft_reasons.append(f"short_price_history={len(asset_trades)}")

    wallet_score = scores.get(signal.wallet)
    if wallet_score is None:
        soft_reasons.append("wallet_score_missing")
    else:
        avg_trades_per_day = wallet_score.trade_count / max(1, wallet_score.active_days)
        details.update(
            {
                "wallet_score": round(float(wallet_score.score or 0.0), 4),
                "wallet_trade_count": int(wallet_score.trade_count or 0),
                "wallet_active_days": int(wallet_score.active_days or 0),
                "wallet_trades_per_day": round(avg_trades_per_day, 4),
            }
        )
        if avg_trades_per_day > config.max_wallet_trades_per_day:
            hard_reasons.append(f"noisy_wallet={avg_trades_per_day:.1f}_trades_per_day")

    flow = asset_flow_share(signal, asset_trades)
    details.update(flow)
    if (
        flow["asset_wallets"] < config.min_asset_flow_wallets
        and flow["wallet_asset_share"] >= config.max_wallet_asset_share
        and flow["asset_notional"] >= max(250.0, signal.size_usdc * 2.0)
    ):
        soft_reasons.append(f"single_wallet_flow_share={flow['wallet_asset_share']:.2f}")

    if hard_reasons:
        reason = "; ".join(hard_reasons + soft_reasons)
        return AgentVote(
            agent="skeptic",
            verdict="disagree",
            confidence=0.0,
            reason=reason,
            details=details,
        )

    reason = "; ".join(["risk_checks_passed", *soft_reasons])
    return AgentVote(
        agent="skeptic",
        verdict="agree",
        confidence=max(0.0, min(1.0, 1.0 - min(0.9, volatility))),
        reason=reason,
        details=details,
    )


def relative_price_volatility(trades: list[Trade]) -> float:
    prices = [float(trade.price or 0.0) for trade in trades if float(trade.price or 0.0) > 0]
    if len(prices) < 2:
        return 0.0
    average = statistics.fmean(prices)
    if average <= 0:
        return 0.0
    return float(statistics.pstdev(prices) / average)


def asset_flow_share(signal: Signal, trades: list[Trade]) -> dict[str, object]:
    total = sum(max(0.0, float(trade.notional)) for trade in trades)
    wallet_total = sum(max(0.0, float(trade.notional)) for trade in trades if trade.proxy_wallet == signal.wallet)
    wallets = {trade.proxy_wallet for trade in trades if trade.proxy_wallet}
    share = wallet_total / total if total > 0 else 0.0
    return {
        "asset_notional": round(total, 4),
        "wallet_asset_notional": round(wallet_total, 4),
        "wallet_asset_share": round(share, 6),
        "asset_wallets": len(wallets),
    }


def ensure_consensus_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS consensus_votes (
            signal_id TEXT PRIMARY KEY,
            decided_at INTEGER NOT NULL,
            final_decision TEXT NOT NULL,
            strategist_verdict TEXT NOT NULL,
            strategist_confidence REAL NOT NULL,
            strategist_reason TEXT NOT NULL DEFAULT '',
            skeptic_verdict TEXT NOT NULL,
            skeptic_confidence REAL NOT NULL,
            skeptic_reason TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL DEFAULT '',
            wallet TEXT NOT NULL DEFAULT '',
            wallet_score REAL NOT NULL DEFAULT 0,
            condition_id TEXT NOT NULL DEFAULT '',
            asset TEXT NOT NULL DEFAULT '',
            outcome TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            signal_confidence REAL NOT NULL DEFAULT 0,
            suggested_price REAL NOT NULL DEFAULT 0,
            size_usdc REAL NOT NULL DEFAULT 0,
            reason TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_consensus_votes_time
            ON consensus_votes(decided_at DESC);
        CREATE INDEX IF NOT EXISTS idx_consensus_votes_final
            ON consensus_votes(final_decision, decided_at DESC);
        """
    )


def persist_consensus_decisions(conn: sqlite3.Connection, decisions: Iterable[ConsensusDecision]) -> int:
    ensure_consensus_schema(conn)
    count = 0
    for decision in decisions:
        signal = decision.signal
        conn.execute(
            """
            INSERT OR REPLACE INTO consensus_votes(
                signal_id, decided_at, final_decision,
                strategist_verdict, strategist_confidence, strategist_reason,
                skeptic_verdict, skeptic_confidence, skeptic_reason,
                action, wallet, wallet_score, condition_id, asset, outcome, title,
                signal_confidence, suggested_price, size_usdc, reason, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.signal_id,
                decision.decided_at,
                decision.final_decision,
                decision.strategist.verdict,
                decision.strategist.confidence,
                decision.strategist.reason,
                decision.skeptic.verdict,
                decision.skeptic.confidence,
                decision.skeptic.reason,
                signal.action,
                signal.wallet,
                signal.wallet_score,
                signal.condition_id,
                signal.asset,
                signal.outcome,
                signal.title,
                signal.confidence,
                signal.suggested_price,
                signal.size_usdc,
                decision.reason,
                json.dumps(
                    {
                        "strategist": decision.strategist.details,
                        "skeptic": decision.skeptic.details,
                        "source_trade_id": signal.source_trade_id,
                        "signal_reason": signal.reason,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                ),
            ),
        )
        count += 1
    conn.commit()
    return count


def consensus_summary(decisions: Iterable[ConsensusDecision]) -> dict[str, int]:
    result = {"total": 0, "execute": 0, "reject": 0}
    for decision in decisions:
        result["total"] += 1
        if decision.approved:
            result["execute"] += 1
        else:
            result["reject"] += 1
    return result


def consensus_runtime_summary(decisions: Iterable[ConsensusDecision]) -> str:
    summary = consensus_summary(decisions)
    return f"total={summary['total']} execute={summary['execute']} reject={summary['reject']}"


def consensus_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    raw_meta = str(payload.get("metadata_json") or "{}")
    try:
        payload["metadata"] = json.loads(raw_meta)
    except json.JSONDecodeError:
        payload["metadata"] = {}
    payload.pop("metadata_json", None)
    return payload


__all__ = [
    "AgentVote",
    "ConsensusConfig",
    "ConsensusDecision",
    "consensus_filter_signals",
    "consensus_row_to_dict",
    "consensus_runtime_summary",
    "ensure_consensus_schema",
    "generate_strategy_signals",
    "persist_consensus_decisions",
    "skeptic_vote",
    "strategist_vote",
]
