from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from .backtest import BacktestConfig, run_backtest
from .models import OrderBookSnapshot, Trade


@dataclass(frozen=True)
class PolicyCandidate:
    name: str
    use_cohort_policy: bool
    cohort_policy_mode: str = "strict"


@dataclass(frozen=True)
class OptimizerConfig:
    min_closed_trades: int = 25
    drawdown_weight: float = 2.0
    liquidity_coverage_floor: float = 0.05
    liquidity_weight: float = 20.0


DEFAULT_POLICY_CANDIDATES = (
    PolicyCandidate("baseline", use_cohort_policy=False, cohort_policy_mode="baseline"),
    PolicyCandidate("strict_cohort", use_cohort_policy=True, cohort_policy_mode="strict"),
    PolicyCandidate("balanced_cohort", use_cohort_policy=True, cohort_policy_mode="balanced"),
    PolicyCandidate("stable_only", use_cohort_policy=True, cohort_policy_mode="stable_only"),
    PolicyCandidate("liquidity_watch", use_cohort_policy=True, cohort_policy_mode="liquidity_watch"),
)


def policy_settings_from_recommendation(policy: str, *, default_enabled: bool = True) -> tuple[bool, str]:
    normalized = policy.lower().replace("-", "_")
    if normalized == "baseline":
        return False, "strict"
    if normalized in {"strict", "strict_cohort"}:
        return True, "strict"
    if normalized in {"balanced", "balanced_cohort"}:
        return True, "balanced"
    if normalized in {"stable_only", "stable"}:
        return True, "stable_only"
    if normalized in {"liquidity_watch", "liquidity"}:
        return True, "liquidity_watch"
    return default_enabled, "strict"


def run_policy_optimizer(
    trades: list[Trade],
    base_config: BacktestConfig,
    *,
    order_books: dict[str, OrderBookSnapshot] | None = None,
    candidates: tuple[PolicyCandidate, ...] = DEFAULT_POLICY_CANDIDATES,
    optimizer_config: OptimizerConfig | None = None,
) -> dict[str, Any]:
    optimizer_config = optimizer_config or OptimizerConfig()
    rows = []
    results = {}
    for candidate in candidates:
        config = replace(
            base_config,
            use_cohort_policy=candidate.use_cohort_policy,
            cohort_policy_mode=candidate.cohort_policy_mode,
        )
        result = run_backtest(trades, config, order_books=order_books)
        score = optimizer_score(result, bankroll=config.bankroll, optimizer_config=optimizer_config)
        row = _summary_row(candidate.name, result, score)
        rows.append(row)
        results[candidate.name] = result

    rows.sort(key=lambda item: (item["optimizer_score"], item["pnl"], item["closed_trades"]), reverse=True)
    recommended = rows[0] if rows else {}
    return {
        "recommended": recommended,
        "rows": rows,
        "results": results,
        "config": {
            "min_closed_trades": optimizer_config.min_closed_trades,
            "drawdown_weight": optimizer_config.drawdown_weight,
            "liquidity_coverage_floor": optimizer_config.liquidity_coverage_floor,
            "liquidity_weight": optimizer_config.liquidity_weight,
        },
    }


def optimizer_score(
    result: dict[str, Any],
    *,
    bankroll: float,
    optimizer_config: OptimizerConfig | None = None,
) -> float:
    optimizer_config = optimizer_config or OptimizerConfig()
    pnl = float(result.get("pnl") or 0.0)
    drawdown = float(result.get("max_drawdown") or 0.0)
    closed_trades = int(result.get("closed_trades") or 0)
    liquidity_coverage = float(result.get("liquidity_coverage") or 0.0)
    hit_rate = float(result.get("hit_rate") or 0.0)

    trade_penalty = max(0, optimizer_config.min_closed_trades - closed_trades) * 2.0
    drawdown_penalty = drawdown * max(1.0, bankroll) * optimizer_config.drawdown_weight
    liquidity_penalty = (
        max(0.0, optimizer_config.liquidity_coverage_floor - liquidity_coverage)
        * optimizer_config.liquidity_weight
    )
    weak_signal_penalty = 10.0 if closed_trades >= optimizer_config.min_closed_trades and hit_rate < 0.20 else 0.0
    return round(pnl - drawdown_penalty - trade_penalty - liquidity_penalty - weak_signal_penalty, 4)


def _summary_row(policy: str, result: dict[str, Any], score: float) -> dict[str, Any]:
    return {
        "policy": policy,
        "pnl": round(float(result.get("pnl") or 0.0), 4),
        "closed_trades": int(result.get("closed_trades") or 0),
        "hit_rate": round(float(result.get("hit_rate") or 0.0), 4),
        "max_drawdown": round(float(result.get("max_drawdown") or 0.0), 4),
        "max_exposure": round(float(result.get("max_exposure") or 0.0), 4),
        "liquidity_coverage": round(float(result.get("liquidity_coverage") or 0.0), 4),
        "skipped": int(result.get("skipped") or 0),
        "optimizer_score": score,
    }
