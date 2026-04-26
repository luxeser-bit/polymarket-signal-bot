from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
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


def prepared_training_tables_available(db_path: str | Path) -> bool:
    path = Path(db_path)
    if not path.exists():
        return False
    try:
        uri = f"{path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        try:
            tables = {
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name IN ('raw_transactions', 'scored_wallets')
                    """
                )
            }
            if {"raw_transactions", "scored_wallets"} - tables:
                return False
            scored = conn.execute("SELECT 1 FROM scored_wallets LIMIT 1").fetchone()
            raw = conn.execute("SELECT 1 FROM raw_transactions LIMIT 1").fetchone()
            return scored is not None and raw is not None
        finally:
            conn.close()
    except sqlite3.Error:
        return False


def load_indexed_policy_trades(
    db_path: str | Path,
    *,
    min_score: float = 0.55,
    since_ts: int | None = None,
    limit: int = 100_000,
) -> list[Trade]:
    path = Path(db_path)
    if not path.exists():
        return []
    clauses = [
        "s.score >= ?",
        "LOWER(r.side) IN ('buy', 'sell')",
        "r.user_address != ''",
        "r.market_id != ''",
        "r.timestamp > 0",
        "r.price > 0",
        "ABS(r.amount) > 0",
    ]
    params: list[Any] = [float(min_score)]
    if since_ts is not None:
        clauses.append("r.timestamp >= ?")
        params.append(int(since_ts))
    params.append(int(limit))
    query = f"""
    SELECT
        r.hash,
        r.log_index,
        LOWER(r.user_address) AS wallet,
        UPPER(r.side) AS side,
        r.market_id,
        ABS(r.amount) AS amount,
        r.price,
        r.timestamp
    FROM raw_transactions r
    JOIN scored_wallets s ON s.wallet = LOWER(r.user_address)
    WHERE {' AND '.join(clauses)}
    ORDER BY r.timestamp DESC
    LIMIT ?
    """
    try:
        uri = f"{path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, tuple(params)).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return []
    trades = [
        Trade(
            trade_id=f"{row['hash']}:{row['log_index']}",
            proxy_wallet=str(row["wallet"]),
            side=str(row["side"]),
            asset=str(row["market_id"]),
            condition_id=str(row["market_id"]),
            size=float(row["amount"] or 0.0),
            price=float(row["price"] or 0.0),
            timestamp=int(row["timestamp"] or 0),
            title=str(row["market_id"]),
            outcome="",
            transaction_hash=str(row["hash"]),
            source="indexed-policy",
        )
        for row in rows
    ]
    trades.sort(key=lambda item: item.timestamp)
    return trades


def run_policy_optimizer_from_db(
    db_path: str | Path,
    base_config: BacktestConfig,
    *,
    history_days: int = 30,
    limit: int = 100_000,
    candidates: tuple[PolicyCandidate, ...] = DEFAULT_POLICY_CANDIDATES,
    optimizer_config: OptimizerConfig | None = None,
) -> dict[str, Any]:
    import time

    since_ts = int(time.time()) - max(1, history_days) * 86400
    trades = load_indexed_policy_trades(
        db_path,
        min_score=base_config.min_wallet_score,
        since_ts=since_ts,
        limit=limit,
    )
    result = run_policy_optimizer(
        trades,
        base_config,
        candidates=candidates,
        optimizer_config=optimizer_config,
    )
    result["source"] = {
        "db_path": str(db_path),
        "uses_scored_wallets": True,
        "trades": len(trades),
    }
    return result


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
