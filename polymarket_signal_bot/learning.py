from __future__ import annotations

import time
from typing import Any

from .storage import Store
from .taxonomy import market_category, market_category_label


def wallet_outcome_report(
    store: Store,
    *,
    since_days: int = 90,
    min_events: int = 1,
    limit: int = 20,
) -> dict[str, Any]:
    since_ts = int(time.time()) - max(1, since_days) * 86400
    rows = store.conn.execute(
        """
        SELECT event_type, event_at, wallet, asset, outcome, title, cohort_status,
               policy_mode, risk_status, reason, size_usdc, pnl, confidence,
               wallet_score, hold_seconds
        FROM paper_events
        WHERE event_at >= ? AND wallet != ''
        """,
        (since_ts,),
    ).fetchall()

    groups: dict[tuple[str, str], dict[str, Any]] = {}
    category_totals: dict[str, dict[str, Any]] = {}
    for row in rows:
        category = market_category(" ".join([str(row["title"] or ""), str(row["outcome"] or "")]))
        group = groups.setdefault((str(row["wallet"]), category), _empty_group(str(row["wallet"]), category))
        category_group = category_totals.setdefault(category, _empty_group("", category))
        _apply_event(group, row)
        _apply_event(category_group, row)

    wallet_rows = [_finalize_group(group) for group in groups.values() if group["events"] >= min_events]
    wallet_rows.sort(key=lambda item: (item["learningScore"], item["pnl"], item["closed"]), reverse=True)
    category_rows = [_finalize_group(group) for group in category_totals.values()]
    category_rows.sort(key=lambda item: (item["pnl"], item["events"]), reverse=True)
    return {
        "sinceTs": since_ts,
        "sinceDays": since_days,
        "totalEvents": len(rows),
        "wallets": wallet_rows[:limit],
        "categories": category_rows,
    }


def format_wallet_outcome_summary(report: dict[str, Any]) -> str:
    return (
        f"window={int(report['sinceDays'])}d events={int(report['totalEvents'])} "
        f"wallet_category_rows={len(report['wallets'])}"
    )


def _empty_group(wallet: str, category: str) -> dict[str, Any]:
    return {
        "wallet": wallet,
        "category": category,
        "categoryLabel": market_category_label(category),
        "events": 0,
        "signals": 0,
        "opened": 0,
        "blocked": 0,
        "closed": 0,
        "wins": 0,
        "losses": 0,
        "pnl": 0.0,
        "size": 0.0,
        "confidence_sum": 0.0,
        "wallet_score_sum": 0.0,
        "hold_seconds_sum": 0,
        "risk_trim": 0,
        "stop_loss": 0,
        "take_profit": 0,
        "max_hold": 0,
        "stale_price": 0,
        "other_exits": 0,
        "block_reasons": {},
        "cohorts": {},
    }


def _apply_event(group: dict[str, Any], row: Any) -> None:
    event_type = str(row["event_type"] or "")
    reason = str(row["reason"] or "")
    cohort = str(row["cohort_status"] or "")
    pnl = float(row["pnl"] or 0.0)
    group["events"] += 1
    group["confidence_sum"] += float(row["confidence"] or 0.0)
    group["wallet_score_sum"] += float(row["wallet_score"] or 0.0)
    group["size"] += float(row["size_usdc"] or 0.0)
    if cohort:
        group["cohorts"][cohort] = group["cohorts"].get(cohort, 0) + 1
    if event_type == "SIGNAL_CREATED":
        group["signals"] += 1
    elif event_type == "OPENED":
        group["opened"] += 1
    elif event_type == "BLOCKED":
        group["blocked"] += 1
        group["block_reasons"][reason] = group["block_reasons"].get(reason, 0) + 1
    elif event_type == "CLOSED":
        group["closed"] += 1
        group["pnl"] += pnl
        group["hold_seconds_sum"] += int(row["hold_seconds"] or 0)
        if pnl > 0:
            group["wins"] += 1
        elif pnl < 0:
            group["losses"] += 1
        if reason in {"risk_trim", "stop_loss", "take_profit", "max_hold", "stale_price"}:
            group[reason] += 1
        else:
            group["other_exits"] += 1


def _finalize_group(group: dict[str, Any]) -> dict[str, Any]:
    events = max(1, int(group["events"]))
    closed = max(1, int(group["closed"]))
    signals = max(1, int(group["signals"]))
    avg_pnl = float(group["pnl"]) / closed
    hit_rate = float(group["wins"]) / closed if group["closed"] else 0.0
    blocked_rate = float(group["blocked"]) / signals
    risk_exit_rate = float(group["risk_trim"] + group["stop_loss"] + group["stale_price"]) / closed if group["closed"] else 0.0
    pnl_component = _clamp(0.5 + avg_pnl / 10.0)
    block_component = 1.0 - _clamp(blocked_rate) if group["signals"] else 0.5
    risk_component = 1.0 - _clamp(risk_exit_rate) if group["closed"] else 0.5
    learning_score = _clamp(
        0.45 * hit_rate
        + 0.25 * pnl_component
        + 0.20 * block_component
        + 0.10 * risk_component
    )
    if group["closed"] and group["wins"] == 0 and float(group["pnl"]) <= 0:
        learning_score = min(learning_score, 0.20)
    top_block_reason = _top_key(group["block_reasons"])
    top_cohort = _top_key(group["cohorts"])
    return {
        "wallet": group["wallet"],
        "category": group["category"],
        "categoryLabel": group["categoryLabel"],
        "events": int(group["events"]),
        "signals": int(group["signals"]),
        "opened": int(group["opened"]),
        "blocked": int(group["blocked"]),
        "closed": int(group["closed"]),
        "wins": int(group["wins"]),
        "losses": int(group["losses"]),
        "pnl": round(float(group["pnl"]), 4),
        "avgPnl": round(avg_pnl, 4) if group["closed"] else 0.0,
        "hitRate": round(hit_rate, 4),
        "blockedRate": round(blocked_rate, 4),
        "riskExitRate": round(risk_exit_rate, 4),
        "avgConfidence": round(float(group["confidence_sum"]) / events, 4),
        "avgWalletScore": round(float(group["wallet_score_sum"]) / events, 4),
        "avgHoldHours": round((int(group["hold_seconds_sum"]) / closed) / 3600, 2) if group["closed"] else 0.0,
        "riskTrim": int(group["risk_trim"]),
        "stopLoss": int(group["stop_loss"]),
        "takeProfit": int(group["take_profit"]),
        "maxHold": int(group["max_hold"]),
        "stalePrice": int(group["stale_price"]),
        "topBlockReason": top_block_reason,
        "topCohort": top_cohort,
        "learningScore": round(learning_score, 4),
    }


def _top_key(values: dict[str, int]) -> str:
    if not values:
        return ""
    return max(values.items(), key=lambda item: (item[1], item[0]))[0]


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))
