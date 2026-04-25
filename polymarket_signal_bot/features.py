from __future__ import annotations

import json
import time
from typing import Any

from .storage import Store
from .taxonomy import market_category


def build_decision_features(
    store: Store,
    *,
    since_days: int | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    rows = _decision_source_rows(store, since_days=since_days, limit=limit)
    built_at = int(time.time())
    features = [_feature_row(row, built_at) for row in rows]
    inserted = store.replace_decision_features(features)
    summary = store.decision_feature_summary()
    store.set_runtime_state("features_last_build_at", str(built_at))
    store.set_runtime_state("features_last_summary", format_feature_summary(summary))
    return {"inserted": inserted, "summary": summary}


def format_feature_summary(summary: dict[str, Any]) -> str:
    return (
        f"features={int(summary['features'])} "
        f"avg_learning={float(summary['avg_learning_score']):.3f} "
        f"avg_liquidity={float(summary['avg_liquidity_score']):.3f} "
        f"label_pnl=${float(summary['label_pnl']):.2f}"
    )


def _decision_source_rows(
    store: Store,
    *,
    since_days: int | None,
    limit: int | None,
) -> list[Any]:
    params: list[object] = []
    where = ""
    if since_days is not None:
        since_ts = int(time.time()) - max(1, since_days) * 86400
        where = "WHERE e.event_at >= ?"
        params.append(since_ts)
    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT ?"
        params.append(max(1, limit))
    return store.conn.execute(
        f"""
        SELECT
            e.*,
            s.reason AS signal_reason,
            s.wallet_score AS signal_wallet_score,
            s.confidence AS signal_confidence,
            s.size_usdc AS signal_size_usdc,
            s.observed_price AS signal_observed_price,
            s.suggested_price AS signal_suggested_price,
            s.stop_loss AS signal_stop_loss,
            s.take_profit AS signal_take_profit,
            s.title AS signal_title,
            s.outcome AS signal_outcome,
            s.condition_id AS signal_condition_id,
            h.observed_at AS book_observed_at,
            h.spread AS book_spread,
            h.liquidity_score AS book_liquidity_score,
            h.bid_depth_usdc AS book_bid_depth_usdc,
            h.ask_depth_usdc AS book_ask_depth_usdc
        FROM paper_events e
        LEFT JOIN signals s ON s.signal_id = e.signal_id
        LEFT JOIN order_books_history h ON h.snapshot_id = (
            SELECT h2.snapshot_id
            FROM order_books_history h2
            WHERE h2.asset = e.asset AND h2.observed_at <= e.event_at
            ORDER BY h2.observed_at DESC
            LIMIT 1
        )
        {where}
        ORDER BY e.event_at ASC
        {limit_clause}
        """,
        tuple(params),
    ).fetchall()


def _feature_row(row: Any, built_at: int) -> dict[str, object]:
    signal_reason = str(row["signal_reason"] or "")
    reason_fields = _parse_reason(signal_reason)
    event_type = str(row["event_type"] or "")
    reason = str(row["reason"] or "")
    title = str(row["title"] or row["signal_title"] or "")
    outcome = str(row["outcome"] or row["signal_outcome"] or "")
    category = market_category(" ".join([title, outcome, str(row["asset"] or "")]))
    label, label_win, close_reason, blocked_reason = _label(event_type, reason, float(row["pnl"] or 0.0))
    book_observed_at = int(row["book_observed_at"] or 0)
    event_at = int(row["event_at"] or 0)
    book_age_seconds = max(0, event_at - book_observed_at) if book_observed_at else 0
    metadata = {
        "source": "paper_events",
        "signal_reason": signal_reason,
        "book_source": "history" if book_observed_at else "none",
    }
    return {
        "feature_id": str(row["event_id"]),
        "built_at": built_at,
        "event_id": str(row["event_id"]),
        "event_at": event_at,
        "event_type": event_type,
        "signal_id": str(row["signal_id"] or ""),
        "position_id": str(row["position_id"] or ""),
        "wallet": str(row["wallet"] or ""),
        "asset": str(row["asset"] or ""),
        "condition_id": str(row["condition_id"] or row["signal_condition_id"] or ""),
        "category": category,
        "outcome": outcome,
        "title": title,
        "policy_mode": str(row["policy_mode"] or ""),
        "cohort_status": str(row["cohort_status"] or ""),
        "risk_status": str(row["risk_status"] or ""),
        "reason": reason,
        "wallet_score": _num(row["signal_wallet_score"], row["wallet_score"]),
        "signal_confidence": _num(row["signal_confidence"], row["confidence"]),
        "signal_size_usdc": _num(row["signal_size_usdc"], row["size_usdc"]),
        "event_price": _num(row["price"]),
        "observed_price": _num(row["signal_observed_price"]),
        "suggested_price": _num(row["signal_suggested_price"]),
        "stop_loss": _num(row["signal_stop_loss"]),
        "take_profit": _num(row["signal_take_profit"]),
        "learning_score": _reason_float(reason_fields, "learning_score", 0.5),
        "learning_events": int(_reason_float(reason_fields, "learning_events", 0)),
        "learning_delta": _reason_float(reason_fields, "learning_delta", 0.0),
        "learning_size_mult": _reason_float(reason_fields, "learning_size_mult", 1.0),
        "learning_auto_open": int(_reason_float(reason_fields, "learning_auto_open", 1)),
        "spread": _num(row["book_spread"], default=1.0),
        "liquidity_score": _num(row["book_liquidity_score"]),
        "bid_depth_usdc": _num(row["book_bid_depth_usdc"]),
        "ask_depth_usdc": _num(row["book_ask_depth_usdc"]),
        "book_age_seconds": book_age_seconds,
        "label": label,
        "label_pnl": float(row["pnl"] or 0.0),
        "label_win": label_win,
        "close_reason": close_reason,
        "blocked_reason": blocked_reason,
        "hold_seconds": int(row["hold_seconds"] or 0),
        "metadata_json": json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
    }


def _label(event_type: str, reason: str, pnl: float) -> tuple[str, int, str, str]:
    if event_type == "CLOSED":
        if pnl > 0:
            return "win", 1, reason, ""
        if pnl < 0:
            return "loss", 0, reason, ""
        return "flat", 0, reason, ""
    if event_type == "BLOCKED":
        return "blocked", 0, "", reason
    if event_type == "OPENED":
        return "opened", 0, "", ""
    if event_type == "SIGNAL_CREATED":
        return "created", 0, "", ""
    return event_type.lower() or "unknown", 0, "", ""


def _parse_reason(reason: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for item in reason.split(";"):
        key, sep, value = item.partition("=")
        if sep and key:
            fields[key.strip()] = value.strip()
    return fields


def _reason_float(fields: dict[str, str], key: str, default: float) -> float:
    try:
        return float(fields.get(key, default))
    except (TypeError, ValueError):
        return default


def _num(*values: Any, default: float = 0.0) -> float:
    for value in values:
        try:
            if value is not None and value != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return default
