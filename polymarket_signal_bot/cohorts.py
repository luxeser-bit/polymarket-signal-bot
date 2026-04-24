from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .storage import Store


@dataclass(frozen=True)
class CohortConfig:
    history_days: int = 30
    min_notional: float = 0.0
    min_trades: int = 1
    limit: int = 20


def wallet_cohort_report(store: Store, config: CohortConfig | None = None) -> dict[str, Any]:
    config = config or CohortConfig()
    now = int(time.time())
    since_ts = now - max(1, config.history_days) * 86400
    rows = _wallet_rows(store, since_ts, config)
    daily = _daily_notional_by_wallet(store, since_ts)
    wallets = [_wallet_payload(row, daily.get(row["wallet"], []), now, config) for row in rows]
    wallets.sort(key=lambda item: (item["stabilityScore"], item["notional"]), reverse=True)
    return {
        "generatedAt": now,
        "historyDays": config.history_days,
        "wallets": wallets[: config.limit],
        "statusCohorts": _group_by(wallets, "status"),
        "sourceCohorts": _group_by(wallets, "source"),
        "totalWallets": len(wallets),
    }


def format_cohort_summary(report: dict[str, Any]) -> str:
    return (
        f"cohorts wallets={report['totalWallets']} "
        f"top={len(report['wallets'])} history_days={report['historyDays']}"
    )


def _wallet_rows(store: Store, since_ts: int, config: CohortConfig) -> list[dict[str, Any]]:
    rows = store.conn.execute(
        """
        SELECT
            t.proxy_wallet AS wallet,
            COALESCE(MAX(w.source), 'unknown') AS source,
            COUNT(*) AS trades,
            SUM(t.notional) AS notional,
            AVG(t.notional) AS avg_notional,
            COUNT(DISTINCT t.condition_id) AS market_count,
            COUNT(DISTINCT CAST(t.timestamp / 86400 AS INTEGER)) AS active_days,
            MIN(t.timestamp) AS first_trade_ts,
            MAX(t.timestamp) AS last_trade_ts,
            SUM(CASE WHEN t.side = 'BUY' THEN 1 ELSE 0 END) AS buys,
            SUM(CASE WHEN t.side = 'SELL' THEN 1 ELSE 0 END) AS sells,
            COALESCE(MAX(s.score), 0) AS wallet_score,
            COALESCE(MAX(s.repeatability_score), 0.5) AS repeatability_score,
            COALESCE(MAX(s.drawdown_score), 0.5) AS drawdown_score,
            COALESCE(MAX(s.leaderboard_pnl), 0) AS leaderboard_pnl,
            COALESCE(MAX(s.leaderboard_volume), 0) AS leaderboard_volume
        FROM trades t
        LEFT JOIN wallets w ON w.address = t.proxy_wallet
        LEFT JOIN wallet_scores s ON s.wallet = t.proxy_wallet
        WHERE t.timestamp >= ? AND t.proxy_wallet != ''
        GROUP BY t.proxy_wallet
        HAVING COUNT(*) >= ? AND SUM(t.notional) >= ?
        ORDER BY notional DESC
        """,
        (since_ts, config.min_trades, config.min_notional),
    ).fetchall()
    return [dict(row) for row in rows]


def _daily_notional_by_wallet(store: Store, since_ts: int) -> dict[str, list[float]]:
    rows = store.conn.execute(
        """
        SELECT
            proxy_wallet AS wallet,
            CAST(timestamp / 86400 AS INTEGER) AS trade_day,
            SUM(notional) AS day_notional
        FROM trades
        WHERE timestamp >= ? AND proxy_wallet != ''
        GROUP BY proxy_wallet, CAST(timestamp / 86400 AS INTEGER)
        """,
        (since_ts,),
    ).fetchall()
    daily: dict[str, list[float]] = {}
    for row in rows:
        daily.setdefault(str(row["wallet"]), []).append(float(row["day_notional"] or 0.0))
    return daily


def _wallet_payload(
    row: dict[str, Any],
    daily_notional: list[float],
    now: int,
    config: CohortConfig,
) -> dict[str, Any]:
    trades = int(row["trades"] or 0)
    notional = float(row["notional"] or 0.0)
    active_days = int(row["active_days"] or 0)
    market_count = int(row["market_count"] or 0)
    first_trade_ts = int(row["first_trade_ts"] or now)
    age_days = max(1, min(config.history_days, int((now - first_trade_ts) / 86400) + 1))
    day_coverage = _clamp(active_days / max(1, age_days))
    maturity = _clamp(active_days / 4)
    day_balance = _day_balance(daily_notional, notional)
    diversity = _clamp(market_count / 8)
    discipline = _discipline(trades, active_days)
    wallet_score = float(row["wallet_score"] or 0.0)
    repeatability = float(row["repeatability_score"] or 0.5)
    drawdown = float(row["drawdown_score"] or 0.5)
    source_score = _source_score(str(row["source"] or "unknown"))

    stability = _clamp(
        0.18 * wallet_score
        + 0.14 * repeatability
        + 0.12 * drawdown
        + 0.16 * day_balance
        + 0.14 * maturity
        + 0.10 * day_coverage
        + 0.08 * diversity
        + 0.06 * discipline
        + 0.02 * source_score
    )
    status = _status(stability, active_days, market_count, trades, notional, config)
    return {
        "wallet": str(row["wallet"]),
        "source": str(row["source"] or "unknown"),
        "status": status,
        "stabilityScore": round(stability, 4),
        "walletScore": round(wallet_score, 4),
        "repeatability": round(repeatability, 4),
        "drawdown": round(drawdown, 4),
        "dayBalance": round(day_balance, 4),
        "dayCoverage": round(day_coverage, 4),
        "discipline": round(discipline, 4),
        "activeDays": active_days,
        "marketCount": market_count,
        "trades": trades,
        "buys": int(row["buys"] or 0),
        "sells": int(row["sells"] or 0),
        "notional": round(notional, 2),
        "avgNotional": round(float(row["avg_notional"] or 0.0), 2),
        "leaderboardPnl": round(float(row["leaderboard_pnl"] or 0.0), 2),
        "leaderboardVolume": round(float(row["leaderboard_volume"] or 0.0), 2),
        "firstTradeAt": first_trade_ts,
        "lastTradeAt": int(row["last_trade_ts"] or 0),
    }


def _status(
    stability: float,
    active_days: int,
    market_count: int,
    trades: int,
    notional: float,
    config: CohortConfig,
) -> str:
    if trades < config.min_trades or notional < config.min_notional:
        return "NOISE"
    if active_days >= 4 and market_count >= 3 and stability >= 0.58:
        return "STABLE"
    if active_days >= 2 and market_count >= 2 and stability >= 0.46:
        return "CANDIDATE"
    if stability < 0.25 or trades <= 1:
        return "NOISE"
    return "WATCH"


def _day_balance(daily_notional: list[float], total_notional: float) -> float:
    if total_notional <= 0 or len(daily_notional) <= 1:
        return 0.0
    max_share = max(daily_notional) / total_notional
    return _clamp(1.0 - max_share)


def _discipline(trades: int, active_days: int) -> float:
    if active_days <= 0:
        return 0.0
    trades_per_day = trades / active_days
    if trades_per_day <= 12:
        return 1.0
    if trades_per_day >= 60:
        return 0.1
    return _clamp(1.0 - ((trades_per_day - 12) / 48))


def _source_score(source: str) -> float:
    if source == "leaderboard":
        return 1.0
    if source in {"market-flow", "trade-flow"}:
        return 0.72
    if source == "file":
        return 0.55
    return 0.42


def _group_by(wallets: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for wallet in wallets:
        value = str(wallet.get(key) or "unknown")
        item = groups.setdefault(
            value,
            {
                key: value,
                "wallets": 0,
                "stable": 0,
                "notional": 0.0,
                "stabilityTotal": 0.0,
            },
        )
        item["wallets"] += 1
        item["stable"] += 1 if wallet["status"] == "STABLE" else 0
        item["notional"] += float(wallet["notional"])
        item["stabilityTotal"] += float(wallet["stabilityScore"])
    rows = []
    for item in groups.values():
        wallets_count = max(1, int(item["wallets"]))
        rows.append(
            {
                key: item[key],
                "wallets": int(item["wallets"]),
                "stable": int(item["stable"]),
                "notional": round(float(item["notional"]), 2),
                "avgStability": round(float(item["stabilityTotal"]) / wallets_count, 4),
            }
        )
    rows.sort(key=lambda row: (row["stable"], row["avgStability"], row["notional"]), reverse=True)
    return rows


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))
