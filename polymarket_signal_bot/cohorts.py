from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .storage import Store


DEFAULT_INDEXER_DB_PATH = Path("data/indexer.db")
DEFAULT_POLICY_PATH = Path("data/best_policy.json")


@dataclass(frozen=True)
class CohortConfig:
    history_days: int = 30
    min_notional: float = 0.0
    min_trades: int = 1
    limit: int = 20


@dataclass(frozen=True)
class CohortThresholds:
    stable_min_sharpe: float = 1.5
    stable_min_win_rate: float = 0.55
    stable_min_trades: int = 50
    stable_max_drawdown: float = 20.0
    stable_min_avg_hold_time: float = 300.0
    candidate_min_sharpe: float = 1.0
    candidate_min_win_rate: float = 0.50
    candidate_min_trades: int = 20
    candidate_max_drawdown: float = 30.0
    watch_min_win_rate: float = 0.45
    watch_min_trades: int = 10


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


def update_cohorts(
    db_path: str | Path = DEFAULT_INDEXER_DB_PATH,
    *,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    thresholds: CohortThresholds | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or load_cohort_thresholds(policy_path)
    conn = _connect(db_path)
    try:
        ensure_cohort_schema(conn)
        if not _table_exists(conn, "scored_wallets"):
            return {"updated": 0, "counts": {}, "thresholds": thresholds.__dict__}
        scored_count = int(conn.execute("SELECT COUNT(*) FROM scored_wallets").fetchone()[0] or 0)
        if scored_count <= 0:
            return {"updated": 0, "counts": {}, "thresholds": thresholds.__dict__}
        now = int(time.time())
        wallet_expr = _scored_wallet_address_expression(conn)
        score_expr = _scored_numeric_expression(conn, "score", "0.0")
        pnl_expr = _scored_numeric_expression(conn, "pnl", "0.0")
        sharpe_expr = _scored_numeric_expression(conn, "sharpe", "0.0")
        volume_expr = _scored_numeric_expression(conn, "volume", "0.0")
        trade_count_expr = _scored_numeric_expression(conn, "trade_count", "0")
        win_rate_expr = _scored_numeric_expression(conn, "win_rate", "0.0")
        profit_factor_expr = _scored_numeric_expression(conn, "profit_factor", "0.0")
        max_drawdown_expr = _scored_numeric_expression(conn, "max_drawdown", "999999.0")
        avg_hold_time_expr = _scored_numeric_expression(conn, "avg_hold_time", "0.0")
        conn.execute("DELETE FROM wallet_cohorts")
        conn.execute(
            f"""
            WITH scored AS (
                SELECT
                    {wallet_expr} AS wallet,
                    {score_expr} AS score,
                    {pnl_expr} AS pnl,
                    {sharpe_expr} AS sharpe,
                    {volume_expr} AS volume,
                    CAST({trade_count_expr} AS INTEGER) AS trade_count,
                    {win_rate_expr} AS win_rate,
                    {profit_factor_expr} AS profit_factor,
                    ABS({max_drawdown_expr}) AS max_drawdown,
                    {avg_hold_time_expr} AS avg_hold_time
                FROM scored_wallets
                WHERE {wallet_expr} != ''
            ),
            assigned AS (
                SELECT
                    *,
                    CASE
                        WHEN sharpe >= ?
                             AND win_rate >= ?
                             AND trade_count >= ?
                             AND max_drawdown <= ?
                             AND avg_hold_time >= ?
                            THEN 'STABLE'
                        WHEN sharpe >= ?
                             AND win_rate >= ?
                             AND trade_count >= ?
                             AND max_drawdown <= ?
                            THEN 'CANDIDATE'
                        WHEN win_rate >= ?
                             AND trade_count >= ?
                            THEN 'WATCH'
                        ELSE 'NOISE'
                    END AS cohort,
                    ROUND(
                        0.30 * MIN(1.0, MAX(0.0, sharpe / ?))
                        + 0.25 * MIN(1.0, MAX(0.0, win_rate / ?))
                        + 0.20 * MIN(1.0, MAX(0.0, CAST(trade_count AS REAL) / ?))
                        + 0.15 * (1.0 - MIN(1.0, MAX(0.0, max_drawdown / ?)))
                        + 0.10 * MIN(1.0, MAX(0.0, avg_hold_time / ?)),
                        4
                    ) AS stability_score
                FROM scored
            )
            INSERT INTO wallet_cohorts(
                wallet, user_address, status, cohort, stability_score, score, pnl, sharpe,
                volume, trade_count, win_rate, profit_factor, max_drawdown, avg_hold_time, updated_at
            )
            SELECT
                wallet,
                wallet AS user_address,
                cohort AS status,
                cohort,
                stability_score,
                score,
                pnl,
                sharpe,
                volume,
                trade_count,
                win_rate,
                profit_factor,
                max_drawdown,
                avg_hold_time,
                ?
            FROM assigned
            """,
            (
                thresholds.stable_min_sharpe,
                thresholds.stable_min_win_rate,
                thresholds.stable_min_trades,
                thresholds.stable_max_drawdown,
                thresholds.stable_min_avg_hold_time,
                thresholds.candidate_min_sharpe,
                thresholds.candidate_min_win_rate,
                thresholds.candidate_min_trades,
                thresholds.candidate_max_drawdown,
                thresholds.watch_min_win_rate,
                thresholds.watch_min_trades,
                max(thresholds.stable_min_sharpe, 0.0001),
                max(thresholds.stable_min_win_rate, 0.0001),
                max(float(thresholds.stable_min_trades), 1.0),
                max(thresholds.candidate_max_drawdown, thresholds.stable_max_drawdown, 0.0001),
                max(thresholds.stable_min_avg_hold_time, 1.0),
                now,
            ),
        )
        rows = conn.execute(
            "SELECT status, COUNT(*) AS wallets FROM wallet_cohorts GROUP BY status"
        ).fetchall()
        counts = {str(row["status"]): int(row["wallets"]) for row in rows}
        updated = sum(counts.values())
        conn.execute(
            """
            INSERT INTO training_state(key, value, updated_at)
            VALUES ('cohorts_last_summary', ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (json.dumps({"counts": counts, "updated": updated}, sort_keys=True), now),
        )
        conn.commit()
        return {"updated": updated, "counts": counts, "thresholds": thresholds.__dict__}
    finally:
        conn.close()


def load_wallet_cohorts(
    db_path: str | Path = DEFAULT_INDEXER_DB_PATH,
    *,
    statuses: set[str] | None = None,
    limit: int | None = None,
) -> dict[str, dict[str, Any]]:
    if not Path(db_path).exists():
        return {}
    conn = _connect(db_path, read_only=True)
    try:
        if not _table_exists(conn, "wallet_cohorts"):
            return {}
        columns = _table_columns(conn, "wallet_cohorts")
        wallet_expr = "COALESCE(NULLIF(user_address, ''), wallet)" if "user_address" in columns else "wallet"
        status_expr = "COALESCE(NULLIF(cohort, ''), status)" if "cohort" in columns else "status"
        sharpe_expr = "sharpe" if "sharpe" in columns else "0.0"
        avg_hold_time_expr = "avg_hold_time" if "avg_hold_time" in columns else "0.0"
        clauses = []
        params: list[Any] = []
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"{status_expr} IN ({placeholders})")
            params.extend(sorted(statuses))
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        limit_sql = "LIMIT ?" if limit else ""
        if limit:
            params.append(limit)
        rows = conn.execute(
            f"""
            SELECT
                {wallet_expr} AS wallet,
                {status_expr} AS status,
                stability_score,
                score,
                pnl,
                {sharpe_expr} AS sharpe,
                volume,
                trade_count,
                win_rate,
                profit_factor,
                max_drawdown,
                {avg_hold_time_expr} AS avg_hold_time,
                updated_at
            FROM wallet_cohorts
            {where}
            ORDER BY stability_score DESC, volume DESC
            {limit_sql}
            """,
            tuple(params),
        ).fetchall()
        return {
            str(row["wallet"]): {
                "wallet": str(row["wallet"]),
                "status": str(row["status"]),
                "cohort": str(row["status"]),
                "stabilityScore": float(row["stability_score"] or 0.0),
                "score": float(row["score"] or 0.0),
                "pnl": float(row["pnl"] or 0.0),
                "sharpe": float(row["sharpe"] or 0.0),
                "volume": float(row["volume"] or 0.0),
                "tradeCount": int(row["trade_count"] or 0),
                "winRate": float(row["win_rate"] or 0.0),
                "profitFactor": float(row["profit_factor"] or 0.0),
                "maxDrawdown": float(row["max_drawdown"] or 0.0),
                "avgHoldTime": float(row["avg_hold_time"] or 0.0),
                "updatedAt": int(row["updated_at"] or 0),
            }
            for row in rows
        }
    finally:
        conn.close()


def load_cohort_thresholds(policy_path: str | Path = DEFAULT_POLICY_PATH) -> CohortThresholds:
    path = Path(policy_path)
    if not path.exists():
        return CohortThresholds()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return CohortThresholds()
    source = _cohort_policy_source(payload)
    if not isinstance(source, dict):
        return CohortThresholds()
    defaults = CohortThresholds()
    values = _threshold_values_from_policy(source, defaults)
    return CohortThresholds(**values)


def _cohort_policy_source(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    for key in ("cohort_thresholds", "cohorts", "thresholds"):
        source = payload.get(key)
        if isinstance(source, dict):
            return source
    return payload


def _threshold_values_from_policy(source: dict[str, Any], defaults: CohortThresholds) -> dict[str, Any]:
    aliases: dict[str, tuple[str, tuple[str, ...]]] = {
        "stable_min_sharpe": ("stable", ("min_sharpe", "sharpe")),
        "stable_min_win_rate": ("stable", ("min_win_rate", "win_rate")),
        "stable_min_trades": ("stable", ("min_trades", "trade_count", "trades")),
        "stable_max_drawdown": ("stable", ("max_drawdown", "drawdown")),
        "stable_min_avg_hold_time": ("stable", ("min_avg_hold_time", "avg_hold_time", "hold_time")),
        "candidate_min_sharpe": ("candidate", ("min_sharpe", "sharpe")),
        "candidate_min_win_rate": ("candidate", ("min_win_rate", "win_rate")),
        "candidate_min_trades": ("candidate", ("min_trades", "trade_count", "trades")),
        "candidate_max_drawdown": ("candidate", ("max_drawdown", "drawdown")),
        "watch_min_win_rate": ("watch", ("min_win_rate", "win_rate")),
        "watch_min_trades": ("watch", ("min_trades", "trade_count", "trades")),
    }
    values: dict[str, Any] = {}
    for field, default in defaults.__dict__.items():
        cohort, nested_keys = aliases[field]
        raw_value = _policy_value(source, field, cohort, nested_keys)
        if raw_value is None:
            values[field] = default
            continue
        try:
            if isinstance(default, int):
                values[field] = int(float(raw_value))
            else:
                values[field] = float(raw_value)
        except (TypeError, ValueError):
            values[field] = default
    return values


def _policy_value(
    source: dict[str, Any],
    field: str,
    cohort: str,
    nested_keys: tuple[str, ...],
) -> Any:
    direct_keys = (
        field,
        field.replace("_min_", "_"),
        field.replace("_max_", "_"),
    )
    for key in direct_keys:
        if key in source:
            return source[key]

    for section_key in (cohort, cohort.upper(), cohort.capitalize()):
        section = source.get(section_key)
        if not isinstance(section, dict):
            continue
        for key in nested_keys:
            if key in section:
                return section[key]
    return None


def ensure_cohort_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS wallet_cohorts (
            wallet TEXT PRIMARY KEY,
            user_address TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            cohort TEXT NOT NULL DEFAULT '',
            stability_score REAL NOT NULL,
            score REAL NOT NULL,
            pnl REAL NOT NULL,
            sharpe REAL NOT NULL DEFAULT 0,
            volume REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            win_rate REAL NOT NULL,
            profit_factor REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            avg_hold_time REAL NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS training_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    _ensure_wallet_cohort_columns(conn)
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_wallet_cohorts_status_score
            ON wallet_cohorts(status, stability_score DESC);
        CREATE INDEX IF NOT EXISTS idx_wallet_cohorts_cohort_score
            ON wallet_cohorts(cohort, stability_score DESC);
        CREATE INDEX IF NOT EXISTS idx_wallet_cohorts_user_address
            ON wallet_cohorts(user_address);
        """
    )
    conn.commit()


def _ensure_wallet_cohort_columns(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "wallet_cohorts")
    migrations = {
        "user_address": "TEXT NOT NULL DEFAULT ''",
        "cohort": "TEXT NOT NULL DEFAULT ''",
        "sharpe": "REAL NOT NULL DEFAULT 0",
        "avg_hold_time": "REAL NOT NULL DEFAULT 0",
    }
    for column, ddl in migrations.items():
        if column not in columns:
            conn.execute(f"ALTER TABLE wallet_cohorts ADD COLUMN {column} {ddl}")

    conn.execute("UPDATE wallet_cohorts SET user_address = wallet WHERE COALESCE(user_address, '') = ''")
    conn.execute("UPDATE wallet_cohorts SET cohort = status WHERE COALESCE(cohort, '') = ''")


def _connect(db_path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    path = Path(db_path)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=30)
    else:
        uri = f"{path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _scored_wallet_address_expression(conn: sqlite3.Connection) -> str:
    columns = _table_columns(conn, "scored_wallets")
    if {"user_address", "address", "wallet"}.issubset(columns):
        return "COALESCE(NULLIF(user_address, ''), NULLIF(address, ''), wallet)"
    if {"user_address", "wallet"}.issubset(columns):
        return "COALESCE(NULLIF(user_address, ''), wallet)"
    if {"address", "wallet"}.issubset(columns):
        return "COALESCE(NULLIF(address, ''), wallet)"
    if "user_address" in columns:
        return "user_address"
    if "address" in columns:
        return "address"
    if "wallet" in columns:
        return "wallet"
    return "''"


def _scored_numeric_expression(conn: sqlite3.Connection, column: str, default: str) -> str:
    columns = _table_columns(conn, "scored_wallets")
    if column not in columns:
        return default
    return f"COALESCE(CAST({column} AS REAL), {default})"


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
