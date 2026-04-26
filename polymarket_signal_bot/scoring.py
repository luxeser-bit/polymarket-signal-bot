from __future__ import annotations

import math
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import Trade, Wallet, WalletScore

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - surfaced by calculate_all.
    pd = None  # type: ignore[assignment]


DEFAULT_INDEXER_DB_PATH = Path("data/indexer.db")


@dataclass(frozen=True)
class SQLScoringConfig:
    db_path: str | Path = DEFAULT_INDEXER_DB_PATH
    table: str = "raw_transactions"
    scored_table: str = "scored_wallets"
    limit: int | None = None
    since_ts: int | None = None
    min_trades: int = 1
    computed_at: int | None = None


def calculate_all(
    db_path: str | Path = DEFAULT_INDEXER_DB_PATH,
    *,
    limit: int | None = None,
    since_ts: int | None = None,
    min_trades: int = 1,
) -> Any:
    """Calculate wallet metrics from ``raw_transactions`` using SQL aggregations.

    The returned DataFrame contains one row per wallet with the 9 training
    metrics used by the autonomous feedback loop: pnl, sharpe, max_drawdown,
    profit_factor, win_rate, avg_hold_time, volume, trade_count, consistency.
    ``limit`` limits the raw transaction sample before aggregation, which keeps
    test runs fast on large indexed databases.
    """

    config = SQLScoringConfig(
        db_path=db_path,
        limit=limit,
        since_ts=since_ts,
        min_trades=min_trades,
    )
    conn = _connect(config.db_path)
    try:
        ensure_scoring_schema(conn, scored_table=config.scored_table)
        return calculate_all_from_connection(conn, config=config)
    finally:
        conn.close()


def calculate_all_from_connection(conn: sqlite3.Connection, *, config: SQLScoringConfig) -> Any:
    if pd is None:
        raise RuntimeError("pandas is required for SQL scoring. Install requirements-streamlit.txt.")
    _ensure_raw_transaction_indexes(conn, config.table)
    if not _table_exists(conn, config.table):
        return pd.DataFrame(columns=_score_columns())

    where = [
        "user_address != ''",
        "UPPER(side) IN ('BUY', 'SELL')",
        "CAST(price AS REAL) > 0",
        "ABS(CAST(amount AS REAL)) > 0",
    ]
    params: list[Any] = []
    if config.since_ts is not None:
        where.append("timestamp >= ?")
        params.append(int(config.since_ts))
    where_sql = " AND ".join(where)
    sample_sql = "ORDER BY timestamp DESC LIMIT ?" if config.limit else ""
    if config.limit:
        params.append(int(config.limit))

    query = f"""
    WITH raw_source AS (
        SELECT *
        FROM {config.table}
        WHERE {where_sql}
        {sample_sql}
    ),
    filtered AS (
        SELECT
            LOWER(user_address) AS wallet,
            COALESCE(NULLIF(market_id, ''), contract) AS market_id,
            UPPER(side) AS side,
            CAST(timestamp AS INTEGER) AS timestamp,
            CAST(timestamp / 86400 AS INTEGER) AS trade_day,
            CAST(amount AS REAL) AS amount,
            CAST(price AS REAL) AS price,
            CASE
                WHEN CAST(price AS REAL) > 0 THEN ABS(CAST(amount AS REAL) * CAST(price AS REAL))
                ELSE ABS(CAST(amount AS REAL))
            END AS notional,
            CASE
                WHEN UPPER(side) = 'SELL' THEN ABS(CAST(amount AS REAL) * CAST(price AS REAL))
                WHEN UPPER(side) = 'BUY' THEN -ABS(CAST(amount AS REAL) * CAST(price AS REAL))
                ELSE 0.0
            END AS pnl_proxy
        FROM raw_source
    ),
    wallet_base AS (
        SELECT
            wallet,
            COUNT(*) AS trade_count,
            SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buy_count,
            SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sell_count,
            COUNT(DISTINCT market_id) AS market_count,
            COUNT(DISTINCT trade_day) AS active_days,
            MIN(timestamp) AS first_trade_ts,
            MAX(timestamp) AS last_trade_ts,
            SUM(notional) AS volume,
            AVG(notional) AS avg_trade_size,
            SUM(pnl_proxy) AS pnl,
            SUM(CASE WHEN pnl_proxy > 0 THEN pnl_proxy ELSE 0 END) AS gross_profit,
            ABS(SUM(CASE WHEN pnl_proxy < 0 THEN pnl_proxy ELSE 0 END)) AS gross_loss,
            AVG(CASE WHEN pnl_proxy > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
        FROM filtered
        GROUP BY wallet
        HAVING COUNT(*) >= ?
    ),
    daily AS (
        SELECT wallet, trade_day, SUM(pnl_proxy) AS daily_pnl
        FROM filtered
        GROUP BY wallet, trade_day
    ),
    daily_stats AS (
        SELECT
            wallet,
            AVG(daily_pnl) AS avg_daily_pnl,
            AVG(daily_pnl * daily_pnl) AS avg_daily_pnl_sq,
            COUNT(*) AS daily_count
        FROM daily
        GROUP BY wallet
    ),
    equity AS (
        SELECT
            wallet,
            trade_day,
            SUM(daily_pnl) OVER (
                PARTITION BY wallet
                ORDER BY trade_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS equity
        FROM daily
    ),
    equity_with_peak AS (
        SELECT
            wallet,
            trade_day,
            equity,
            MAX(equity) OVER (
                PARTITION BY wallet
                ORDER BY trade_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS peak_equity
        FROM equity
    ),
    drawdowns AS (
        SELECT
            wallet,
            MAX(peak_equity - equity) AS max_drawdown
        FROM equity_with_peak
        GROUP BY wallet
    ),
    hold_pairs AS (
        SELECT
            wallet,
            market_id,
            timestamp,
            LEAD(timestamp) OVER (PARTITION BY wallet, market_id ORDER BY timestamp) AS next_ts
        FROM filtered
    ),
    hold_stats AS (
        SELECT wallet, AVG(next_ts - timestamp) AS avg_hold_time
        FROM hold_pairs
        WHERE next_ts IS NOT NULL AND next_ts > timestamp
        GROUP BY wallet
    )
    SELECT
        b.wallet,
        b.trade_count,
        b.buy_count,
        b.sell_count,
        b.market_count,
        b.active_days,
        b.first_trade_ts,
        b.last_trade_ts,
        COALESCE(b.volume, 0.0) AS volume,
        COALESCE(b.avg_trade_size, 0.0) AS avg_trade_size,
        COALESCE(b.pnl, 0.0) AS pnl,
        CASE
            WHEN COALESCE(b.gross_loss, 0.0) <= 0 THEN COALESCE(b.gross_profit, 0.0)
            ELSE COALESCE(b.gross_profit, 0.0) / b.gross_loss
        END AS profit_factor,
        COALESCE(b.win_rate, 0.0) AS win_rate,
        COALESCE(h.avg_hold_time, 0.0) AS avg_hold_time,
        COALESCE(d.max_drawdown, 0.0) AS max_drawdown,
        COALESCE(s.avg_daily_pnl, 0.0) AS avg_daily_pnl,
        COALESCE(s.avg_daily_pnl_sq, 0.0) AS avg_daily_pnl_sq,
        COALESCE(s.daily_count, 0) AS daily_count,
        CASE
            WHEN b.active_days <= 0 THEN 0.0
            ELSE MIN(1.0, CAST(b.active_days AS REAL) / MAX(1.0, ((b.last_trade_ts - b.first_trade_ts) / 86400.0) + 1.0))
        END AS consistency
    FROM wallet_base b
    LEFT JOIN daily_stats s ON s.wallet = b.wallet
    LEFT JOIN drawdowns d ON d.wallet = b.wallet
    LEFT JOIN hold_stats h ON h.wallet = b.wallet
    ORDER BY volume DESC, trade_count DESC
    """
    query_params = [*params, int(config.min_trades)]
    df = pd.read_sql_query(query, conn, params=tuple(query_params))
    if df.empty:
        return _empty_scores_df()
    return _finalize_score_frame(df, computed_at=config.computed_at or int(time.time()))


def save_scored_wallets(
    df: Any,
    db_path: str | Path = DEFAULT_INDEXER_DB_PATH,
    *,
    scored_table: str = "scored_wallets",
) -> int:
    conn = _connect(db_path)
    try:
        ensure_scoring_schema(conn, scored_table=scored_table)
        return save_scored_wallets_to_connection(conn, df, scored_table=scored_table)
    finally:
        conn.close()


def save_scored_wallets_to_connection(
    conn: sqlite3.Connection,
    df: Any,
    *,
    scored_table: str = "scored_wallets",
) -> int:
    if df is None or df.empty:
        return 0
    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                str(row["wallet"]),
                int(row["computed_at"]),
                float(row["score"]),
                float(row["pnl"]),
                float(row["sharpe"]),
                float(row["max_drawdown"]),
                float(row["profit_factor"]),
                float(row["win_rate"]),
                float(row["avg_hold_time"]),
                float(row["volume"]),
                int(row["trade_count"]),
                float(row["consistency"]),
                int(row["active_days"]),
                int(row["market_count"]),
                int(row["buy_count"]),
                int(row["sell_count"]),
                float(row["avg_trade_size"]),
                int(row["first_trade_ts"]),
                int(row["last_trade_ts"]),
                str(row.get("reason", "")),
            )
        )
    conn.executemany(
        f"""
        INSERT INTO {scored_table}(
            wallet, computed_at, score, pnl, sharpe, max_drawdown, profit_factor,
            win_rate, avg_hold_time, volume, trade_count, consistency, active_days,
            market_count, buy_count, sell_count, avg_trade_size, first_trade_ts,
            last_trade_ts, reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            computed_at = excluded.computed_at,
            score = excluded.score,
            pnl = excluded.pnl,
            sharpe = excluded.sharpe,
            max_drawdown = excluded.max_drawdown,
            profit_factor = excluded.profit_factor,
            win_rate = excluded.win_rate,
            avg_hold_time = excluded.avg_hold_time,
            volume = excluded.volume,
            trade_count = excluded.trade_count,
            consistency = excluded.consistency,
            active_days = excluded.active_days,
            market_count = excluded.market_count,
            buy_count = excluded.buy_count,
            sell_count = excluded.sell_count,
            avg_trade_size = excluded.avg_trade_size,
            first_trade_ts = excluded.first_trade_ts,
            last_trade_ts = excluded.last_trade_ts,
            reason = excluded.reason
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def ensure_scoring_schema(conn: sqlite3.Connection, *, scored_table: str = "scored_wallets") -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {scored_table} (
            wallet TEXT PRIMARY KEY,
            computed_at INTEGER NOT NULL,
            score REAL NOT NULL,
            pnl REAL NOT NULL,
            sharpe REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            profit_factor REAL NOT NULL,
            win_rate REAL NOT NULL,
            avg_hold_time REAL NOT NULL,
            volume REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            consistency REAL NOT NULL,
            active_days INTEGER NOT NULL,
            market_count INTEGER NOT NULL,
            buy_count INTEGER NOT NULL,
            sell_count INTEGER NOT NULL,
            avg_trade_size REAL NOT NULL,
            first_trade_ts INTEGER NOT NULL,
            last_trade_ts INTEGER NOT NULL,
            reason TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_{scored_table}_score ON {scored_table}(score DESC);
        CREATE INDEX IF NOT EXISTS idx_{scored_table}_computed ON {scored_table}(computed_at DESC);
        """
    )
    conn.commit()


def _connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_raw_transaction_indexes(conn: sqlite3.Connection, table: str) -> None:
    if not _table_exists(conn, table):
        return
    conn.executescript(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_user_time ON {table}(user_address, timestamp);
        CREATE INDEX IF NOT EXISTS idx_{table}_market_time ON {table}(market_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_{table}_timestamp ON {table}(timestamp);
        CREATE INDEX IF NOT EXISTS idx_{table}_event_user_time ON {table}(event_type, user_address, timestamp);
        CREATE INDEX IF NOT EXISTS idx_{table}_side_user_time ON {table}(side, user_address, timestamp);
        """
    )
    conn.commit()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _finalize_score_frame(df: Any, *, computed_at: int) -> Any:
    df = df.copy()
    for column in _numeric_score_inputs():
        df[column] = df[column].fillna(0)
    volume_denominator = max(1.0, float(df["volume"].quantile(0.95) or df["volume"].max() or 1.0))
    trade_denominator = max(1.0, float(df["trade_count"].quantile(0.95) or df["trade_count"].max() or 1.0))
    hold_denominator = max(1.0, float(df["avg_hold_time"].quantile(0.75) or 3600.0))
    variance = (df["avg_daily_pnl_sq"] - df["avg_daily_pnl"] * df["avg_daily_pnl"]).clip(lower=0)
    df["sharpe"] = 0.0
    valid_sharpe = (df["daily_count"] > 1) & (variance > 0)
    df.loc[valid_sharpe, "sharpe"] = df.loc[valid_sharpe, "avg_daily_pnl"] / (variance[valid_sharpe] ** 0.5)
    df["score"] = (
        0.18 * df["win_rate"].clip(0, 1)
        + 0.14 * (df["profit_factor"] / 3.0).clip(0, 1)
        + 0.14 * ((df["sharpe"] + 1.0) / 3.0).clip(0, 1)
        + 0.12 * (1.0 - (df["max_drawdown"] / (df["volume"].abs() + 1.0)).clip(0, 1))
        + 0.12 * (df["volume"] / volume_denominator).clip(0, 1)
        + 0.10 * (df["trade_count"] / trade_denominator).clip(0, 1)
        + 0.10 * df["consistency"].clip(0, 1)
        + 0.06 * (df["market_count"] / 12.0).clip(0, 1)
        + 0.04 * (df["avg_hold_time"] / hold_denominator).clip(0, 1)
    ).round(4)
    df["computed_at"] = int(computed_at)
    df["reason"] = (
        "sql_metrics pnl="
        + df["pnl"].round(4).astype(str)
        + " sharpe="
        + df["sharpe"].round(4).astype(str)
        + " pf="
        + df["profit_factor"].round(4).astype(str)
        + " win="
        + df["win_rate"].round(4).astype(str)
    )
    return df[_score_columns()].sort_values(["score", "volume"], ascending=[False, False])


def _empty_scores_df() -> Any:
    if pd is None:
        return []
    return pd.DataFrame(columns=_score_columns())


def _score_columns() -> list[str]:
    return [
        "wallet",
        "computed_at",
        "score",
        "pnl",
        "sharpe",
        "max_drawdown",
        "profit_factor",
        "win_rate",
        "avg_hold_time",
        "volume",
        "trade_count",
        "consistency",
        "active_days",
        "market_count",
        "buy_count",
        "sell_count",
        "avg_trade_size",
        "first_trade_ts",
        "last_trade_ts",
        "reason",
    ]


def _numeric_score_inputs() -> list[str]:
    return [
        "trade_count",
        "buy_count",
        "sell_count",
        "market_count",
        "active_days",
        "first_trade_ts",
        "last_trade_ts",
        "volume",
        "avg_trade_size",
        "pnl",
        "profit_factor",
        "win_rate",
        "avg_hold_time",
        "max_drawdown",
        "avg_daily_pnl",
        "avg_daily_pnl_sq",
        "daily_count",
        "consistency",
    ]


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
