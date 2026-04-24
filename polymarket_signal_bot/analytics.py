from __future__ import annotations

import importlib
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .storage import DEFAULT_DB_PATH
from .storage import Store
from .taxonomy import market_category


DEFAULT_DUCKDB_PATH = Path("data/polysignal.duckdb")

EXPORT_TABLES = [
    "wallets",
    "trades",
    "wallet_scores",
    "signals",
    "paper_positions",
    "paper_events",
    "order_books_latest",
    "signal_reviews",
    "wallet_sync_state",
    "alert_events",
]


class DuckDbMissingError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExportConfig:
    sqlite_path: Path = DEFAULT_DB_PATH
    duckdb_path: Path = DEFAULT_DUCKDB_PATH
    chunk_size: int = 50000
    rebuild: bool = False


def duckdb_available() -> bool:
    return importlib.util.find_spec("duckdb") is not None


def missing_duckdb_message() -> str:
    return (
        "DuckDB Python package is not installed. Install it first, for example: "
        "python -m pip install duckdb"
    )


def export_to_duckdb(config: ExportConfig) -> dict[str, Any]:
    duckdb = _load_duckdb()
    sqlite_path = Path(config.sqlite_path)
    duckdb_path = Path(config.duckdb_path)
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    if config.rebuild and duckdb_path.exists():
        duckdb_path.unlink()

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    duck_conn = duckdb.connect(str(duckdb_path))
    started = time.time()
    try:
        counts: dict[str, int] = {}
        for table in EXPORT_TABLES:
            if _sqlite_table_exists(sqlite_conn, table):
                counts[table] = _export_table(sqlite_conn, duck_conn, table, config.chunk_size)
        counts["asset_categories"] = _export_asset_categories(sqlite_conn, duck_conn, config.chunk_size)
        _create_views(duck_conn)
        duck_conn.execute(
            """
            CREATE OR REPLACE TABLE export_meta AS
            SELECT ?::VARCHAR AS sqlite_path,
                   ?::VARCHAR AS exported_at,
                   ?::DOUBLE AS elapsed_seconds
            """,
            (str(sqlite_path), time.strftime("%Y-%m-%d %H:%M:%S"), time.time() - started),
        )
        return {
            "duckdb_path": str(duckdb_path),
            "counts": counts,
            "elapsed_seconds": round(time.time() - started, 3),
            "size_bytes": duckdb_path.stat().st_size if duckdb_path.exists() else 0,
        }
    finally:
        duck_conn.close()
        sqlite_conn.close()


def refresh_analytics_snapshot(
    store: Store,
    *,
    duckdb_path: str | Path = DEFAULT_DUCKDB_PATH,
    chunk_size: int = 50000,
    rebuild: bool = False,
) -> dict[str, Any]:
    started_at = time.time()
    store.set_runtime_state("analytics_status", "running")
    try:
        result = export_to_duckdb(
            ExportConfig(
                sqlite_path=store.path,
                duckdb_path=Path(duckdb_path),
                chunk_size=chunk_size,
                rebuild=rebuild,
            )
        )
    except Exception as exc:  # noqa: BLE001 - analytics should not kill ingestion.
        store.set_runtime_state("analytics_status", "error")
        store.set_runtime_state("analytics_last_error", str(exc)[:300])
        return {
            "ok": False,
            "error": str(exc),
            "elapsed_seconds": round(time.time() - started_at, 3),
        }
    store.set_runtime_state("analytics_status", "ready")
    store.set_runtime_state(
        "analytics_last_summary",
        (
            f"duckdb={result['duckdb_path']} size={format_bytes(result['size_bytes'])} "
            f"elapsed={result['elapsed_seconds']}s"
        ),
    )
    store.set_runtime_state("analytics_last_export_at", str(int(time.time())))
    return {"ok": True, **result}


def analytics_report(duckdb_path: str | Path = DEFAULT_DUCKDB_PATH, *, limit: int = 10) -> dict[str, Any]:
    duckdb = _load_duckdb()
    path = Path(duckdb_path)
    if not path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {path}")
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return {
            "meta": _fetch_all(conn, "SELECT * FROM export_meta LIMIT 1"),
            "counts": _table_counts(conn),
            "categories": _fetch_all(
                conn,
                """
                SELECT category, trades, notional, wallets, assets
                FROM v_category_flow
                ORDER BY notional DESC
                LIMIT ?
                """,
                (limit,),
            ),
            "wallets": _fetch_all(
                conn,
                """
                SELECT proxy_wallet, trades, notional, active_days, market_count
                FROM v_wallet_flow
                ORDER BY notional DESC
                LIMIT ?
                """,
                (limit,),
            ),
            "markets": _fetch_all(
                conn,
                """
                SELECT asset, title, category, trades, notional, wallets, latest_price
                FROM v_market_flow
                ORDER BY notional DESC
                LIMIT ?
                """,
                (limit,),
            ),
            "cohorts": _fetch_all(
                conn,
                """
                SELECT proxy_wallet, source, status, stability_score, score,
                       repeatability_score, drawdown_score, trades, notional,
                       active_days, market_count
                FROM v_wallet_cohort
                ORDER BY stability_score DESC, notional DESC
                LIMIT ?
                """,
                (limit,),
            ),
            "daily": _fetch_all(
                conn,
                """
                SELECT trade_day, trades, notional, wallets
                FROM v_daily_flow
                ORDER BY trade_day DESC
                LIMIT ?
                """,
                (limit,),
            ),
            "paper_events": _fetch_all(
                conn,
                """
                SELECT event_type, reason, events, pnl, avg_confidence
                FROM v_paper_event_summary
                ORDER BY events DESC, event_type ASC, reason ASC
                LIMIT ?
                """,
                (limit,),
            ),
        }
    finally:
        conn.close()


def sqlite_counts(sqlite_path: str | Path = DEFAULT_DB_PATH) -> dict[str, int]:
    path = Path(sqlite_path)
    if not path.exists():
        return {}
    conn = sqlite3.connect(path)
    try:
        counts = {}
        for table in EXPORT_TABLES:
            if _sqlite_table_exists(conn, table):
                counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return counts
    finally:
        conn.close()


def _load_duckdb():
    try:
        return importlib.import_module("duckdb")
    except ImportError as exc:
        raise DuckDbMissingError(missing_duckdb_message()) from exc


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _export_table(
    sqlite_conn: sqlite3.Connection,
    duck_conn: Any,
    table: str,
    chunk_size: int,
) -> int:
    columns = _sqlite_columns(sqlite_conn, table)
    if not columns:
        return 0
    column_defs = ", ".join(f"{name} {_duck_type(sqlite_type)}" for name, sqlite_type in columns)
    duck_conn.execute(f"DROP TABLE IF EXISTS {table}")
    duck_conn.execute(f"CREATE TABLE {table} ({column_defs})")

    column_names = [name for name, _ in columns]
    quoted_cols = ", ".join(column_names)
    placeholders = ", ".join("?" for _ in column_names)
    cursor = sqlite_conn.execute(f"SELECT {quoted_cols} FROM {table}")
    insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"
    count = 0
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        values = [tuple(row[name] for name in column_names) for row in rows]
        duck_conn.executemany(insert_sql, values)
        count += len(values)
    return count


def _export_asset_categories(sqlite_conn: sqlite3.Connection, duck_conn: Any, chunk_size: int) -> int:
    duck_conn.execute("DROP TABLE IF EXISTS asset_categories")
    duck_conn.execute(
        """
        CREATE TABLE asset_categories (
            asset VARCHAR,
            condition_id VARCHAR,
            title VARCHAR,
            slug VARCHAR,
            event_slug VARCHAR,
            outcome VARCHAR,
            category VARCHAR
        )
        """
    )
    if not _sqlite_table_exists(sqlite_conn, "trades"):
        return 0
    cursor = sqlite_conn.execute(
        """
        SELECT asset,
               MAX(condition_id) AS condition_id,
               MAX(title) AS title,
               MAX(slug) AS slug,
               MAX(event_slug) AS event_slug,
               MAX(outcome) AS outcome
        FROM trades
        WHERE asset != ''
        GROUP BY asset
        """
    )
    insert_sql = "INSERT INTO asset_categories VALUES (?, ?, ?, ?, ?, ?, ?)"
    count = 0
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        values = []
        for row in rows:
            text = " ".join(str(row[key] or "") for key in row.keys())
            values.append(
                (
                    row["asset"],
                    row["condition_id"],
                    row["title"],
                    row["slug"],
                    row["event_slug"],
                    row["outcome"],
                    market_category(text),
                )
            )
        duck_conn.executemany(insert_sql, values)
        count += len(values)
    return count


def _create_views(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_wallet_flow AS
        SELECT
            proxy_wallet,
            COUNT(*) AS trades,
            SUM(notional) AS notional,
            AVG(notional) AS avg_notional,
            COUNT(DISTINCT CAST(to_timestamp(timestamp) AS DATE)) AS active_days,
            COUNT(DISTINCT condition_id) AS market_count,
            MIN(timestamp) AS first_trade_ts,
            MAX(timestamp) AS last_trade_ts,
            SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buys,
            SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sells
        FROM trades
        GROUP BY proxy_wallet
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_market_flow AS
        SELECT
            t.asset,
            MAX(t.condition_id) AS condition_id,
            MAX(t.title) AS title,
            COALESCE(MAX(c.category), 'other') AS category,
            COUNT(*) AS trades,
            SUM(t.notional) AS notional,
            COUNT(DISTINCT t.proxy_wallet) AS wallets,
            arg_max(t.price, t.timestamp) AS latest_price,
            MIN(t.timestamp) AS first_trade_ts,
            MAX(t.timestamp) AS last_trade_ts
        FROM trades t
        LEFT JOIN asset_categories c ON c.asset = t.asset
        GROUP BY t.asset
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_category_flow AS
        SELECT
            COALESCE(c.category, 'other') AS category,
            COUNT(*) AS trades,
            SUM(t.notional) AS notional,
            COUNT(DISTINCT t.proxy_wallet) AS wallets,
            COUNT(DISTINCT t.asset) AS assets
        FROM trades t
        LEFT JOIN asset_categories c ON c.asset = t.asset
        GROUP BY COALESCE(c.category, 'other')
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_daily_flow AS
        SELECT
            CAST(to_timestamp(timestamp) AS DATE) AS trade_day,
            COUNT(*) AS trades,
            SUM(notional) AS notional,
            COUNT(DISTINCT proxy_wallet) AS wallets,
            COUNT(DISTINCT asset) AS assets
        FROM trades
        GROUP BY CAST(to_timestamp(timestamp) AS DATE)
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_wallet_daily_flow AS
        SELECT
            proxy_wallet,
            CAST(to_timestamp(timestamp) AS DATE) AS trade_day,
            SUM(notional) AS day_notional
        FROM trades
        GROUP BY proxy_wallet, CAST(to_timestamp(timestamp) AS DATE)
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_wallet_cohort AS
        WITH day_balance AS (
            SELECT
                proxy_wallet,
                CASE
                    WHEN SUM(day_notional) <= 0 OR COUNT(*) <= 1 THEN 0.0
                    ELSE 1.0 - (MAX(day_notional) / SUM(day_notional))
                END AS day_balance
            FROM v_wallet_daily_flow
            GROUP BY proxy_wallet
        ),
        scored AS (
            SELECT
                f.proxy_wallet,
                COALESCE(w.source, 'unknown') AS source,
                f.trades,
                f.notional,
                f.avg_notional,
                f.active_days,
                f.market_count,
                f.buys,
                f.sells,
                COALESCE(s.score, 0.0) AS score,
                COALESCE(s.repeatability_score, 0.5) AS repeatability_score,
                COALESCE(s.drawdown_score, 0.5) AS drawdown_score,
                COALESCE(d.day_balance, 0.0) AS day_balance,
                LEAST(1.0, f.active_days / 4.0) AS maturity,
                LEAST(1.0, f.market_count / 8.0) AS diversity,
                CASE
                    WHEN f.active_days <= 0 THEN 0.0
                    WHEN f.trades / f.active_days <= 12 THEN 1.0
                    WHEN f.trades / f.active_days >= 60 THEN 0.1
                    ELSE LEAST(1.0, GREATEST(0.0, 1.0 - (((f.trades / f.active_days) - 12.0) / 48.0)))
                END AS discipline
            FROM v_wallet_flow f
            LEFT JOIN wallets w ON LOWER(w.address) = LOWER(f.proxy_wallet)
            LEFT JOIN wallet_scores s ON LOWER(s.wallet) = LOWER(f.proxy_wallet)
            LEFT JOIN day_balance d ON d.proxy_wallet = f.proxy_wallet
        ),
        stability AS (
            SELECT
                *,
                LEAST(1.0, GREATEST(0.0,
                    0.18 * score
                    + 0.14 * repeatability_score
                    + 0.12 * drawdown_score
                    + 0.16 * day_balance
                    + 0.14 * maturity
                    + 0.08 * diversity
                    + 0.06 * discipline
                )) AS stability_score
            FROM scored
        )
        SELECT
            *,
            CASE
                WHEN active_days >= 4 AND market_count >= 3 AND stability_score >= 0.58 THEN 'STABLE'
                WHEN active_days >= 2 AND market_count >= 2 AND stability_score >= 0.46 THEN 'CANDIDATE'
                WHEN stability_score < 0.25 OR trades <= 1 THEN 'NOISE'
                ELSE 'WATCH'
            END AS status
        FROM stability
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_liquidity_latest AS
        SELECT
            asset,
            best_bid,
            best_ask,
            mid,
            spread,
            bid_depth_usdc,
            ask_depth_usdc,
            liquidity_score,
            updated_at
        FROM order_books_latest
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_paper_event_summary AS
        SELECT
            event_type,
            COALESCE(NULLIF(reason, ''), 'none') AS reason,
            COUNT(*) AS events,
            COALESCE(SUM(pnl), 0) AS pnl,
            COALESCE(AVG(confidence), 0) AS avg_confidence,
            COALESCE(AVG(wallet_score), 0) AS avg_wallet_score
        FROM paper_events
        GROUP BY event_type, COALESCE(NULLIF(reason, ''), 'none')
        """
    )


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [(str(row[1]), str(row[2] or "TEXT")) for row in rows]


def _duck_type(sqlite_type: str) -> str:
    normalized = sqlite_type.upper()
    if "INT" in normalized:
        return "BIGINT"
    if "REAL" in normalized or "FLOA" in normalized or "DOUB" in normalized:
        return "DOUBLE"
    return "VARCHAR"


def _fetch_all(conn: Any, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    cursor = conn.execute(query, tuple(params))
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _table_counts(conn: Any) -> dict[str, int]:
    counts = {}
    for table in [*EXPORT_TABLES, "asset_categories"]:
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            (table,),
        ).fetchone()[0]
        if exists:
            counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return counts


def format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
