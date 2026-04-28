from __future__ import annotations

"""Autonomous training loop for indexed Polymarket history."""

import argparse
import json
import logging
import os
import pickle
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .cohorts import load_wallet_cohorts, update_cohorts
from .scoring import calculate_all, save_scored_wallets

LOGGER = logging.getLogger(__name__)

DEFAULT_INDEXER_DB_PATH = Path("data/indexer.db")
DEFAULT_MODEL_PATH = Path("data/exit_model.pkl")
DEFAULT_STATS_PATH = Path("data/exit_stats.json")
DEFAULT_EXAMPLES_PATH = Path("data/exit_examples.json")
DEFAULT_POLICY_PATH = Path("data/best_policy.json")


@dataclass(frozen=True)
class AutoTrainerConfig:
    db_path: Path = DEFAULT_INDEXER_DB_PATH
    model_path: Path = DEFAULT_MODEL_PATH
    stats_path: Path = DEFAULT_STATS_PATH
    examples_path: Path = DEFAULT_EXAMPLES_PATH
    policy_path: Path = DEFAULT_POLICY_PATH
    test: bool = False
    limit: int | None = None
    min_new_records: int = 0
    force: bool = True


def run_full_training_cycle(
    *,
    db_path: str | Path = DEFAULT_INDEXER_DB_PATH,
    model_path: str | Path = DEFAULT_MODEL_PATH,
    stats_path: str | Path = DEFAULT_STATS_PATH,
    examples_path: str | Path = DEFAULT_EXAMPLES_PATH,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    test: bool = False,
    limit: int | None = None,
    min_new_records: int = 0,
    force: bool = True,
) -> dict[str, Any]:
    config = AutoTrainerConfig(
        db_path=Path(db_path),
        model_path=Path(model_path),
        stats_path=Path(stats_path),
        examples_path=Path(examples_path),
        policy_path=Path(policy_path),
        test=test,
        limit=limit or (10_000 if test else None),
        min_new_records=min_new_records,
        force=force,
    )
    started = time.time()
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info("training cycle started db=%s test=%s limit=%s", config.db_path, config.test, config.limit)
    LOGGER.info("ensuring training schema")
    _ensure_training_schema(config.db_path)
    before_count = _raw_count(config.db_path)
    LOGGER.info("raw_transactions count=%s", before_count)
    if not force and not _has_enough_new_records(config.db_path, before_count, min_new_records):
        LOGGER.info("training skipped: not enough new records min_new_records=%s", min_new_records)
        return {
            "ok": True,
            "skipped": True,
            "reason": "not_enough_new_records",
            "raw_transactions": before_count,
        }

    try:
        LOGGER.info("scoring wallets from OrderFilled rows")
        scores = calculate_all(config.db_path, limit=config.limit, min_trades=1)
        LOGGER.info("scoring produced rows=%s", int(getattr(scores, "shape", [0])[0] if scores is not None else 0))
        LOGGER.info("saving scored_wallets")
        saved_scores = save_scored_wallets(scores, config.db_path)
        LOGGER.info("saved scored_wallets=%s", saved_scores)
        LOGGER.info("updating wallet_cohorts via cohorts.update_cohorts")
        cohort_result = update_cohorts(config.db_path, policy_path=config.policy_path)
        LOGGER.info("wallet_cohorts updated=%s counts=%s", cohort_result.get("updated"), cohort_result.get("counts"))
        stable_cohorts = load_wallet_cohorts(
            config.db_path,
            statuses={"STABLE", "CANDIDATE"},
            limit=50_000 if not test else 1_000,
        )
        LOGGER.info("stable/candidate wallets for exit model=%s", len(stable_cohorts))
        LOGGER.info("training exit model")
        exit_result = train_exit_model(
            config.db_path,
            stable_wallets=set(stable_cohorts),
            model_path=config.model_path,
            stats_path=config.stats_path,
            examples_path=config.examples_path,
            limit=config.limit,
        )
        LOGGER.info(
            "exit model trained type=%s examples=%s saved_examples=%s",
            exit_result.get("model_type"),
            exit_result.get("examples"),
            exit_result.get("saved_examples"),
        )
        elapsed = time.time() - started
        summary = {
            "ok": True,
            "skipped": False,
            "test": test,
            "db_path": str(config.db_path),
            "raw_transactions": before_count,
            "scored_wallets": int(saved_scores),
            "cohorts": cohort_result.get("counts", {}),
            "exit_examples": int(exit_result.get("examples", 0)),
            "exit_model": str(config.model_path),
            "exit_stats": str(config.stats_path),
            "exit_examples_path": str(config.examples_path),
            "elapsed_seconds": round(elapsed, 3),
        }
        LOGGER.info("recording training run summary")
        _record_training_run(config.db_path, summary, before_count)
        LOGGER.info("training cycle finished elapsed=%.3fs", elapsed)
        return summary
    except Exception as exc:  # noqa: BLE001 - keep previous model/cohorts on failure.
        LOGGER.exception("Auto training failed")
        summary = {
            "ok": False,
            "error": str(exc)[:500],
            "raw_transactions": before_count,
            "elapsed_seconds": round(time.time() - started, 3),
        }
        _record_training_run(config.db_path, summary, before_count)
        return summary


def train_exit_model(
    db_path: str | Path,
    *,
    stable_wallets: set[str],
    model_path: str | Path = DEFAULT_MODEL_PATH,
    stats_path: str | Path = DEFAULT_STATS_PATH,
    examples_path: str | Path = DEFAULT_EXAMPLES_PATH,
    limit: int | None = None,
) -> dict[str, Any]:
    examples = _exit_examples(Path(db_path), stable_wallets=stable_wallets, limit=limit)
    stats = _exit_stats(examples)
    model = _fit_exit_model(examples)
    model_path = Path(model_path)
    stats_path = Path(stats_path)
    examples_path = Path(examples_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    examples_path.parent.mkdir(parents=True, exist_ok=True)
    with model_path.open("wb") as handle:
        pickle.dump(model, handle)
    stats_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    sample_examples = examples[:10]
    examples_path.write_text(
        json.dumps(sample_examples, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return {
        "examples": len(examples),
        "saved_examples": len(sample_examples),
        "stats": stats,
        "model_type": model.get("type", "unknown"),
    }


def _exit_examples(
    db_path: Path,
    *,
    stable_wallets: set[str],
    limit: int | None = None,
) -> list[dict[str, Any]]:
    if not db_path.exists() or not stable_wallets:
        return []
    placeholders = ",".join("?" for _ in stable_wallets)
    limit_sql = "LIMIT ?" if limit else ""
    params: list[Any] = [*sorted(stable_wallets)]
    if limit:
        params.append(int(limit))
    query = f"""
    WITH fills AS (
        SELECT
            LOWER(user_address) AS wallet,
            market_id,
            UPPER(side) AS side,
            timestamp,
            ABS(amount) AS amount,
            price,
            CASE WHEN price > 0 THEN ABS(amount) * price ELSE ABS(amount) END AS notional
        FROM raw_transactions
        WHERE LOWER(user_address) IN ({placeholders})
          AND market_id != ''
          AND event_type = 'OrderFilled'
          AND UPPER(side) IN ('BUY', 'SELL')
          AND price > 0
          AND ABS(amount) > 0
          AND timestamp > 0
    ),
    ordered AS (
        SELECT
            wallet,
            market_id,
            side,
            timestamp,
            amount,
            price,
            notional,
            LEAD(timestamp) OVER (PARTITION BY wallet, market_id ORDER BY timestamp) AS exit_ts,
            LEAD(price) OVER (PARTITION BY wallet, market_id ORDER BY timestamp) AS exit_price,
            LEAD(side) OVER (PARTITION BY wallet, market_id ORDER BY timestamp) AS exit_side
        FROM fills
    ),
    examples AS (
        SELECT
            wallet,
            market_id,
            timestamp AS entry_ts,
            exit_ts,
            MAX(0, exit_ts - timestamp) AS hold_seconds,
            notional AS entry_notional,
            ABS(COALESCE(exit_price, price) - price) AS volatility_proxy,
            CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) AS hour_of_day,
            CASE
                WHEN side = 'BUY' THEN COALESCE(exit_price, price) - price
                WHEN side = 'SELL' THEN price - COALESCE(exit_price, price)
                ELSE 0
            END AS pnl_proxy,
            CASE WHEN exit_side = side THEN 1 ELSE 0 END AS partial_fixation
        FROM ordered
        WHERE exit_ts IS NOT NULL AND exit_ts > timestamp
    )
    SELECT * FROM examples
    ORDER BY entry_ts DESC
    {limit_sql}
    """
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, tuple(params)).fetchall()
    finally:
        conn.close()
    return [
        {
            "wallet": str(row["wallet"] or ""),
            "market_id": str(row["market_id"] or ""),
            "entry_ts": int(row["entry_ts"] or 0),
            "exit_ts": int(row["exit_ts"] or 0),
            "hold_seconds": float(row["hold_seconds"] or 0.0),
            "entry_notional": float(row["entry_notional"] or 0.0),
            "volatility_proxy": float(row["volatility_proxy"] or 0.0),
            "hour_of_day": float(row["hour_of_day"] or 0.0),
            "pnl_proxy": float(row["pnl_proxy"] or 0.0),
            "partial_fixation": float(row["partial_fixation"] or 0.0),
        }
        for row in rows
    ]


def _exit_stats(examples: list[dict[str, Any]]) -> dict[str, Any]:
    if not examples:
        return {
            "examples": 0,
            "median_hold_seconds": 0,
            "p75_hold_seconds": 0,
            "p90_hold_seconds": 0,
            "post_fixation_reversal_rate": 0.0,
        }
    holds = sorted(example["hold_seconds"] for example in examples)
    fixation = [example for example in examples if example["partial_fixation"] > 0]
    reversals = [example for example in fixation if example["pnl_proxy"] < 0]
    return {
        "examples": len(examples),
        "median_hold_seconds": _percentile(holds, 0.50),
        "p75_hold_seconds": _percentile(holds, 0.75),
        "p90_hold_seconds": _percentile(holds, 0.90),
        "post_fixation_reversal_rate": round(len(reversals) / max(1, len(fixation)), 4),
        "avg_entry_notional": round(sum(example["entry_notional"] for example in examples) / len(examples), 4),
    }


def _fit_exit_model(examples: list[dict[str, Any]]) -> dict[str, Any]:
    if not examples:
        return {"type": "rule", "default_hold_seconds": 6 * 3600, "features": []}
    features = ["entry_notional", "volatility_proxy", "hour_of_day"]
    try:
        from sklearn.linear_model import Ridge  # type: ignore
    except ModuleNotFoundError:
        holds = sorted(example["hold_seconds"] for example in examples)
        return {
            "type": "rule",
            "default_hold_seconds": _percentile(holds, 0.5),
            "p75_hold_seconds": _percentile(holds, 0.75),
            "features": features,
        }
    x_rows = [[example[feature] for feature in features] for example in examples]
    y_rows = [example["hold_seconds"] for example in examples]
    model = Ridge(alpha=1.0)
    model.fit(x_rows, y_rows)
    return {
        "type": "ridge",
        "features": features,
        "intercept": float(model.intercept_),
        "coef": [float(value) for value in model.coef_],
    }


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return round(values[0], 4)
    idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * pct))))
    return round(float(values[idx]), 4)


def _ensure_training_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS training_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at INTEGER NOT NULL,
                ok INTEGER NOT NULL,
                raw_transactions INTEGER NOT NULL,
                scored_wallets INTEGER NOT NULL DEFAULT 0,
                stable_wallets INTEGER NOT NULL DEFAULT 0,
                candidate_wallets INTEGER NOT NULL DEFAULT 0,
                watch_wallets INTEGER NOT NULL DEFAULT 0,
                noise_wallets INTEGER NOT NULL DEFAULT 0,
                exit_examples INTEGER NOT NULL DEFAULT 0,
                summary_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS training_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _record_training_run(db_path: Path, summary: dict[str, Any], raw_count: int) -> None:
    now = int(time.time())
    counts = summary.get("cohorts") if isinstance(summary.get("cohorts"), dict) else {}
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            INSERT INTO training_runs(
                started_at, ok, raw_transactions, scored_wallets, stable_wallets,
                candidate_wallets, watch_wallets, noise_wallets, exit_examples, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                1 if summary.get("ok") else 0,
                int(raw_count),
                int(summary.get("scored_wallets") or 0),
                int(counts.get("STABLE") or 0),
                int(counts.get("CANDIDATE") or 0),
                int(counts.get("WATCH") or 0),
                int(counts.get("NOISE") or 0),
                int(summary.get("exit_examples") or 0),
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.execute(
            """
            INSERT INTO training_state(key, value, updated_at)
            VALUES ('last_training_summary', ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (json.dumps(summary, ensure_ascii=False, sort_keys=True), now),
        )
        conn.commit()
    finally:
        conn.close()


def _raw_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        if not _table_exists(conn, "raw_transactions"):
            return 0
        return int(conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0])
    finally:
        conn.close()


def _has_enough_new_records(db_path: Path, current_count: int, min_new_records: int) -> bool:
    if min_new_records <= 0:
        return True
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        if not _table_exists(conn, "training_runs"):
            return True
        row = conn.execute(
            "SELECT raw_transactions FROM training_runs WHERE ok = 1 ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        previous = int(row[0]) if row else 0
    finally:
        conn.close()
    return current_count - previous >= min_new_records


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the autonomous Polymarket training cycle.")
    parser.add_argument("--db", default=os.environ.get("INDEXER_DB_PATH", str(DEFAULT_INDEXER_DB_PATH)))
    parser.add_argument("--model-path", default=str(DEFAULT_MODEL_PATH))
    parser.add_argument("--stats-path", default=str(DEFAULT_STATS_PATH))
    parser.add_argument("--examples-path", default=str(DEFAULT_EXAMPLES_PATH))
    parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    parser.add_argument("--test", action="store_true", help="Train on a limited sample and print a report.")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--min-new-records", type=int, default=0)
    parser.add_argument("--no-force", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    summary = run_full_training_cycle(
        db_path=args.db,
        model_path=args.model_path,
        stats_path=args.stats_path,
        examples_path=args.examples_path,
        policy_path=args.policy_path,
        test=args.test,
        limit=args.limit or None,
        min_new_records=args.min_new_records,
        force=not args.no_force,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
