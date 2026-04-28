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
    model = _fit_exit_model(examples)
    stats = {**_exit_stats(examples), **_model_stats(model)}
    model_path = Path(model_path)
    stats_path = Path(stats_path)
    examples_path = Path(examples_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    examples_path.parent.mkdir(parents=True, exist_ok=True)
    serializer = _dump_exit_model(model, model_path)
    stats_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    sample_examples = [_exit_example_payload(example, model) for example in examples[:10]]
    examples_path.write_text(
        json.dumps(sample_examples, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return {
        "examples": len(examples),
        "saved_examples": len(sample_examples),
        "stats": stats,
        "model_type": model.get("type", "unknown"),
        "serializer": serializer,
    }


def _exit_examples(
    db_path: Path,
    *,
    stable_wallets: set[str],
    limit: int | None = None,
) -> list[dict[str, Any]]:
    if not db_path.exists() or not stable_wallets:
        return []
    normalized_wallets = {wallet.lower() for wallet in stable_wallets if wallet}
    if not normalized_wallets:
        return []
    placeholders = ",".join("?" for _ in normalized_wallets)
    limit_sql = "LIMIT ?" if limit else ""
    params: list[Any] = [*sorted(normalized_wallets)]
    if limit:
        params.append(int(limit))
    query = f"""
    WITH fills AS (
        SELECT
            LOWER(user_address) AS wallet,
            market_id,
            UPPER(side) AS side,
            timestamp,
            block_number,
            log_index,
            hash,
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
    buys AS (
        SELECT * FROM fills WHERE side = 'BUY'
    ),
    sells AS (
        SELECT * FROM fills WHERE side = 'SELL'
    ),
    pairs AS (
        SELECT
            b.wallet,
            b.market_id,
            b.timestamp AS entry_ts,
            b.block_number AS entry_block,
            b.log_index AS entry_log_index,
            b.hash AS entry_hash,
            b.amount AS entry_amount,
            b.price AS entry_price,
            b.notional AS entry_notional,
            s.timestamp AS exit_ts,
            s.block_number AS exit_block,
            s.log_index AS exit_log_index,
            s.hash AS exit_hash,
            s.amount AS exit_amount,
            s.price AS exit_price,
            s.notional AS exit_notional
        FROM buys b
        JOIN sells s
          ON s.wallet = b.wallet
         AND s.market_id = b.market_id
         AND (
             s.timestamp > b.timestamp
             OR (
                 s.timestamp = b.timestamp
                 AND (
                     s.block_number > b.block_number
                     OR (s.block_number = b.block_number AND s.log_index > b.log_index)
                 )
             )
         )
        WHERE NOT EXISTS (
            SELECT 1
            FROM sells closer
            WHERE closer.wallet = b.wallet
              AND closer.market_id = b.market_id
              AND (
                  closer.timestamp > b.timestamp
                  OR (
                      closer.timestamp = b.timestamp
                      AND (
                          closer.block_number > b.block_number
                          OR (closer.block_number = b.block_number AND closer.log_index > b.log_index)
                      )
                  )
              )
              AND (
                  closer.timestamp < s.timestamp
                  OR (
                      closer.timestamp = s.timestamp
                      AND (
                          closer.block_number < s.block_number
                          OR (closer.block_number = s.block_number AND closer.log_index < s.log_index)
                      )
                  )
              )
        )
    ),
    examples AS (
        SELECT
            wallet,
            market_id,
            entry_ts,
            exit_ts,
            MAX(0, exit_ts - entry_ts) AS hold_seconds,
            entry_notional,
            ABS(exit_price - entry_price) AS volatility_proxy,
            CAST(strftime('%H', datetime(entry_ts, 'unixepoch')) AS INTEGER) AS hour_of_day,
            exit_price - entry_price AS pnl_proxy,
            0 AS partial_fixation,
            'BUY' AS entry_side,
            'SELL' AS exit_side,
            entry_price,
            exit_price,
            entry_amount,
            exit_amount,
            entry_hash,
            exit_hash
        FROM pairs
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
            "entry_side": str(row["entry_side"] or "BUY"),
            "exit_side": str(row["exit_side"] or "SELL"),
            "entry_price": float(row["entry_price"] or 0.0),
            "exit_price": float(row["exit_price"] or 0.0),
            "entry_amount": float(row["entry_amount"] or 0.0),
            "exit_amount": float(row["exit_amount"] or 0.0),
            "entry_hash": str(row["entry_hash"] or ""),
            "exit_hash": str(row["exit_hash"] or ""),
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
        return {
            "type": "dummy_median",
            "default_hold_seconds": 6 * 3600,
            "features": [],
            "train_examples": 0,
            "test_examples": 0,
            "mae_seconds": None,
            "r2": None,
            "fallback_reason": "no_examples",
        }
    features = ["entry_notional", "volatility_proxy", "hour_of_day"]
    train_examples, test_examples = _train_test_split(examples)
    train_holds = sorted(example["hold_seconds"] for example in train_examples)
    median_hold = _percentile(train_holds, 0.5)
    if len(train_examples) < 2 or not test_examples:
        return _dummy_exit_model(
            train_examples,
            test_examples,
            features,
            fallback_reason="not_enough_train_test_examples",
        )
    try:
        from sklearn.linear_model import Ridge  # type: ignore
    except ModuleNotFoundError:
        return _dummy_exit_model(
            train_examples,
            test_examples,
            features,
            fallback_reason="sklearn_unavailable",
        )
    x_rows = [[example[feature] for feature in features] for example in train_examples]
    y_rows = [example["hold_seconds"] for example in train_examples]
    model = Ridge(alpha=1.0)
    model.fit(x_rows, y_rows)
    test_x = [[example[feature] for feature in features] for example in test_examples]
    test_y = [example["hold_seconds"] for example in test_examples]
    predictions = [float(value) for value in model.predict(test_x)]
    mae = _mean_absolute_error(test_y, predictions)
    r2 = _r2_score(test_y, predictions)
    if r2 is not None and r2 < 0:
        fallback = _dummy_exit_model(
            train_examples,
            test_examples,
            features,
            fallback_reason="negative_r2",
        )
        fallback["rejected_model"] = {
            "type": "ridge",
            "mae_seconds": mae,
            "r2": r2,
            "intercept": float(model.intercept_),
            "coef": [float(value) for value in model.coef_],
        }
        return fallback
    return {
        "type": "ridge",
        "features": features,
        "default_hold_seconds": median_hold,
        "intercept": float(model.intercept_),
        "coef": [float(value) for value in model.coef_],
        "train_examples": len(train_examples),
        "test_examples": len(test_examples),
        "mae_seconds": mae,
        "r2": r2,
    }


def _train_test_split(
    examples: list[dict[str, Any]],
    *,
    train_ratio: float = 0.8,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(examples, key=lambda example: (float(example.get("entry_ts") or 0), str(example.get("entry_hash") or "")))
    if len(ordered) <= 1:
        return ordered, []
    split_index = int(len(ordered) * train_ratio)
    split_index = min(len(ordered) - 1, max(1, split_index))
    return ordered[:split_index], ordered[split_index:]


def _dummy_exit_model(
    train_examples: list[dict[str, Any]],
    test_examples: list[dict[str, Any]],
    features: list[str],
    *,
    fallback_reason: str,
) -> dict[str, Any]:
    train_holds = sorted(example["hold_seconds"] for example in train_examples)
    if not train_holds:
        train_holds = sorted(example["hold_seconds"] for example in test_examples)
    median_hold = _percentile(train_holds, 0.5) if train_holds else 6 * 3600
    test_y = [example["hold_seconds"] for example in test_examples]
    predictions = [median_hold for _ in test_y]
    return {
        "type": "dummy_median",
        "features": features,
        "default_hold_seconds": median_hold,
        "median_hold_seconds": median_hold,
        "p75_hold_seconds": _percentile(train_holds, 0.75) if train_holds else median_hold,
        "train_examples": len(train_examples),
        "test_examples": len(test_examples),
        "mae_seconds": _mean_absolute_error(test_y, predictions) if test_y else None,
        "r2": _r2_score(test_y, predictions) if test_y else None,
        "fallback_reason": fallback_reason,
    }


def _model_stats(model: dict[str, Any]) -> dict[str, Any]:
    return {
        "model_type": model.get("type", "unknown"),
        "train_examples": int(model.get("train_examples") or 0),
        "test_examples": int(model.get("test_examples") or 0),
        "mae_seconds": model.get("mae_seconds"),
        "r2": model.get("r2"),
        "fallback_reason": model.get("fallback_reason", ""),
    }


def _mean_absolute_error(y_true: list[float], y_pred: list[float]) -> float | None:
    if not y_true:
        return None
    return round(sum(abs(actual - predicted) for actual, predicted in zip(y_true, y_pred)) / len(y_true), 4)


def _r2_score(y_true: list[float], y_pred: list[float]) -> float | None:
    if not y_true:
        return None
    mean_y = sum(y_true) / len(y_true)
    ss_tot = sum((actual - mean_y) ** 2 for actual in y_true)
    ss_res = sum((actual - predicted) ** 2 for actual, predicted in zip(y_true, y_pred))
    if ss_tot <= 0:
        return 1.0 if ss_res <= 1e-9 else 0.0
    return round(1.0 - (ss_res / ss_tot), 4)


def _dump_exit_model(model: dict[str, Any], model_path: Path) -> str:
    try:
        import joblib  # type: ignore
    except ModuleNotFoundError:
        LOGGER.warning("joblib is not installed; falling back to pickle for exit model persistence")
        with model_path.open("wb") as handle:
            pickle.dump(model, handle)
        return "pickle"
    joblib.dump(model, model_path)
    return "joblib"


def _exit_example_payload(example: dict[str, Any], model: dict[str, Any]) -> dict[str, Any]:
    entry_price = float(example.get("entry_price") or 0.0)
    exit_price = float(example.get("exit_price") or 0.0)
    pnl_percent = ((exit_price - entry_price) / entry_price * 100.0) if entry_price else 0.0
    return {
        "whale_address": str(example.get("wallet") or ""),
        "market_id": str(example.get("market_id") or ""),
        "entry_time": int(example.get("entry_ts") or 0),
        "exit_time": int(example.get("exit_ts") or 0),
        "pnl_percent": round(pnl_percent, 4),
        "predicted_time": round(_predict_exit_time(example, model), 4),
    }


def _predict_exit_time(example: dict[str, Any], model: dict[str, Any]) -> float:
    if model.get("type") == "ridge":
        features = [str(feature) for feature in model.get("features", [])]
        coefs = [float(value) for value in model.get("coef", [])]
        prediction = float(model.get("intercept") or 0.0)
        for feature, coef in zip(features, coefs):
            prediction += coef * float(example.get(feature) or 0.0)
        return max(0.0, prediction)
    return max(0.0, float(model.get("default_hold_seconds") or model.get("median_hold_seconds") or 0.0))


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
