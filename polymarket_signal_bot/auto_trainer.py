from __future__ import annotations

"""Autonomous training loop for indexed Polymarket history."""

import argparse
import contextlib
import json
import logging
import math
import os
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
MIN_EXIT_SECONDS = 30
MAX_EXIT_SECONDS = 30 * 24 * 3600
XGBOOST_MIN_R2_LIFT = 0.02
XGBOOST_CV_FOLDS = 5

EXIT_MODEL_FEATURES = [
    "entry_notional",
    "market_volatility_60min",
    "spread_at_entry",
    "depth_at_entry",
    "whale_total_trades",
    "whale_total_volume",
    "whale_sharpe",
    "whale_win_rate",
    "time_to_expiration",
    "market_type_code",
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
]

MARKET_TYPE_CODES = {
    "other": 0.0,
    "crypto": 1.0,
    "sports": 2.0,
    "politics": 3.0,
    "economy": 4.0,
    "culture": 5.0,
}


@dataclass
class DummyExitRegressor:
    """Small joblib-friendly predictor used when learned models underperform."""

    median_hold_seconds: float

    def predict(self, rows: Any) -> list[float]:
        items = list(rows) if not hasattr(rows, "shape") else rows
        count = len(items)
        return [float(self.median_hold_seconds) for _ in range(count)]


@dataclass
class ExitModelPredictor:
    """Thin wrapper that lets saved sklearn/XGBoost pipelines predict from dict rows."""

    estimator: Any
    features: list[str]
    model_type: str
    default_hold_seconds: float

    def predict(self, rows: Any) -> list[float]:
        matrix = _coerce_feature_matrix(rows, self.features)
        try:
            predictions = self.estimator.predict(matrix)
        except Exception:
            return [float(self.default_hold_seconds) for _ in range(len(matrix))]
        return [max(0.0, float(value)) for value in predictions]


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
            "exit_model_type": exit_result.get("model_type"),
            "exit_model_r2": exit_result.get("stats", {}).get("r2") if isinstance(exit_result.get("stats"), dict) else None,
            "exit_model_mae_seconds": (
                exit_result.get("stats", {}).get("mae_seconds") if isinstance(exit_result.get("stats"), dict) else None
            ),
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
    LOGGER.info("collecting strict BUY->SELL exit examples wallets=%s limit=%s", len(stable_wallets), limit)
    examples = _exit_examples(Path(db_path), stable_wallets=stable_wallets, limit=limit)
    LOGGER.info("collected exit examples=%s", len(examples))
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
    fill_limit_sql = "ORDER BY timestamp DESC LIMIT ?" if limit else ""
    params: list[Any] = [*sorted(normalized_wallets)]
    if limit:
        params.append(max(int(limit) * 50, int(limit)))
        params.append(int(limit))
    query = f"""
    WITH fills AS (
        SELECT
            LOWER(user_address) AS wallet,
            market_id,
            UPPER(side) AS side,
            CAST(timestamp AS INTEGER) AS timestamp,
            CAST(block_number AS INTEGER) AS block_number,
            CAST(log_index AS INTEGER) AS log_index,
            hash,
            COALESCE(raw_json, '') AS raw_json,
            ABS(CAST(amount AS REAL)) AS amount,
            CAST(price AS REAL) AS price,
            CASE
                WHEN CAST(price AS REAL) > 0 THEN ABS(CAST(amount AS REAL)) * CAST(price AS REAL)
                ELSE ABS(CAST(amount AS REAL))
            END AS notional
        FROM raw_transactions
        WHERE user_address IN ({placeholders})
          AND market_id != ''
          AND event_type = 'OrderFilled'
          AND UPPER(side) IN ('BUY', 'SELL')
          AND CAST(price AS REAL) > 0
          AND ABS(CAST(amount AS REAL)) > 0
          AND CAST(timestamp AS INTEGER) > 0
        {fill_limit_sql}
    ),
    ordered AS (
        SELECT
            *,
            ROW_NUMBER() OVER fill_order AS fill_seq
        FROM fills
        WINDOW fill_order AS (
            PARTITION BY wallet, market_id
            ORDER BY timestamp, block_number, log_index, hash
        )
    ),
    with_next_sell AS (
        SELECT
            *,
            MIN(CASE WHEN side = 'SELL' THEN fill_seq END) OVER (
                PARTITION BY wallet, market_id
                ORDER BY fill_seq
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            ) AS next_sell_seq
        FROM ordered
    ),
    pairs AS (
        SELECT
            b.wallet,
            b.market_id,
            b.timestamp AS entry_ts,
            b.block_number AS entry_block,
            b.log_index AS entry_log_index,
            b.hash AS entry_hash,
            b.raw_json AS entry_raw_json,
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
        FROM with_next_sell b
        JOIN ordered s
          ON s.wallet = b.wallet
         AND s.market_id = b.market_id
         AND s.fill_seq = b.next_sell_seq
        WHERE b.side = 'BUY'
          AND s.side = 'SELL'
          AND b.next_sell_seq IS NOT NULL
    ),
    candidate_pairs AS (
        SELECT *
        FROM pairs
        WHERE MAX(0, exit_ts - entry_ts) BETWEEN {MIN_EXIT_SECONDS} AND {MAX_EXIT_SECONDS}
          AND ABS(CASE WHEN entry_price > 0 THEN ((exit_price - entry_price) / entry_price) * 100.0 ELSE 0.0 END) <= 99.0
        ORDER BY entry_ts DESC
        {limit_sql}
    ),
    examples AS (
        SELECT
            candidate_pairs.wallet,
            candidate_pairs.market_id,
            candidate_pairs.entry_ts,
            candidate_pairs.exit_ts,
            MAX(0, candidate_pairs.exit_ts - candidate_pairs.entry_ts) AS hold_seconds,
            candidate_pairs.entry_notional,
            COALESCE((
                SELECT
                    CASE
                        WHEN COUNT(*) <= 1 THEN 0.0
                        WHEN (AVG(CAST(mv.price AS REAL) * CAST(mv.price AS REAL)) - AVG(CAST(mv.price AS REAL)) * AVG(CAST(mv.price AS REAL))) <= 0 THEN 0.0
                        ELSE SQRT(AVG(CAST(mv.price AS REAL) * CAST(mv.price AS REAL)) - AVG(CAST(mv.price AS REAL)) * AVG(CAST(mv.price AS REAL)))
                    END
                FROM raw_transactions mv
                WHERE mv.event_type = 'OrderFilled'
                  AND mv.market_id = candidate_pairs.market_id
                  AND CAST(mv.price AS REAL) > 0
                  AND CAST(mv.timestamp AS INTEGER) BETWEEN candidate_pairs.entry_ts - 3600 AND candidate_pairs.entry_ts
            ), ABS(candidate_pairs.exit_price - candidate_pairs.entry_price)) AS market_volatility_60min,
            0.0 AS spread_at_entry,
            0.0 AS depth_at_entry,
            CAST(strftime('%H', datetime(candidate_pairs.entry_ts, 'unixepoch')) AS INTEGER) AS hour_of_day,
            CAST(strftime('%w', datetime(candidate_pairs.entry_ts, 'unixepoch')) AS INTEGER) AS day_of_week,
            (candidate_pairs.exit_price - candidate_pairs.entry_price) * MIN(candidate_pairs.entry_amount, candidate_pairs.exit_amount) AS pnl_proxy,
            CASE
                WHEN candidate_pairs.entry_price > 0 THEN ((candidate_pairs.exit_price - candidate_pairs.entry_price) / candidate_pairs.entry_price) * 100.0
                ELSE 0.0
            END AS pnl_percent,
            0 AS partial_fixation,
            'BUY' AS entry_side,
            'SELL' AS exit_side,
            candidate_pairs.entry_price,
            candidate_pairs.exit_price,
            candidate_pairs.entry_amount,
            candidate_pairs.exit_amount,
            candidate_pairs.entry_hash,
            candidate_pairs.exit_hash,
            candidate_pairs.entry_raw_json,
            COALESCE(sw.trade_count, 0) AS whale_total_trades,
            COALESCE(sw.volume, 0.0) AS whale_total_volume,
            COALESCE(sw.sharpe, 0.0) AS whale_sharpe,
            COALESCE(sw.win_rate, 0.0) AS whale_win_rate
        FROM candidate_pairs
        LEFT JOIN scored_wallets sw ON sw.wallet = candidate_pairs.wallet
    )
    SELECT * FROM examples
    ORDER BY entry_ts DESC
    """
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scored_wallets (
                wallet TEXT PRIMARY KEY,
                trade_count INTEGER NOT NULL DEFAULT 0,
                volume REAL NOT NULL DEFAULT 0,
                sharpe REAL NOT NULL DEFAULT 0,
                win_rate REAL NOT NULL DEFAULT 0
            )
            """
        )
        rows = conn.execute(query, tuple(params)).fetchall()
    finally:
        conn.close()
    return [_normalize_exit_example(dict(row)) for row in rows]


def _normalize_exit_example(row: dict[str, Any]) -> dict[str, Any]:
    entry_ts = int(row.get("entry_ts") or 0)
    hour_of_day = float(row.get("hour_of_day") or 0.0)
    day_of_week = float(row.get("day_of_week") or 0.0)
    raw_json = str(row.get("entry_raw_json") or "")
    title = _raw_event_title(raw_json) or str(row.get("market_id") or "")
    market_type = _market_type(title)
    expiration_ts = _raw_event_expiration(raw_json)
    time_to_expiration = max(0.0, float(expiration_ts - entry_ts)) if expiration_ts and entry_ts else 0.0
    hour_angle = 2.0 * math.pi * (hour_of_day % 24.0) / 24.0
    day_angle = 2.0 * math.pi * (day_of_week % 7.0) / 7.0
    market_volatility = float(row.get("market_volatility_60min", row.get("volatility_proxy", 0.0)) or 0.0)
    example = {
        "wallet": str(row.get("wallet") or ""),
        "market_id": str(row.get("market_id") or ""),
        "entry_ts": entry_ts,
        "exit_ts": int(row.get("exit_ts") or 0),
        "hold_seconds": float(row.get("hold_seconds") or 0.0),
        "entry_notional": float(row.get("entry_notional") or 0.0),
        "volatility_proxy": market_volatility,
        "market_volatility_60min": market_volatility,
        "spread_at_entry": float(row.get("spread_at_entry") or 0.0),
        "depth_at_entry": float(row.get("depth_at_entry") or 0.0),
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "hour_sin": math.sin(hour_angle),
        "hour_cos": math.cos(hour_angle),
        "day_sin": math.sin(day_angle),
        "day_cos": math.cos(day_angle),
        "market_type": market_type,
        "market_type_code": MARKET_TYPE_CODES.get(market_type, MARKET_TYPE_CODES["other"]),
        "time_to_expiration": time_to_expiration,
        "whale_total_trades": float(row.get("whale_total_trades") or 0.0),
        "whale_total_volume": float(row.get("whale_total_volume") or 0.0),
        "whale_sharpe": float(row.get("whale_sharpe") or 0.0),
        "whale_win_rate": float(row.get("whale_win_rate") or 0.0),
        "pnl_proxy": float(row.get("pnl_proxy") or 0.0),
        "pnl_percent": float(row.get("pnl_percent") or 0.0),
        "partial_fixation": float(row.get("partial_fixation") or 0.0),
        "entry_side": str(row.get("entry_side") or "BUY"),
        "exit_side": str(row.get("exit_side") or "SELL"),
        "entry_price": float(row.get("entry_price") or 0.0),
        "exit_price": float(row.get("exit_price") or 0.0),
        "entry_amount": float(row.get("entry_amount") or 0.0),
        "exit_amount": float(row.get("exit_amount") or 0.0),
        "entry_hash": str(row.get("entry_hash") or ""),
        "exit_hash": str(row.get("exit_hash") or ""),
    }
    return example


def _raw_event_title(raw_json: str) -> str:
    payload = _safe_json(raw_json)
    if not isinstance(payload, dict):
        return ""
    keys = ("title", "market_title", "question", "slug", "name")
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for nested_key in ("market", "condition", "event"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            for key in keys:
                value = nested.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return ""


def _raw_event_expiration(raw_json: str) -> int:
    payload = _safe_json(raw_json)
    if not isinstance(payload, dict):
        return 0
    keys = ("end_date", "endDate", "expiration", "expiration_ts", "close_time", "end_ts")
    for key in keys:
        value = payload.get(key)
        parsed = _parse_timestamp(value)
        if parsed:
            return parsed
    for nested_key in ("market", "condition", "event"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            for key in keys:
                parsed = _parse_timestamp(nested.get(key))
                if parsed:
                    return parsed
    return 0


def _safe_json(raw_json: str) -> Any:
    if not raw_json:
        return None
    try:
        return json.loads(raw_json)
    except (TypeError, json.JSONDecodeError):
        return None


def _parse_timestamp(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        timestamp = int(value)
        return timestamp // 1000 if timestamp > 10_000_000_000 else timestamp
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0
        if stripped.isdigit():
            return _parse_timestamp(int(stripped))
        with contextlib.suppress(ValueError):
            from datetime import datetime

            return int(datetime.fromisoformat(stripped.replace("Z", "+00:00")).timestamp())
    return 0


def _market_type(text: str) -> str:
    lower = text.lower()
    if any(token in lower for token in ("bitcoin", "btc", "ethereum", "eth", "solana", "crypto", "xrp", "doge")):
        return "crypto"
    if any(token in lower for token in (" vs ", "spread", "nba", "nfl", "nhl", "mlb", "ufc", "fifa", "champions")):
        return "sports"
    if any(token in lower for token in ("election", "trump", "biden", "senate", "president", "minister", "congress")):
        return "politics"
    if any(token in lower for token in ("fed", "rates", "inflation", "cpi", "oil", "gdp", "unemployment")):
        return "economy"
    if any(token in lower for token in ("oscar", "grammy", "movie", "album", "song", "celebrity")):
        return "culture"
    return "other"


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
            "features": EXIT_MODEL_FEATURES,
            "train_examples": 0,
            "test_examples": 0,
            "mae_seconds": None,
            "r2": None,
            "fallback_reason": "no_examples",
            "predictor": DummyExitRegressor(6 * 3600),
        }
    features = list(EXIT_MODEL_FEATURES)
    train_examples, test_examples = _train_test_split(examples)
    train_holds = sorted(example["hold_seconds"] for example in train_examples)
    median_hold = _percentile(train_holds, 0.5)
    if len(train_examples) < 2 or not test_examples or len(examples) < 50:
        return _dummy_exit_model(
            train_examples,
            test_examples,
            features,
            fallback_reason="not_enough_examples_for_ml",
        )
    try:
        from sklearn.linear_model import Ridge  # type: ignore
        from sklearn.pipeline import Pipeline  # type: ignore
        from sklearn.preprocessing import StandardScaler  # type: ignore
    except ModuleNotFoundError:
        return _dummy_exit_model(
            train_examples,
            test_examples,
            features,
            fallback_reason="sklearn_unavailable",
        )
    train_x = _feature_matrix(train_examples, features)
    train_y = [float(example["hold_seconds"]) for example in train_examples]
    test_x = _feature_matrix(test_examples, features)
    test_y = [float(example["hold_seconds"]) for example in test_examples]

    ridge_pipeline = Pipeline([("scaler", StandardScaler()), ("model", Ridge(alpha=1.0))])
    ridge_pipeline.fit(train_x, train_y)
    ridge_predictions = [float(value) for value in ridge_pipeline.predict(test_x)]
    ridge_result = _candidate_model_result(
        "ridge",
        ridge_pipeline,
        train_examples,
        test_examples,
        features,
        median_hold,
        test_y,
        ridge_predictions,
    )

    xgboost_result = _fit_xgboost_candidate(
        train_examples,
        test_examples,
        features,
        median_hold,
        pipeline_cls=Pipeline,
        scaler_cls=StandardScaler,
    )
    ridge_r2 = _score_value(ridge_result.get("r2"))
    xgboost_r2 = _score_value(xgboost_result.get("r2")) if xgboost_result else None
    if xgboost_result and xgboost_r2 is not None and xgboost_r2 > 0 and xgboost_r2 > (ridge_r2 or 0.0) + XGBOOST_MIN_R2_LIFT:
        xgboost_result["rejected_models"] = [_strip_predictor(ridge_result)]
        return xgboost_result
    if ridge_r2 is not None and ridge_r2 > 0:
        if xgboost_result:
            ridge_result["rejected_models"] = [_strip_predictor(xgboost_result)]
        return ridge_result

    fallback = _dummy_exit_model(
        train_examples,
        test_examples,
        features,
        fallback_reason="non_positive_model_r2",
    )
    rejected = [_strip_predictor(ridge_result)]
    if xgboost_result:
        rejected.append(_strip_predictor(xgboost_result))
    fallback["rejected_models"] = rejected
    return fallback


def _fit_xgboost_candidate(
    train_examples: list[dict[str, Any]],
    test_examples: list[dict[str, Any]],
    features: list[str],
    median_hold: float,
    *,
    pipeline_cls: Any,
    scaler_cls: Any,
) -> dict[str, Any] | None:
    try:
        from xgboost import XGBRegressor  # type: ignore
    except ModuleNotFoundError:
        LOGGER.warning("xgboost is not installed; exit model will compare Ridge vs Dummy only")
        return None
    except Exception as exc:  # noqa: BLE001 - xgboost can fail to import native runtime.
        LOGGER.warning("xgboost unavailable: %s", exc)
        return None

    train_x = _feature_matrix(train_examples, features)
    train_y = [float(example["hold_seconds"]) for example in train_examples]
    test_x = _feature_matrix(test_examples, features)
    test_y = [float(example["hold_seconds"]) for example in test_examples]
    estimator = XGBRegressor(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42,
        n_jobs=1,
    )
    pipeline = pipeline_cls([("scaler", scaler_cls()), ("model", estimator)])
    try:
        pipeline.fit(train_x, train_y)
        predictions = [float(value) for value in pipeline.predict(test_x)]
    except Exception as exc:  # noqa: BLE001 - keep Ridge/Dummy available if native XGBoost fails.
        LOGGER.warning("xgboost training failed: %s", exc)
        return None
    result = _candidate_model_result(
        "xgboost",
        pipeline,
        train_examples,
        test_examples,
        features,
        median_hold,
        test_y,
        predictions,
    )
    cv = _cross_validate_regressor(
        pipeline_cls([("scaler", scaler_cls()), ("model", estimator.__class__(**estimator.get_params()))]),
        train_examples,
        features,
    )
    result["cv_r2"] = cv.get("r2")
    result["cv_mae_seconds"] = cv.get("mae_seconds")
    result["cv_folds"] = cv.get("folds", 0)
    return result


def _candidate_model_result(
    model_type: str,
    estimator: Any,
    train_examples: list[dict[str, Any]],
    test_examples: list[dict[str, Any]],
    features: list[str],
    default_hold: float,
    test_y: list[float],
    predictions: list[float],
) -> dict[str, Any]:
    return {
        "type": model_type,
        "features": features,
        "default_hold_seconds": default_hold,
        "train_examples": len(train_examples),
        "test_examples": len(test_examples),
        "mae_seconds": _mean_absolute_error(test_y, predictions),
        "r2": _r2_score(test_y, predictions),
        "predictor": ExitModelPredictor(estimator, features, model_type, default_hold),
    }


def _cross_validate_regressor(estimator: Any, examples: list[dict[str, Any]], features: list[str]) -> dict[str, Any]:
    if len(examples) < XGBOOST_CV_FOLDS * 2:
        return {"folds": 0, "r2": None, "mae_seconds": None}
    ordered = sorted(examples, key=lambda example: (float(example.get("entry_ts") or 0), str(example.get("entry_hash") or "")))
    folds = min(XGBOOST_CV_FOLDS, len(ordered))
    r2_values: list[float] = []
    mae_values: list[float] = []
    for fold in range(folds):
        test = [example for index, example in enumerate(ordered) if index % folds == fold]
        train = [example for index, example in enumerate(ordered) if index % folds != fold]
        if len(train) < 2 or not test:
            continue
        estimator.fit(_feature_matrix(train, features), [float(example["hold_seconds"]) for example in train])
        predictions = [float(value) for value in estimator.predict(_feature_matrix(test, features))]
        y_true = [float(example["hold_seconds"]) for example in test]
        r2 = _r2_score(y_true, predictions)
        mae = _mean_absolute_error(y_true, predictions)
        if r2 is not None:
            r2_values.append(float(r2))
        if mae is not None:
            mae_values.append(float(mae))
    return {
        "folds": len(mae_values),
        "r2": round(sum(r2_values) / len(r2_values), 4) if r2_values else None,
        "mae_seconds": round(sum(mae_values) / len(mae_values), 4) if mae_values else None,
    }


def _feature_matrix(examples: list[dict[str, Any]], features: list[str]) -> list[list[float]]:
    return [[_feature_value(example, feature) for feature in features] for example in examples]


def _coerce_feature_matrix(rows: Any, features: list[str]) -> list[list[float]]:
    if hasattr(rows, "tolist"):
        raw = rows.tolist()
        return [[float(value or 0.0) for value in row] for row in raw]
    items = list(rows)
    if not items:
        return []
    first = items[0]
    if isinstance(first, dict):
        return _feature_matrix(items, features)
    return [[float(value or 0.0) for value in row] for row in items]


def _feature_value(example: dict[str, Any], feature: str) -> float:
    if feature == "market_type_code" and "market_type_code" not in example:
        return MARKET_TYPE_CODES.get(_market_type(str(example.get("market_type") or "")), 0.0)
    if feature == "market_volatility_60min" and "market_volatility_60min" not in example:
        return float(example.get("volatility_proxy") or 0.0)
    try:
        value = float(example.get(feature) or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(value):
        return 0.0
    return value


def _strip_predictor(model: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in model.items() if key != "predictor"}


def _score_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _train_test_split(
    examples: list[dict[str, Any]],
    *,
    train_ratio: float = 0.8,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(examples, key=lambda example: (float(example.get("entry_ts") or 0), str(example.get("entry_hash") or "")))
    if len(ordered) <= 1:
        return ordered, []
    winners = [example for example in ordered if float(example.get("pnl_proxy") or 0.0) > 0]
    losers = [example for example in ordered if float(example.get("pnl_proxy") or 0.0) <= 0]
    if winners and losers and min(len(winners), len(losers)) >= 2:
        train: list[dict[str, Any]] = []
        test: list[dict[str, Any]] = []
        for group in (winners, losers):
            split_index = int(len(group) * train_ratio)
            split_index = min(len(group) - 1, max(1, split_index))
            train.extend(group[:split_index])
            test.extend(group[split_index:])
        return (
            sorted(train, key=lambda example: (float(example.get("entry_ts") or 0), str(example.get("entry_hash") or ""))),
            sorted(test, key=lambda example: (float(example.get("entry_ts") or 0), str(example.get("entry_hash") or ""))),
        )
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
        "predictor": DummyExitRegressor(median_hold),
    }


def _model_stats(model: dict[str, Any]) -> dict[str, Any]:
    return {
        "model_type": model.get("type", "unknown"),
        "features": list(model.get("features") or []),
        "train_examples": int(model.get("train_examples") or 0),
        "test_examples": int(model.get("test_examples") or 0),
        "mae_seconds": model.get("mae_seconds"),
        "r2": model.get("r2"),
        "cv_mae_seconds": model.get("cv_mae_seconds"),
        "cv_r2": model.get("cv_r2"),
        "cv_folds": int(model.get("cv_folds") or 0),
        "fallback_reason": model.get("fallback_reason", ""),
        "rejected_models": model.get("rejected_models", []),
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
    import joblib  # type: ignore

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
    predictor = model.get("predictor")
    if predictor is not None and hasattr(predictor, "predict"):
        try:
            values = predictor.predict([example])
            return max(0.0, float(values[0])) if values else 0.0
        except Exception:
            pass
    if model.get("type") == "ridge":
        features = [str(feature) for feature in model.get("features", [])]
        coefs = [float(value) for value in model.get("coef", [])]
        prediction = float(model.get("intercept") or 0.0)
        for feature, coef in zip(features, coefs):
            prediction += coef * _feature_value(example, feature)
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
    parser.add_argument(
        "--test-ml",
        action="store_true",
        help="Run a limited ML training pass and report Ridge/XGBoost/Dummy selection metrics.",
    )
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
        test=bool(args.test or args.test_ml),
        limit=args.limit or (500 if args.test_ml else None),
        min_new_records=args.min_new_records,
        force=not args.no_force,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
