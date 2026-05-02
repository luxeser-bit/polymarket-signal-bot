from __future__ import annotations

"""FastAPI backend for PolySignal.

Run locally:

    python server/api.py

The server is intentionally a thin orchestration layer. Existing modules remain
the source of indexing, monitoring, scoring, cohorts, and paper trading logic;
this file starts those workers as subprocesses and exposes their SQLite-backed
state over HTTP/WebSocket for the React control room.
"""

import asyncio
import contextlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional until server deps are installed.
    load_dotenv = None  # type: ignore[assignment]

try:
    from fastapi import Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
except ModuleNotFoundError:  # pragma: no cover - surfaced by main().
    Body = None  # type: ignore[assignment]
    FastAPI = None  # type: ignore[assignment]
    HTTPException = RuntimeError  # type: ignore[assignment]
    WebSocket = Any  # type: ignore[misc,assignment]
    WebSocketDisconnect = Exception  # type: ignore[assignment]
    CORSMiddleware = None  # type: ignore[assignment]
    BaseModel = object  # type: ignore[assignment,misc]

try:
    from web3 import Web3
except ModuleNotFoundError:  # pragma: no cover - optional for indexer progress RPC.
    Web3 = None  # type: ignore[assignment]

from polymarket_signal_bot.cohorts import load_wallet_cohorts
from polymarket_signal_bot.consensus_engine import consensus_row_to_dict
from polymarket_signal_bot.api import ApiConfig, ApiError, PolymarketClient
from polymarket_signal_bot.live_paper_runner import _portfolio_snapshot
from polymarket_signal_bot.monitor import Monitor, MonitorConfig
from polymarket_signal_bot.scoring import calculate_all
from polymarket_signal_bot.storage import DEFAULT_DB_PATH, Store


TARGET_RAW_EVENTS = 86_000_000
COMPONENT_KEYS = ("indexer", "monitor", "live_paper")
SERVER_LOG_DIR = ROOT / "data" / "server_logs"
PROCESS_LOCK = threading.RLock()
PROCESS_REGISTRY: dict[str, "ManagedProcess"] = {}
ADOPTED_PROCESS_REGISTRY: dict[str, "AdoptedProcess"] = {}
COMPONENT_STATUS_CACHE: dict[str, Any] = {"components": None, "expires_at": 0.0}
COMPONENT_STATUS_CACHE_SECONDS = 5.0
TRAINING_LOCK = threading.RLock()
TRAINING_PROCESS: "ManagedTrainingProcess | None" = None
METRICS_LOCK = threading.RLock()
METRICS_CURSOR: dict[str, float] = {
    "last_block": 0.0,
    "seen_at": 0.0,
    "current_speed": 0.0,
    "speed_at": 0.0,
}
METRICS_SPEED_STALE_SECONDS = 30.0
INDEXER_COUNTER_CACHE: dict[str, Any] = {"snapshot": None, "expires_at": 0.0}
INDEXER_COUNTER_CACHE_SECONDS = 2.0
LIVE_PAYLOAD_CACHE: dict[str, Any] = {"payload": None, "expires_at": 0.0}
LIVE_PAYLOAD_LOCK = threading.RLock()
INDEXER_PROGRESS_LOCK = threading.RLock()
INDEXER_PROGRESS_SAMPLES: list[tuple[float, int]] = []
POLYGON_BLOCK_CACHE: dict[str, Any] = {"block": None, "expires_at": 0.0}
INDEXER_WATCHDOG_LOCK = threading.RLock()
INDEXER_WATCHDOG_STATE: dict[str, Any] = {
    "pid": 0,
    "raw_events": -1,
    "last_block": -1,
    "last_progress_at": 0.0,
    "last_check_at": 0.0,
    "last_restart_at": 0.0,
    "restart_count": 0,
    "last_action": "",
}


@dataclass(frozen=True)
class ServerSettings:
    main_db: Path = ROOT / DEFAULT_DB_PATH
    indexer_db: Path = ROOT / "data" / "indexer.db"
    paper_state_db: Path = ROOT / "data" / "paper_state.db"
    exit_examples_path: Path = ROOT / "data" / "exit_examples.json"
    exit_stats_path: Path = ROOT / "data" / "exit_stats.json"
    host: str = "127.0.0.1"
    port: int = 8000
    monitor_interval_seconds: int = 60
    paper_poll_interval_seconds: int = 60
    paper_price_interval_seconds: int = 15
    indexer_chunk_size: int = 100
    indexer_max_workers: int = 5
    indexer_rpc_rps: float = 25.0
    indexer_skip_contract_check: bool = True
    indexer_watchdog_enabled: bool = True
    indexer_stall_seconds: int = 300
    indexer_restart_cooldown_seconds: int = 300
    polygon_rpc_url: str = ""
    dry_run: bool = True
    scoring_fallback_max_rows: int = 50_000


@dataclass(frozen=True)
class ComponentSpec:
    key: str
    label: str
    command: tuple[str, ...]
    log_path: Path
    patterns: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)
    dry_run: bool = False


@dataclass
class ManagedProcess:
    process: subprocess.Popen[Any]
    started_at: float
    log_handle: Any
    dry_run: bool = False


@dataclass
class AdoptedProcess:
    pid: int
    started_at: float
    dry_run: bool = False


@dataclass
class ManagedTrainingProcess:
    process: subprocess.Popen[Any]
    started_at: float
    log_handle: Any
    test: bool = False
    limit: int | None = None


if isinstance(BaseModel, type) and BaseModel is not object:

    class PaperStartRequest(BaseModel):
        dry_run: bool | None = None

    class TrainingStartRequest(BaseModel):
        test: bool | None = None
        limit: int | None = None
        force: bool | None = None

else:

    class PaperStartRequest:  # pragma: no cover - used only without pydantic installed.
        def __init__(self, dry_run: bool | None = None) -> None:
            self.dry_run = dry_run

    class TrainingStartRequest:  # pragma: no cover - used only without pydantic installed.
        def __init__(self, test: bool | None = None, limit: int | None = None, force: bool | None = None) -> None:
            self.test = test
            self.limit = limit
            self.force = force


class MissingDependencyApp:
    """Importable placeholder when FastAPI is not installed."""

    def __call__(self, *_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError(_missing_dependency_message())


def create_app() -> Any:
    if FastAPI is None:
        return MissingDependencyApp()

    api = FastAPI(
        title="PolySignal Backend",
        version="0.1.0",
        description="Process control and real-time data API for polymarket-signal-bot.",
    )
    api.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @api.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "service": "polysignal-api", "time": int(time.time())}

    @api.post("/system/start")
    async def system_start() -> dict[str, Any]:
        settings = settings_from_env()
        results = start_all(settings)
        return {"ok": True, "components": results}

    @api.post("/system/stop")
    async def system_stop() -> dict[str, Any]:
        settings = settings_from_env()
        results = stop_all(settings, timeout_seconds=10.0)
        return {"ok": True, "components": results}

    @api.post("/system/{component_key}/start")
    async def system_component_start(component_key: str) -> dict[str, Any]:
        settings = settings_from_env()
        spec = component_spec(settings, component_key)
        result = start_component(spec)
        return {"ok": True, "component": result}

    @api.post("/system/{component_key}/stop")
    async def system_component_stop(component_key: str) -> dict[str, Any]:
        settings = settings_from_env()
        spec = component_spec(settings, component_key)
        result = stop_component(spec, timeout_seconds=10.0)
        return {"ok": True, "component": result}

    @api.get("/system/status")
    async def system_status() -> dict[str, Any]:
        settings = settings_from_env()
        components = await asyncio.to_thread(cached_component_statuses, settings)
        return {"components": components, "time": int(time.time())}

    @api.get("/api/metrics")
    async def api_metrics() -> dict[str, Any]:
        return await asyncio.to_thread(indexer_metrics, settings_from_env())

    @api.get("/api/indexer/progress")
    async def api_indexer_progress() -> dict[str, Any]:
        return await asyncio.to_thread(indexer_progress_snapshot, settings_from_env())

    @api.get("/api/wallets")
    async def api_wallets() -> dict[str, Any]:
        return await asyncio.to_thread(wallet_metrics, settings_from_env())

    @api.get("/api/consensus")
    async def api_consensus() -> dict[str, Any]:
        return await asyncio.to_thread(consensus_snapshot, settings_from_env())

    @api.post("/api/training/start")
    async def training_start(request: TrainingStartRequest | None = Body(default=None)) -> dict[str, Any]:
        settings = settings_from_env()
        result = start_training(
            settings,
            test=bool(request.test) if request and request.test is not None else False,
            limit=request.limit if request and request.limit else None,
            force=bool(request.force) if request and request.force is not None else True,
        )
        return {"ok": True, "training": result}

    @api.post("/api/training/stop")
    async def training_stop() -> dict[str, Any]:
        return {"ok": True, "training": stop_training(settings_from_env(), timeout_seconds=10.0)}

    @api.get("/api/training/status")
    async def training_status() -> dict[str, Any]:
        return await asyncio.to_thread(training_status_snapshot, settings_from_env())

    @api.get("/api/positions")
    async def api_positions() -> dict[str, Any]:
        return await asyncio.to_thread(positions_snapshot, settings_from_env())

    @api.get("/api/orderbook/{market_id}")
    async def api_orderbook(market_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(orderbook_snapshot, settings_from_env(), market_id)

    @api.get("/api/trade_log")
    async def api_trade_log(limit: int = 250, market: str = "", date_from: int = 0, date_to: int = 0) -> dict[str, Any]:
        return await asyncio.to_thread(
            trade_log_snapshot,
            settings_from_env(),
            limit=limit,
            market=market,
            date_from=date_from,
            date_to=date_to,
        )

    @api.get("/api/equity")
    async def api_equity(timeframe_seconds: int = 300, max_points: int = 360) -> dict[str, Any]:
        return await asyncio.to_thread(
            equity_snapshot,
            settings_from_env(),
            timeframe_seconds=timeframe_seconds,
            max_points=max_points,
        )

    @api.post("/api/paper/start")
    async def paper_start(request: PaperStartRequest | None = Body(default=None)) -> dict[str, Any]:
        settings = settings_from_env()
        dry_run = settings.dry_run if request is None or request.dry_run is None else bool(request.dry_run)
        spec = component_specs(settings, paper_dry_run=dry_run)["live_paper"]
        result = start_component(spec)
        return {"ok": True, "component": result}

    @api.post("/api/paper/stop")
    async def paper_stop() -> dict[str, Any]:
        settings = settings_from_env()
        spec = component_specs(settings)["live_paper"]
        return {"ok": True, "component": stop_component(spec, timeout_seconds=10.0)}

    @api.get("/api/paper/status")
    async def paper_status() -> dict[str, Any]:
        return await asyncio.to_thread(paper_status_snapshot, settings_from_env())

    @api.websocket("/ws/live")
    async def ws_live(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                settings = settings_from_env()
                try:
                    payload = await asyncio.wait_for(
                        asyncio.to_thread(live_payload, settings),
                        timeout=2.5,
                    )
                except TimeoutError:
                    payload = {"error": "live payload timeout", "time": int(time.time())}
                await websocket.send_json(payload)
                await asyncio.sleep(1.5)
        except WebSocketDisconnect:
            return
        except Exception as exc:  # noqa: BLE001 - keep the server process alive.
            with contextlib.suppress(Exception):
                await websocket.send_json({"error": str(exc), "time": int(time.time())})

    return api


def settings_from_env() -> ServerSettings:
    load_dotenv_file()
    monitor_defaults = MonitorConfig()
    main_db = _env_path("POLYSIGNAL_DB", ROOT / DEFAULT_DB_PATH)
    return ServerSettings(
        main_db=main_db,
        indexer_db=_env_path("INDEXER_DB_PATH", ROOT / "data" / "indexer.db"),
        paper_state_db=_env_path("POLYSIGNAL_PAPER_STATE_DB", _env_path("PAPER_STATE_DB", ROOT / "data" / "paper_state.db")),
        exit_examples_path=_env_path("EXIT_EXAMPLES_PATH", ROOT / "data" / "exit_examples.json"),
        exit_stats_path=_env_path("EXIT_STATS_PATH", ROOT / "data" / "exit_stats.json"),
        host=os.environ.get("API_HOST", os.environ.get("SERVER_HOST", "127.0.0.1")),
        port=_env_int("API_PORT", _env_int("SERVER_PORT", 8000)),
        monitor_interval_seconds=_env_int("MONITOR_INTERVAL_SECONDS", monitor_defaults.interval_seconds),
        paper_poll_interval_seconds=_env_int("POLYSIGNAL_POLL_INTERVAL", 60),
        paper_price_interval_seconds=_env_int("POLYSIGNAL_PRICE_INTERVAL", 15),
        indexer_chunk_size=_env_int("INDEXER_CHUNK_SIZE", _env_int("CHUNK_SIZE", 100)),
        indexer_max_workers=_env_int("INDEXER_MAX_WORKERS", _env_int("MAX_WORKERS", 5)),
        indexer_rpc_rps=_env_float("INDEXER_RPC_RPS", _env_float("RPC_RPS", 25.0)),
        indexer_skip_contract_check=_env_bool("INDEXER_SKIP_CONTRACT_CHECK", True),
        indexer_watchdog_enabled=_env_bool("INDEXER_WATCHDOG_ENABLED", True),
        indexer_stall_seconds=_env_int("INDEXER_STALL_SECONDS", 300),
        indexer_restart_cooldown_seconds=_env_int("INDEXER_RESTART_COOLDOWN_SECONDS", 300),
        polygon_rpc_url=os.environ.get("POLYGON_RPC_URL", ""),
        dry_run=_env_bool("DRY_RUN", True),
        scoring_fallback_max_rows=_env_int("API_SCORING_FALLBACK_MAX_ROWS", 50_000),
    )


def load_dotenv_file() -> None:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env", override=False)


def component_specs(settings: ServerSettings, *, paper_dry_run: bool | None = None) -> dict[str, ComponentSpec]:
    SERVER_LOG_DIR.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("POLYSIGNAL_DB", str(settings.main_db))
    env.setdefault("INDEXER_DB_PATH", str(settings.indexer_db))
    env.setdefault("POLYSIGNAL_PAPER_STATE_DB", str(settings.paper_state_db))
    dry_run = settings.dry_run if paper_dry_run is None else bool(paper_dry_run)

    paper_command = [
        sys.executable,
        "-m",
        "polymarket_signal_bot.live_paper_runner",
        "--db",
        str(settings.main_db),
        "--state-db",
        str(settings.paper_state_db),
        "--log-path",
        str(SERVER_LOG_DIR / "live_paper.log"),
        "--poll-interval",
        str(settings.paper_poll_interval_seconds),
        "--price-interval",
        str(settings.paper_price_interval_seconds),
    ]
    if dry_run:
        paper_command.append("--dry-run")

    indexer_command: list[str] = [
        sys.executable,
        "-m",
        "src.indexer",
        "--sync",
        "--db",
        str(settings.indexer_db),
        "--chunk-size",
        str(settings.indexer_chunk_size),
        "--max-workers",
        str(settings.indexer_max_workers),
        "--rpc-rps",
        str(settings.indexer_rpc_rps),
    ]
    if settings.indexer_skip_contract_check:
        indexer_command.append("--skip-contract-check")

    return {
        "indexer": ComponentSpec(
            key="indexer",
            label="Indexer",
            command=tuple(indexer_command),
            log_path=SERVER_LOG_DIR / "indexer.log",
            patterns=("*src.indexer*", "*polymarket_signal_bot.indexer*"),
            env=env,
        ),
        "monitor": ComponentSpec(
            key="monitor",
            label="Monitor",
            command=(
                sys.executable,
                "-m",
                "polymarket_signal_bot",
                "--db",
                str(settings.main_db),
                "monitor",
                "--interval-seconds",
                str(settings.monitor_interval_seconds),
                "--training-db",
                str(settings.indexer_db),
            ),
            log_path=SERVER_LOG_DIR / "monitor.log",
            patterns=("*polymarket_signal_bot*monitor*", "*src.monitor*"),
            env=env,
        ),
        "live_paper": ComponentSpec(
            key="live_paper",
            label="Live Paper",
            command=tuple(paper_command),
            log_path=SERVER_LOG_DIR / "live_paper.log",
            patterns=("*polymarket_signal_bot.live_paper_runner*", "*polymarket_signal_bot*live-paper*"),
            env=env,
            dry_run=dry_run,
        ),
    }


def start_all(settings: ServerSettings) -> dict[str, dict[str, Any]]:
    return {key: start_component(spec) for key, spec in component_specs(settings).items()}


def stop_all(settings: ServerSettings, *, timeout_seconds: float = 10.0) -> dict[str, dict[str, Any]]:
    specs = component_specs(settings)
    return {key: stop_component(specs[key], timeout_seconds=timeout_seconds) for key in reversed(COMPONENT_KEYS)}


def component_spec(settings: ServerSettings, key: str) -> ComponentSpec:
    normalized = key.strip().lower().replace("-", "_")
    aliases = {
        "paper": "live_paper",
        "live": "live_paper",
        "livepaper": "live_paper",
        "live_paper_runner": "live_paper",
    }
    normalized = aliases.get(normalized, normalized)
    specs = component_specs(settings)
    if normalized not in specs:
        valid = ", ".join(specs)
        raise HTTPException(status_code=404, detail=f"Unknown component '{key}'. Valid components: {valid}")
    return specs[normalized]


def start_training(
    settings: ServerSettings,
    *,
    test: bool = False,
    limit: int | None = None,
    force: bool = True,
) -> dict[str, Any]:
    global TRAINING_PROCESS
    with TRAINING_LOCK:
        current = TRAINING_PROCESS
        if current and current.process.poll() is None:
            return {
                **training_process_status(current),
                "started": False,
                "message": "already running",
                "last_run": latest_training_summary(settings.indexer_db),
            }
        cleanup_training_process()
        SERVER_LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = SERVER_LOG_DIR / "auto_trainer.log"
        log_handle = log_path.open("ab")
        command = training_command(settings, test=test, limit=limit, force=force)
        env = os.environ.copy()
        env.setdefault("INDEXER_DB_PATH", str(settings.indexer_db))
        env.setdefault("POLYSIGNAL_DB", str(settings.main_db))
        creationflags = 0
        startupinfo = None
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        process = subprocess.Popen(
            list(command),
            cwd=str(ROOT),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
            startupinfo=startupinfo,
        )
        TRAINING_PROCESS = ManagedTrainingProcess(
            process=process,
            started_at=time.time(),
            log_handle=log_handle,
            test=test,
            limit=limit,
        )
        return {
            **training_process_status(TRAINING_PROCESS),
            "started": True,
            "command": list(command),
            "log_path": str(log_path),
            "last_run": latest_training_summary(settings.indexer_db),
        }


def stop_training(settings: ServerSettings, *, timeout_seconds: float = 10.0) -> dict[str, Any]:
    global TRAINING_PROCESS
    with TRAINING_LOCK:
        current = TRAINING_PROCESS
        if current is None or current.process.poll() is not None:
            cleanup_training_process()
            return {
                "running": False,
                "status": "stopped",
                "pid": 0,
                "stopped": False,
                "message": "not running",
                "last_run": latest_training_summary(settings.indexer_db),
                "log_tail": tail_file(SERVER_LOG_DIR / "auto_trainer.log"),
            }
        pid = int(current.process.pid)
        terminated = terminate_pid(pid, timeout_seconds=timeout_seconds, process=current.process)
        cleanup_training_process()
        return {
            "running": not terminated,
            "status": "running" if not terminated else "stopped",
            "pid": pid if not terminated else 0,
            "stopped": terminated,
            "message": "stopped" if terminated else "kill failed",
            "last_run": latest_training_summary(settings.indexer_db),
            "log_tail": tail_file(SERVER_LOG_DIR / "auto_trainer.log"),
        }


def training_command(
    settings: ServerSettings,
    *,
    test: bool = False,
    limit: int | None = None,
    force: bool = True,
) -> tuple[str, ...]:
    command = [
        sys.executable,
        "-m",
        "polymarket_signal_bot.auto_trainer",
        "--db",
        str(settings.indexer_db),
        "--model-path",
        str(ROOT / "data" / "exit_model.pkl"),
        "--stats-path",
        str(settings.exit_stats_path),
        "--examples-path",
        str(settings.exit_examples_path),
        "--policy-path",
        str(ROOT / "data" / "best_policy.json"),
    ]
    if test:
        command.append("--test")
    if limit:
        command.extend(["--limit", str(int(limit))])
    if not force:
        command.append("--no-force")
    return tuple(command)


def training_status_snapshot(settings: ServerSettings) -> dict[str, Any]:
    with TRAINING_LOCK:
        cleanup_training_process()
        current = TRAINING_PROCESS
        status = training_process_status(current) if current else {
            "running": False,
            "status": "stopped",
            "pid": 0,
            "uptime_seconds": 0,
            "test": False,
            "limit": None,
        }
    log_path = SERVER_LOG_DIR / "auto_trainer.log"
    return {
        **status,
        "last_run": latest_training_summary(settings.indexer_db),
        "exit_examples": exit_examples_snapshot(settings.exit_examples_path),
        "log_path": str(log_path),
        "log_tail": tail_file(log_path),
    }


def training_process_status(process: ManagedTrainingProcess | None) -> dict[str, Any]:
    if process is None or process.process.poll() is not None:
        return {
            "running": False,
            "status": "stopped",
            "pid": 0,
            "uptime_seconds": 0,
            "test": bool(process.test) if process else False,
            "limit": process.limit if process else None,
        }
    return {
        "running": True,
        "status": "running",
        "pid": int(process.process.pid),
        "uptime_seconds": int(time.time() - process.started_at),
        "test": bool(process.test),
        "limit": process.limit,
    }


def cleanup_training_process() -> None:
    global TRAINING_PROCESS
    if TRAINING_PROCESS is None:
        return
    if TRAINING_PROCESS.process.poll() is None:
        return
    with contextlib.suppress(Exception):
        TRAINING_PROCESS.log_handle.close()
    TRAINING_PROCESS = None


def start_component(spec: ComponentSpec) -> dict[str, Any]:
    with PROCESS_LOCK:
        running = component_status(spec, discover=True)
        if running["running"]:
            return {**running, "started": False, "message": "already running"}

        spec.log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = spec.log_path.open("wb")
        creationflags = 0
        startupinfo = None
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        process = subprocess.Popen(
            list(spec.command),
            cwd=str(ROOT),
            env=spec.env or None,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
            startupinfo=startupinfo,
        )
        PROCESS_REGISTRY[spec.key] = ManagedProcess(
            process=process,
            started_at=time.time(),
            log_handle=log_handle,
            dry_run=spec.dry_run,
        )
        if spec.key == "indexer":
            with INDEXER_WATCHDOG_LOCK:
                INDEXER_WATCHDOG_STATE.update(
                    {
                        "pid": int(process.pid),
                        "raw_events": -1,
                        "last_block": -1,
                        "last_progress_at": time.time(),
                        "last_check_at": time.time(),
                        "last_action": "started",
                    }
                )
        clear_component_status_cache()
        return {
            "name": spec.label,
            "running": True,
            "pid": process.pid,
            "started": True,
            "uptime_seconds": 0,
            "dry_run": spec.dry_run,
            "log_tail": tail_file(spec.log_path),
        }


def restart_component(spec: ComponentSpec, *, timeout_seconds: float = 10.0) -> dict[str, Any]:
    stopped = stop_component(spec, timeout_seconds=timeout_seconds, manual=False)
    time.sleep(1.0)
    started = start_component(spec)
    return {"stopped": stopped, "started": started}


def stop_component(
    spec: ComponentSpec,
    *,
    timeout_seconds: float = 10.0,
    manual: bool = True,
) -> dict[str, Any]:
    with PROCESS_LOCK:
        managed = PROCESS_REGISTRY.get(spec.key)
        adopted = ADOPTED_PROCESS_REGISTRY.get(spec.key)
        pid = (
            managed.process.pid
            if managed and managed.process.poll() is None
            else adopted.pid
            if adopted and pid_running(adopted.pid)
            else discover_pid(spec.patterns)
        )
        if not pid:
            cleanup_component(spec.key)
            if manual and spec.key == "indexer":
                reset_indexer_watchdog("manual_stop")
            return {
                "name": spec.label,
                "running": False,
                "pid": 0,
                "stopped": False,
                "message": "not running",
                "log_tail": tail_file(spec.log_path),
            }

        terminated = terminate_pid(pid, timeout_seconds=timeout_seconds, process=managed.process if managed else None)
        cleanup_component(spec.key)
        ADOPTED_PROCESS_REGISTRY.pop(spec.key, None)
        if manual and spec.key == "indexer":
            reset_indexer_watchdog("manual_stop")
        clear_component_status_cache()
        return {
            "name": spec.label,
            "running": not terminated,
            "pid": pid if not terminated else 0,
            "stopped": terminated,
            "message": "stopped" if terminated else "kill failed",
            "log_tail": tail_file(spec.log_path),
        }


def component_statuses(settings: ServerSettings) -> dict[str, dict[str, Any]]:
    specs = component_specs(settings)
    components = {key: component_status(spec, discover=True) for key, spec in specs.items()}
    components["indexer"] = indexer_watchdog_status(settings, specs["indexer"], components["indexer"])
    return components


def cached_component_statuses(settings: ServerSettings) -> dict[str, dict[str, Any]]:
    now = time.time()
    with PROCESS_LOCK:
        cached = COMPONENT_STATUS_CACHE.get("components")
        if cached is not None and float(COMPONENT_STATUS_CACHE.get("expires_at") or 0) > now:
            return dict(cached)
    components = component_statuses(settings)
    with PROCESS_LOCK:
        COMPONENT_STATUS_CACHE["components"] = dict(components)
        COMPONENT_STATUS_CACHE["expires_at"] = now + COMPONENT_STATUS_CACHE_SECONDS
    return components


def clear_component_status_cache() -> None:
    with PROCESS_LOCK:
        COMPONENT_STATUS_CACHE["components"] = None
        COMPONENT_STATUS_CACHE["expires_at"] = 0.0


def reset_indexer_watchdog(reason: str) -> None:
    with INDEXER_WATCHDOG_LOCK:
        INDEXER_WATCHDOG_STATE.update(
            {
                "pid": 0,
                "raw_events": -1,
                "last_block": -1,
                "last_progress_at": 0.0,
                "last_check_at": time.time(),
                "last_action": reason,
            }
        )


def component_status(spec: ComponentSpec, *, discover: bool = False) -> dict[str, Any]:
    with PROCESS_LOCK:
        managed = PROCESS_REGISTRY.get(spec.key)
        if managed and managed.process.poll() is None:
            pid = int(managed.process.pid)
            started_at = managed.started_at
            dry_run = managed.dry_run
        else:
            cleanup_component(spec.key)
            adopted = ADOPTED_PROCESS_REGISTRY.get(spec.key)
            if adopted and pid_running(adopted.pid):
                pid = adopted.pid
                started_at = adopted.started_at
                dry_run = adopted.dry_run
            else:
                ADOPTED_PROCESS_REGISTRY.pop(spec.key, None)
                pid = discover_pid(spec.patterns) if discover else 0
                started_at = time.time() if pid else 0.0
                dry_run = spec.dry_run
                if pid:
                    ADOPTED_PROCESS_REGISTRY[spec.key] = AdoptedProcess(
                        pid=int(pid),
                        started_at=started_at,
                        dry_run=spec.dry_run,
                    )
        running = bool(pid and pid_running(pid))
        return {
            "name": spec.label,
            "running": running,
            "status": "running" if running else "stopped",
            "pid": int(pid or 0),
            "uptime_seconds": int(time.time() - started_at) if running and started_at else 0,
            "dry_run": bool(dry_run) if spec.key == "live_paper" else None,
            "command": list(spec.command),
            "log_path": str(spec.log_path),
            "log_tail": tail_file(spec.log_path),
        }


def indexer_watchdog_status(
    settings: ServerSettings,
    spec: ComponentSpec,
    status: dict[str, Any],
) -> dict[str, Any]:
    status = dict(status)
    status.setdefault("health", status.get("status", "stopped"))
    status["watchdog_enabled"] = bool(settings.indexer_watchdog_enabled)
    if not settings.indexer_watchdog_enabled:
        return status

    now = time.time()
    pid = int(status.get("pid") or 0)
    if not status.get("running"):
        should_restart = False
        restart_count = 0
        last_restart_at = 0.0
        with INDEXER_WATCHDOG_LOCK:
            state_pid = int(INDEXER_WATCHDOG_STATE.get("pid") or 0)
            last_action = str(INDEXER_WATCHDOG_STATE.get("last_action") or "")
            restart_count = int(INDEXER_WATCHDOG_STATE.get("restart_count") or 0)
            last_restart_at = float(INDEXER_WATCHDOG_STATE.get("last_restart_at") or 0.0)
            cooldown_ready = now - last_restart_at >= max(1, settings.indexer_restart_cooldown_seconds)
            should_restart = state_pid > 0 and last_action != "manual_stop" and cooldown_ready
            INDEXER_WATCHDOG_STATE["last_check_at"] = now
            if should_restart:
                INDEXER_WATCHDOG_STATE["last_restart_at"] = now
                INDEXER_WATCHDOG_STATE["restart_count"] = restart_count + 1
                INDEXER_WATCHDOG_STATE["last_action"] = "restart_missing_requested"

        if not should_restart:
            status.update({"health": "stopped", "stalled": False, "stalled_seconds": 0})
            return status

        restart_result = restart_component(spec, timeout_seconds=10.0)
        restarted_status = component_status(spec, discover=True)
        with INDEXER_WATCHDOG_LOCK:
            INDEXER_WATCHDOG_STATE.update(
                {
                    "pid": int(restarted_status.get("pid") or 0),
                    "raw_events": -1,
                    "last_block": -1,
                    "last_progress_at": time.time(),
                    "last_check_at": time.time(),
                    "last_action": "restarted_missing",
                }
            )
        clear_component_status_cache()
        restarted_status.update(
            {
                "health": "restarted" if restarted_status.get("running") else "stopped",
                "stalled": False,
                "stalled_seconds": 0,
                "last_progress_at": int(time.time()),
                "watchdog_enabled": True,
                "watchdog_action": "restarted_missing",
                "watchdog_restart_count": restart_count + 1,
                "watchdog_last_restart_at": int(time.time()),
                "restart": restart_result,
            }
        )
        return restarted_status

    counters = indexer_counter_snapshot(settings.indexer_db)
    raw_events = int(counters.get("raw_events") or 0)
    last_block = int(counters.get("last_block") or 0)
    storage_mtime = indexer_storage_mtime(settings.indexer_db)

    should_restart = False
    restart_count = 0
    last_restart_at = 0.0
    with INDEXER_WATCHDOG_LOCK:
        state_pid = int(INDEXER_WATCHDOG_STATE.get("pid") or 0)
        previous_raw = int(INDEXER_WATCHDOG_STATE.get("raw_events") or -1)
        previous_block = int(INDEXER_WATCHDOG_STATE.get("last_block") or -1)
        pid_changed = state_pid != pid
        progressed = raw_events > previous_raw or last_block > previous_block

        if pid_changed or previous_raw < 0 or previous_block < 0 or progressed:
            INDEXER_WATCHDOG_STATE.update(
                {
                    "pid": pid,
                    "raw_events": raw_events,
                    "last_block": last_block,
                    "last_progress_at": now,
                    "last_check_at": now,
                    "last_action": "progress" if progressed else "adopted",
                }
            )
        else:
            if storage_mtime > float(INDEXER_WATCHDOG_STATE.get("last_progress_at") or 0.0):
                INDEXER_WATCHDOG_STATE["last_progress_at"] = storage_mtime
            INDEXER_WATCHDOG_STATE["last_check_at"] = now

        last_progress_at = float(INDEXER_WATCHDOG_STATE.get("last_progress_at") or now)
        stalled_seconds = max(0, int(now - last_progress_at))
        restart_count = int(INDEXER_WATCHDOG_STATE.get("restart_count") or 0)
        last_restart_at = float(INDEXER_WATCHDOG_STATE.get("last_restart_at") or 0.0)
        cooldown_ready = now - last_restart_at >= max(1, settings.indexer_restart_cooldown_seconds)
        should_restart = stalled_seconds >= max(1, settings.indexer_stall_seconds) and cooldown_ready
        if should_restart:
            INDEXER_WATCHDOG_STATE["last_restart_at"] = now
            INDEXER_WATCHDOG_STATE["restart_count"] = restart_count + 1
            INDEXER_WATCHDOG_STATE["last_action"] = "restart_requested"

    status.update(
        {
            "health": "stalled" if stalled_seconds >= max(1, settings.indexer_stall_seconds) else "running",
            "stalled": stalled_seconds >= max(1, settings.indexer_stall_seconds),
            "stalled_seconds": stalled_seconds,
            "last_progress_at": int(last_progress_at),
            "watchdog_restart_count": restart_count,
            "watchdog_last_restart_at": int(last_restart_at) if last_restart_at else 0,
        }
    )

    if not should_restart:
        return status

    restart_result = restart_component(spec, timeout_seconds=10.0)
    restarted_status = component_status(spec, discover=True)
    with INDEXER_WATCHDOG_LOCK:
        INDEXER_WATCHDOG_STATE.update(
            {
                "pid": int(restarted_status.get("pid") or 0),
                "raw_events": raw_events,
                "last_block": last_block,
                "last_progress_at": time.time(),
                "last_check_at": time.time(),
                "last_action": "restarted",
            }
        )
    clear_component_status_cache()
    restarted_status.update(
        {
            "health": "restarted" if restarted_status.get("running") else "stalled",
            "stalled": False,
            "stalled_seconds": 0,
            "last_progress_at": int(time.time()),
            "watchdog_enabled": True,
            "watchdog_action": "restarted",
            "watchdog_restart_count": restart_count + 1,
            "watchdog_last_restart_at": int(time.time()),
            "restart": restart_result,
        }
    )
    return restarted_status


def indexer_counter_snapshot(db_path: Path) -> dict[str, int]:
    result = {"raw_events": 0, "last_block": 0, "updated_at": 0}
    if not db_path.exists():
        return result
    now = time.time()
    with METRICS_LOCK:
        cached = INDEXER_COUNTER_CACHE.get("snapshot")
        if cached is not None and float(INDEXER_COUNTER_CACHE.get("expires_at") or 0) > now:
            return dict(cached)
    try:
        with sqlite_connect(db_path) as conn:
            result["raw_events"] = raw_events_count(conn)
            if table_exists(conn, "indexer_state"):
                row = conn.execute(
                    "SELECT last_block, updated_at FROM indexer_state ORDER BY updated_at DESC LIMIT 1"
                ).fetchone()
                if row:
                    result["last_block"] = int(row["last_block"] or 0)
                    result["updated_at"] = int(row["updated_at"] or 0)
    except Exception:
        return result
    with METRICS_LOCK:
        INDEXER_COUNTER_CACHE["snapshot"] = dict(result)
        INDEXER_COUNTER_CACHE["expires_at"] = now + INDEXER_COUNTER_CACHE_SECONDS
    return result


def indexer_storage_mtime(db_path: Path) -> float:
    paths = [db_path, db_path.with_name(f"{db_path.name}-wal"), db_path.with_name(f"{db_path.name}-shm")]
    mtimes: list[float] = []
    for path in paths:
        with contextlib.suppress(OSError):
            mtimes.append(path.stat().st_mtime)
    return max(mtimes) if mtimes else 0.0


def cleanup_component(key: str) -> None:
    managed = PROCESS_REGISTRY.get(key)
    if managed:
        if managed.process.poll() is None:
            return
        with contextlib.suppress(Exception):
            managed.log_handle.close()
        PROCESS_REGISTRY.pop(key, None)
    adopted = ADOPTED_PROCESS_REGISTRY.get(key)
    if adopted and not pid_running(adopted.pid):
        ADOPTED_PROCESS_REGISTRY.pop(key, None)


def terminate_pid(pid: int, *, timeout_seconds: float, process: subprocess.Popen[Any] | None = None) -> bool:
    if pid <= 0:
        return True
    if process is not None and process.poll() is None:
        with contextlib.suppress(Exception):
            process.terminate()
        try:
            process.wait(timeout=timeout_seconds)
            return True
        except subprocess.TimeoutExpired:
            with contextlib.suppress(Exception):
                process.kill()
            try:
                process.wait(timeout=3)
                return True
            except subprocess.TimeoutExpired:
                return False

    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T"], capture_output=True, text=True, timeout=5, check=False)
        if wait_until_stopped(pid, timeout_seconds):
            return True
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, text=True, timeout=5, check=False)
        return wait_until_stopped(pid, 3.0)

    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)
    if wait_until_stopped(pid, timeout_seconds):
        return True
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)
    return wait_until_stopped(pid, 3.0)


def wait_until_stopped(pid: int, timeout_seconds: float) -> bool:
    deadline = time.time() + max(0.1, timeout_seconds)
    while time.time() < deadline:
        if not pid_running(pid):
            return True
        time.sleep(0.1)
    return not pid_running(pid)


def pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command", f"Get-Process -Id {int(pid)} -ErrorAction SilentlyContinue"],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            return result.returncode == 0 and bool(result.stdout.strip())
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def discover_pid(patterns: tuple[str, ...]) -> int:
    if not patterns:
        return 0
    try:
        if os.name == "nt":
            clauses = " -or ".join(f"$_.CommandLine -like '{pattern}'" for pattern in patterns)
            command = (
                "Get-CimInstance Win32_Process | "
                f"Where-Object {{ $_.Name -like 'python*' -and ({clauses}) }} | "
                "Select-Object -First 1 -ExpandProperty ProcessId"
            )
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command", command],
                capture_output=True,
                text=True,
                timeout=3,
                check=False,
            )
            return int(result.stdout.strip().splitlines()[0]) if result.stdout.strip() else 0
        pattern = "|".join(pattern.strip("*") for pattern in patterns)
        result = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, timeout=2, check=False)
        return int(result.stdout.strip().splitlines()[0]) if result.stdout.strip() else 0
    except Exception:
        return 0


def indexer_metrics(settings: ServerSettings, *, include_process: bool = True) -> dict[str, Any]:
    indexer_status = (
        cached_component_statuses(settings).get("indexer", {})
        if include_process
        else {"running": False, "health": "unknown", "stalled": False}
    )
    running = bool(indexer_status.get("running"))
    stalled = bool(indexer_status.get("stalled"))
    snapshot = {
        "raw_events": 0,
        "last_block": 0,
        "blocks_per_second": 0.0,
        "progress": 0.0,
        "target": TARGET_RAW_EVENTS,
        "updated_at": 0,
        "db_path": str(settings.indexer_db),
        "running": running,
        "health": str(indexer_status.get("health") or ("running" if running else "stopped")),
        "stalled": stalled,
        "stalled_seconds": int(indexer_status.get("stalled_seconds") or 0),
        "error": "",
    }
    if not settings.indexer_db.exists():
        snapshot["error"] = "indexer db not found"
        return snapshot
    try:
        counters = indexer_counter_snapshot(settings.indexer_db)
        snapshot["raw_events"] = int(counters.get("raw_events") or 0)
        snapshot["last_block"] = int(counters.get("last_block") or 0)
        snapshot["updated_at"] = int(counters.get("updated_at") or 0)
        snapshot["progress"] = min(1.0, float(snapshot["raw_events"]) / TARGET_RAW_EVENTS)
        snapshot["blocks_per_second"] = block_speed(float(snapshot["last_block"])) if running and not stalled else 0.0
        return snapshot
    except Exception as exc:  # noqa: BLE001 - API should stay up during SQLite writes.
        snapshot["error"] = str(exc)
        return snapshot


def indexer_progress_snapshot(settings: ServerSettings) -> dict[str, Any]:
    indexer_status = cached_component_statuses(settings).get("indexer", {})
    running = bool(indexer_status.get("running"))
    stalled = bool(indexer_status.get("stalled"))
    metrics = indexer_metrics(settings, include_process=False)
    last_block = int(metrics.get("last_block") or 0)
    speed = 0.0
    if running and not stalled:
        speed = indexer_average_speed(last_block)
        if speed <= 0:
            speed = float(metrics.get("blocks_per_second") or 0.0)
    else:
        with INDEXER_PROGRESS_LOCK:
            INDEXER_PROGRESS_SAMPLES.clear()
    current_block = polygon_current_block(settings)
    eta_seconds: int | None = None
    if current_block is not None and last_block > 0 and current_block > last_block and speed > 0:
        eta_seconds = int((int(current_block) - last_block) / speed)
    return {
        "last_block": last_block,
        "last_block_date": last_block_date(settings.indexer_db, last_block),
        "current_block_polygon": current_block,
        "estimated_completion_seconds": eta_seconds,
        "speed_blocks_per_second": round(float(speed), 4),
        "running": running,
        "health": str(indexer_status.get("health") or ("running" if running else "stopped")),
        "stalled": stalled,
        "stalled_seconds": int(indexer_status.get("stalled_seconds") or 0),
        "pid": int(indexer_status.get("pid") or 0),
        "updated_at": int(time.time()),
        "error": metrics.get("error", ""),
    }


def last_block_date(db_path: Path, last_block: int) -> str | None:
    if last_block <= 0 or not db_path.exists():
        return None
    try:
        with sqlite_connect(db_path) as conn:
            timestamp: int | None = None
            if table_exists(conn, "raw_transactions"):
                row = conn.execute(
                    """
                    SELECT MAX(timestamp) AS timestamp
                    FROM raw_transactions
                    WHERE block_number = ?
                    """,
                    (last_block,),
                ).fetchone()
                timestamp = int(row["timestamp"] or 0) if row and row["timestamp"] is not None else None
            if not timestamp and table_exists(conn, "indexer_blocks"):
                row = conn.execute(
                    "SELECT timestamp FROM indexer_blocks WHERE block_number = ? LIMIT 1",
                    (last_block,),
                ).fetchone()
                timestamp = int(row["timestamp"] or 0) if row and row["timestamp"] is not None else None
            if not timestamp and table_exists(conn, "indexer_blocks"):
                row = conn.execute(
                    """
                    SELECT timestamp
                    FROM indexer_blocks
                    WHERE block_number <= ?
                    ORDER BY block_number DESC
                    LIMIT 1
                    """,
                    (last_block,),
                ).fetchone()
                timestamp = int(row["timestamp"] or 0) if row and row["timestamp"] is not None else None
            if not timestamp and table_exists(conn, "raw_transactions"):
                row = conn.execute(
                    """
                    SELECT timestamp
                    FROM raw_transactions
                    WHERE block_number <= ?
                    ORDER BY block_number DESC
                    LIMIT 1
                    """,
                    (last_block,),
                ).fetchone()
                timestamp = int(row["timestamp"] or 0) if row and row["timestamp"] is not None else None
        if not timestamp:
            return None
        return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def polygon_current_block(settings: ServerSettings) -> int | None:
    if Web3 is None or not settings.polygon_rpc_url:
        return None
    now = time.time()
    with INDEXER_PROGRESS_LOCK:
        cached = POLYGON_BLOCK_CACHE.get("block")
        if cached is not None and float(POLYGON_BLOCK_CACHE.get("expires_at") or 0) > now:
            return int(cached)
    try:
        provider = Web3.HTTPProvider(settings.polygon_rpc_url, request_kwargs={"timeout": 5})
        current = int(Web3(provider).eth.block_number)
    except Exception:
        return None
    with INDEXER_PROGRESS_LOCK:
        POLYGON_BLOCK_CACHE["block"] = current
        POLYGON_BLOCK_CACHE["expires_at"] = now + 15.0
    return current


def indexer_average_speed(last_block: int) -> float:
    now = time.time()
    if last_block <= 0:
        return 0.0
    with INDEXER_PROGRESS_LOCK:
        INDEXER_PROGRESS_SAMPLES.append((now, int(last_block)))
        cutoff = now - 3600.0
        while INDEXER_PROGRESS_SAMPLES and (
            INDEXER_PROGRESS_SAMPLES[0][0] < cutoff or len(INDEXER_PROGRESS_SAMPLES) > 720
        ):
            INDEXER_PROGRESS_SAMPLES.pop(0)
        if len(INDEXER_PROGRESS_SAMPLES) < 2:
            return 0.0
        first_time, first_block = INDEXER_PROGRESS_SAMPLES[0]
        last_time, latest_block = INDEXER_PROGRESS_SAMPLES[-1]
    elapsed = max(0.001, last_time - first_time)
    return max(0.0, float(latest_block - first_block) / elapsed)


def wallet_metrics(settings: ServerSettings) -> dict[str, Any]:
    model_metrics = exit_stats_snapshot(settings.exit_stats_path)
    result: dict[str, Any] = {
        "counts": {},
        "top_wallets": [],
        "scored_wallet_rows": [],
        "cohort_wallets": {},
        "scored_wallets": 0,
        "db_path": str(settings.indexer_db),
        "last_training": latest_training_summary(settings.indexer_db),
        "exit_examples": exit_examples_snapshot(
            settings.exit_examples_path,
            default_predicted_time=float(model_metrics.get("median_hold_time") or 0.0),
        ),
        "model_metrics": model_metrics,
        "error": "",
    }
    if not settings.indexer_db.exists():
        result["error"] = "indexer db not found"
        return result
    try:
        # Prefer the production cohort loader; it is backed by wallet_cohorts.
        cohorts = load_wallet_cohorts(settings.indexer_db, limit=50)
        with sqlite_connect(settings.indexer_db) as conn:
            if table_exists(conn, "wallet_cohorts"):
                status_expr = cohort_status_expression(conn, alias="")
                rows = conn.execute(
                    f"SELECT {status_expr} AS cohort, COUNT(*) AS wallets FROM wallet_cohorts GROUP BY {status_expr}"
                ).fetchall()
                result["counts"] = {str(row["cohort"]): int(row["wallets"] or 0) for row in rows}
                result["cohort_wallets"] = cohort_wallet_rows(conn, limit=10)
            if table_exists(conn, "scored_wallets"):
                result["scored_wallets"] = int(conn.execute("SELECT COUNT(*) FROM scored_wallets").fetchone()[0] or 0)
                result["top_wallets"] = top_wallet_rows(conn, include_cohorts=table_exists(conn, "wallet_cohorts"))
                result["scored_wallet_rows"] = result["top_wallets"]
            elif raw_transaction_count(conn) <= settings.scoring_fallback_max_rows:
                result["top_wallets"] = scoring_fallback_rows(settings.indexer_db)
                result["scored_wallet_rows"] = result["top_wallets"]
        if not result["top_wallets"] and cohorts:
            result["top_wallets"] = list(cohorts.values())[:50]
            result["scored_wallet_rows"] = result["top_wallets"]
        return result
    except Exception as exc:  # noqa: BLE001 - dashboard can show partial data.
        result["error"] = str(exc)
        return result


def latest_training_summary(db_path: Path) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    try:
        with sqlite_connect(db_path) as conn:
            if table_exists(conn, "training_state"):
                row = conn.execute(
                    "SELECT value, updated_at FROM training_state WHERE key = 'last_training_summary'"
                ).fetchone()
                if row:
                    payload = json.loads(str(row["value"] or "{}"))
                    if isinstance(payload, dict):
                        payload["updated_at"] = int(row["updated_at"] or 0)
                        return payload
            if table_exists(conn, "training_runs"):
                row = conn.execute(
                    "SELECT started_at, ok, summary_json FROM training_runs ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                if row:
                    payload = json.loads(str(row["summary_json"] or "{}"))
                    if isinstance(payload, dict):
                        payload["updated_at"] = int(row["started_at"] or 0)
                        payload.setdefault("ok", bool(row["ok"]))
                        return payload
    except Exception:
        return None
    return None


def positions_snapshot(settings: ServerSettings) -> dict[str, Any]:
    max_hold_hours = _env_int("POLYSIGNAL_PAPER_MAX_HOLD_HOURS", 36)
    stale_price_hours = _env_int("POLYSIGNAL_PAPER_STALE_PRICE_HOURS", 48)
    result = {
        "balance": 0.0,
        "pnl": 0.0,
        "daily_pnl": 0.0,
        "open_positions_count": 0,
        "open_positions": [],
        "total_positions": 0,
        "db_path": str(settings.paper_state_db),
        "source": "paper_state",
        "error": "",
    }
    try:
        if settings.paper_state_db.exists():
            with sqlite_connect(settings.paper_state_db) as conn:
                if table_exists(conn, "balance_history"):
                    latest = conn.execute(
                        "SELECT timestamp, balance, pnl FROM balance_history ORDER BY timestamp DESC LIMIT 1"
                    ).fetchone()
                    if latest:
                        result["balance"] = float(latest["balance"] or 0)
                        result["pnl"] = float(latest["pnl"] or 0)
                    since = int(time.time()) - 86400
                    daily = conn.execute(
                        "SELECT COALESCE(MAX(pnl) - MIN(pnl), 0) AS daily_pnl FROM balance_history WHERE timestamp >= ?",
                        (since,),
                    ).fetchone()
                    result["daily_pnl"] = float(daily["daily_pnl"] or 0) if daily else 0.0

        if settings.main_db.parent.resolve() == settings.paper_state_db.parent.resolve():
            main_positions = main_open_positions_snapshot(
                settings.main_db,
                max_hold_hours=max_hold_hours,
                stale_price_hours=stale_price_hours,
            )
            if main_positions:
                result["open_positions"] = main_positions["rows"]
                result["open_positions_count"] = len(main_positions["rows"])
                result["total_positions"] = main_positions["total"]
                result["source"] = "paper_positions"
                result["db_path"] = str(settings.main_db)
                return result

        if settings.paper_state_db.exists():
            with sqlite_connect(settings.paper_state_db) as conn:
                if table_exists(conn, "positions"):
                    rows = conn.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE UPPER(status) = 'OPEN'
                        ORDER BY opened_at DESC
                        LIMIT 100
                        """
                    ).fetchall()
                    result["open_positions"] = [
                        normalize_state_position_row(
                            row,
                            max_hold_hours=max_hold_hours,
                            stale_price_hours=stale_price_hours,
                        )
                        for row in rows
                    ]
                    result["open_positions_count"] = len(rows)
                    result["total_positions"] = int(conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0] or 0)
        else:
            result["error"] = "paper state db not found"
        return result
    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)
        return result


def main_open_positions_snapshot(
    db_path: Path,
    *,
    max_hold_hours: int,
    stale_price_hours: int,
) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    now = int(time.time())
    with sqlite_connect(db_path) as conn:
        if not table_exists(conn, "paper_positions"):
            return None
        rows = conn.execute(
            """
            SELECT
                p.position_id AS id,
                p.position_id,
                p.signal_id,
                p.opened_at,
                p.wallet,
                p.condition_id AS market_id,
                p.condition_id,
                p.asset,
                p.outcome,
                p.title AS market,
                p.title,
                p.entry_price,
                p.size_usdc AS size,
                p.size_usdc,
                p.shares,
                p.stop_loss,
                p.take_profit,
                p.status,
                p.closed_at,
                p.realized_pnl,
                ob.best_bid,
                ob.mid,
                ob.last_trade_price AS book_last_trade_price,
                ob.timestamp AS book_timestamp,
                (
                    SELECT t.price
                    FROM trades t
                    WHERE t.asset = p.asset
                    ORDER BY t.timestamp DESC
                    LIMIT 1
                ) AS trade_price,
                (
                    SELECT t.timestamp
                    FROM trades t
                    WHERE t.asset = p.asset
                    ORDER BY t.timestamp DESC
                    LIMIT 1
                ) AS trade_timestamp
            FROM paper_positions p
            LEFT JOIN order_books_latest ob ON ob.asset = p.asset
            WHERE UPPER(p.status) = 'OPEN'
            ORDER BY p.opened_at DESC
            LIMIT 100
            """
        ).fetchall()
        total = int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0] or 0)
    return {
        "rows": [
            normalize_main_position_row(
                row,
                now=now,
                max_hold_hours=max_hold_hours,
                stale_price_hours=stale_price_hours,
            )
            for row in rows
        ],
        "total": total,
    }


def normalize_main_position_row(
    row: sqlite3.Row,
    *,
    now: int,
    max_hold_hours: int,
    stale_price_hours: int,
) -> dict[str, Any]:
    entry = float(row["entry_price"] or 0.0)
    current, price_ts, price_source = position_mark_from_row(row, entry)
    size = float(row["size_usdc"] or row["size"] or 0.0)
    shares = float(row["shares"] or (size / entry if entry > 0 else 0.0))
    pnl = shares * current - size
    opened_at = int(row["opened_at"] or 0)
    open_seconds = max(0, now - opened_at)
    price_age = max(0, now - int(price_ts or opened_at or now))
    stop_loss = float(row["stop_loss"] or 0.0)
    take_profit = float(row["take_profit"] or 0.0)
    return {
        "id": str(row["id"] or row["position_id"] or ""),
        "position_id": str(row["position_id"] or row["id"] or ""),
        "signal_id": str(row["signal_id"] or ""),
        "market_id": str(row["market_id"] or row["condition_id"] or ""),
        "condition_id": str(row["condition_id"] or row["market_id"] or ""),
        "asset": str(row["asset"] or ""),
        "market": str(row["market"] or row["title"] or row["condition_id"] or ""),
        "title": str(row["title"] or row["market"] or ""),
        "outcome": str(row["outcome"] or ""),
        "wallet": str(row["wallet"] or ""),
        "side": "BUY",
        "size": round(size, 4),
        "size_usdc": round(size, 4),
        "entry_price": round(entry, 6),
        "current_price": round(current, 6),
        "stop_loss": round(stop_loss, 6),
        "take_profit": round(take_profit, 6),
        "tp_pct": round((take_profit - entry) / entry, 6) if entry > 0 else 0.0,
        "sl_pct": round((entry - stop_loss) / entry, 6) if entry > 0 else 0.0,
        "status": str(row["status"] or "OPEN"),
        "opened_at": opened_at,
        "closed_at": row["closed_at"],
        "pnl": round(pnl, 4),
        "open_seconds": open_seconds,
        "price_age_seconds": price_age,
        "price_source": price_source,
        "hold_status": position_hold_status(
            open_seconds=open_seconds,
            price_age_seconds=price_age,
            max_hold_hours=max_hold_hours,
            stale_price_hours=stale_price_hours,
        ),
    }


def normalize_state_position_row(
    row: sqlite3.Row,
    *,
    max_hold_hours: int,
    stale_price_hours: int,
) -> dict[str, Any]:
    now = int(time.time())
    entry = float(row["entry_price"] or 0.0)
    opened_at = int(row["opened_at"] or 0)
    open_seconds = max(0, now - opened_at)
    data = dict(row)
    data.update(
        {
            "current_price": entry,
            "open_seconds": open_seconds,
            "price_age_seconds": open_seconds,
            "price_source": "state",
            "hold_status": position_hold_status(
                open_seconds=open_seconds,
                price_age_seconds=open_seconds,
                max_hold_hours=max_hold_hours,
                stale_price_hours=stale_price_hours,
            ),
        }
    )
    return data


def position_mark_from_row(row: sqlite3.Row, entry: float) -> tuple[float, int, str]:
    candidates = (
        ("bid", row["best_bid"], row["book_timestamp"]),
        ("mid", row["mid"], row["book_timestamp"]),
        ("book_trade", row["book_last_trade_price"], row["book_timestamp"]),
        ("trade", row["trade_price"], row["trade_timestamp"]),
    )
    for source, price, timestamp in candidates:
        if price is not None and float(price or 0.0) > 0:
            return float(price), int(timestamp or 0), source
    return entry, 0, "entry"


def position_hold_status(
    *,
    open_seconds: int,
    price_age_seconds: int,
    max_hold_hours: int,
    stale_price_hours: int,
) -> str:
    if open_seconds >= max(1, max_hold_hours) * 3600:
        return "MAX HOLD"
    if price_age_seconds >= max(1, stale_price_hours) * 3600:
        return "STALE PRICE"
    return "LIVE"


def orderbook_snapshot(settings: ServerSettings, market_id: str) -> dict[str, Any]:
    identifier = str(market_id or "").strip()
    result: dict[str, Any] = {
        "market_id": identifier,
        "asset": "",
        "title": "",
        "outcome": "",
        "bids": [],
        "asks": [],
        "spread": 0.0,
        "mid_price": 0.0,
        "total_bid_depth": 0.0,
        "total_ask_depth": 0.0,
        "low_liquidity": False,
        "source": "",
        "updated_at": 0,
        "error": "",
    }
    if not identifier:
        result["error"] = "market_id is required"
        return result

    local = resolve_orderbook_reference(settings.main_db, identifier) or {}
    asset = str(local.get("asset") or identifier)
    result.update(
        {
            "market_id": str(local.get("market_id") or local.get("market") or identifier),
            "asset": asset,
            "title": str(local.get("title") or ""),
            "outcome": str(local.get("outcome") or ""),
        }
    )

    live_error = ""
    if asset:
        try:
            client = PolymarketClient(
                ApiConfig(
                    clob_base=os.environ.get("POLYMARKET_CLOB_BASE", "https://clob.polymarket.com"),
                    timeout_seconds=_env_int("POLYMARKET_CLOB_TIMEOUT", 8),
                    min_delay_seconds=0.0,
                )
            )
            payload = client.order_book(asset)
            if isinstance(payload, dict) and (payload.get("bids") or payload.get("asks")):
                return normalize_orderbook_payload(
                    payload,
                    fallback=result,
                    source="clob",
                    low_liquidity_threshold=_env_float("ORDERBOOK_LOW_LIQUIDITY_USD", 5000.0),
                )
        except (ApiError, OSError, RuntimeError, ValueError) as exc:
            live_error = str(exc)

    raw_payload = local.get("raw_payload")
    if isinstance(raw_payload, dict):
        snapshot = normalize_orderbook_payload(
            raw_payload,
            fallback=result,
            source="sqlite",
            low_liquidity_threshold=_env_float("ORDERBOOK_LOW_LIQUIDITY_USD", 5000.0),
        )
        if live_error:
            snapshot["warning"] = f"live CLOB unavailable: {live_error[:180]}"
        return snapshot

    if local:
        synthetic = synthetic_orderbook_payload(local)
        snapshot = normalize_orderbook_payload(
            synthetic,
            fallback=result,
            source="sqlite_summary",
            low_liquidity_threshold=_env_float("ORDERBOOK_LOW_LIQUIDITY_USD", 5000.0),
        )
        if live_error:
            snapshot["warning"] = f"live CLOB unavailable: {live_error[:180]}"
        return snapshot

    result["error"] = live_error or "order book not found"
    return result


def resolve_orderbook_reference(db_path: Path, identifier: str) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    with sqlite_connect(db_path) as conn:
        has_order_books = table_exists(conn, "order_books_latest")
        if has_order_books:
            row = conn.execute(
                """
                SELECT ob.*, '' AS title, '' AS outcome, ob.market AS condition_id
                FROM order_books_latest ob
                WHERE ob.asset = ? OR ob.market = ?
                ORDER BY ob.updated_at DESC
                LIMIT 1
                """,
                (identifier, identifier),
            ).fetchone()
            if row:
                return orderbook_local_row(row)

        if table_exists(conn, "paper_positions") and has_order_books:
            row = conn.execute(
                """
                SELECT
                    p.asset,
                    p.condition_id,
                    p.title,
                    p.outcome,
                    ob.*
                FROM paper_positions p
                LEFT JOIN order_books_latest ob ON ob.asset = p.asset
                WHERE p.position_id = ? OR p.asset = ? OR p.condition_id = ?
                ORDER BY p.opened_at DESC
                LIMIT 1
                """,
                (identifier, identifier, identifier),
            ).fetchone()
            if row:
                return orderbook_local_row(row)
        elif table_exists(conn, "paper_positions"):
            row = conn.execute(
                """
                SELECT
                    p.asset,
                    p.condition_id,
                    p.title,
                    p.outcome,
                    NULL AS market,
                    NULL AS raw_json,
                    NULL AS best_bid,
                    NULL AS best_ask,
                    NULL AS mid,
                    NULL AS spread,
                    NULL AS bid_depth_usdc,
                    NULL AS ask_depth_usdc,
                    NULL AS timestamp,
                    NULL AS updated_at
                FROM paper_positions p
                WHERE p.position_id = ? OR p.asset = ? OR p.condition_id = ?
                ORDER BY p.opened_at DESC
                LIMIT 1
                """,
                (identifier, identifier, identifier),
            ).fetchone()
            if row:
                return orderbook_local_row(row)

        if table_exists(conn, "trades") and has_order_books:
            row = conn.execute(
                """
                SELECT
                    t.asset,
                    t.condition_id,
                    t.title,
                    t.outcome,
                    ob.*
                FROM trades t
                LEFT JOIN order_books_latest ob ON ob.asset = t.asset
                WHERE t.asset = ? OR t.condition_id = ?
                ORDER BY t.timestamp DESC
                LIMIT 1
                """,
                (identifier, identifier),
            ).fetchone()
            if row:
                return orderbook_local_row(row)
        elif table_exists(conn, "trades"):
            row = conn.execute(
                """
                SELECT
                    t.asset,
                    t.condition_id,
                    t.title,
                    t.outcome,
                    NULL AS market,
                    NULL AS raw_json,
                    NULL AS best_bid,
                    NULL AS best_ask,
                    NULL AS mid,
                    NULL AS spread,
                    NULL AS bid_depth_usdc,
                    NULL AS ask_depth_usdc,
                    NULL AS timestamp,
                    NULL AS updated_at
                FROM trades t
                WHERE t.asset = ? OR t.condition_id = ?
                ORDER BY t.timestamp DESC
                LIMIT 1
                """,
                (identifier, identifier),
            ).fetchone()
            if row:
                return orderbook_local_row(row)
    return None


def orderbook_local_row(row: sqlite3.Row) -> dict[str, Any]:
    raw_payload: dict[str, Any] | None = None
    raw_text = str(row["raw_json"] or "") if "raw_json" in row.keys() else ""
    if raw_text:
        with contextlib.suppress(json.JSONDecodeError, TypeError):
            payload = json.loads(raw_text)
            if isinstance(payload, dict):
                raw_payload = payload
    return {
        "asset": str(row["asset"] or ""),
        "market_id": str(row["condition_id"] or row["market"] or ""),
        "market": str(row["market"] or row["condition_id"] or ""),
        "title": str(row["title"] or ""),
        "outcome": str(row["outcome"] or ""),
        "best_bid": _row_float(row, "best_bid"),
        "best_ask": _row_float(row, "best_ask"),
        "mid": _row_float(row, "mid"),
        "spread": _row_float(row, "spread"),
        "bid_depth_usdc": _row_float(row, "bid_depth_usdc"),
        "ask_depth_usdc": _row_float(row, "ask_depth_usdc"),
        "timestamp": _row_int(row, "timestamp") or _row_int(row, "updated_at"),
        "raw_payload": raw_payload,
    }


def synthetic_orderbook_payload(local: dict[str, Any]) -> dict[str, Any]:
    bid = float(local.get("best_bid") or 0.0)
    ask = float(local.get("best_ask") or 0.0)
    bid_depth = float(local.get("bid_depth_usdc") or 0.0)
    ask_depth = float(local.get("ask_depth_usdc") or 0.0)
    return {
        "market": local.get("market_id") or local.get("market") or "",
        "asset_id": local.get("asset") or "",
        "timestamp": local.get("timestamp") or 0,
        "bids": [{"price": bid, "size": bid_depth / bid if bid > 0 else 0.0}] if bid > 0 else [],
        "asks": [{"price": ask, "size": ask_depth / ask if ask > 0 else 0.0}] if ask > 0 else [],
    }


def normalize_orderbook_payload(
    payload: dict[str, Any],
    *,
    fallback: dict[str, Any],
    source: str,
    low_liquidity_threshold: float,
) -> dict[str, Any]:
    bids_all = parse_book_levels(payload.get("bids"), reverse=True)
    asks_all = parse_book_levels(payload.get("asks"), reverse=False)
    best_bid = max((level["price"] for level in bids_all), default=0.0)
    best_ask = min((level["price"] for level in asks_all), default=0.0)
    last_trade = _number_value(payload.get("last_trade_price"))
    mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else last_trade or best_bid or best_ask
    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
    total_bid_depth = sum(level["notional"] for level in bids_all)
    total_ask_depth = sum(level["notional"] for level in asks_all)
    timestamp = normalize_orderbook_timestamp(payload.get("timestamp"))
    return {
        **fallback,
        "market_id": str(payload.get("market") or fallback.get("market_id") or ""),
        "asset": str(payload.get("asset_id") or payload.get("token_id") or fallback.get("asset") or ""),
        "bids": bids_all[:20],
        "asks": asks_all[:20],
        "spread": round(max(0.0, spread), 6),
        "mid_price": round(float(mid_price or 0.0), 6),
        "total_bid_depth": round(total_bid_depth, 4),
        "total_ask_depth": round(total_ask_depth, 4),
        "low_liquidity": min(total_bid_depth, total_ask_depth) < low_liquidity_threshold,
        "source": source,
        "updated_at": timestamp,
        "error": "",
    }


def parse_book_levels(levels: Any, *, reverse: bool) -> list[dict[str, float]]:
    parsed: list[dict[str, float]] = []
    if not isinstance(levels, list):
        return parsed
    for level in levels:
        if isinstance(level, dict):
            price = _number_value(level.get("price"))
            size = _number_value(level.get("size"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _number_value(level[0])
            size = _number_value(level[1])
        else:
            continue
        if price <= 0 or size <= 0:
            continue
        parsed.append({"price": round(price, 6), "size": round(size, 4), "notional": round(price * size, 4)})
    return sorted(parsed, key=lambda item: item["price"], reverse=reverse)


def normalize_orderbook_timestamp(value: Any) -> int:
    timestamp = _number_value(value)
    if timestamp > 10_000_000_000:
        timestamp /= 1000
    return int(timestamp or time.time())


def _row_float(row: sqlite3.Row, key: str) -> float:
    return float(row[key] or 0.0) if key in row.keys() else 0.0


def _row_int(row: sqlite3.Row, key: str) -> int:
    return int(row[key] or 0) if key in row.keys() else 0


def equity_snapshot(
    settings: ServerSettings,
    *,
    timeframe_seconds: int = 300,
    max_points: int = 360,
) -> dict[str, Any]:
    timeframe_seconds = normalize_timeframe_seconds(timeframe_seconds)
    max_points = max(20, min(1000, int(max_points or 360)))
    now = int(time.time())
    since = now - timeframe_seconds
    result: dict[str, Any] = {
        "points": [],
        "timeframe_seconds": timeframe_seconds,
        "current": 0.0,
        "delta": 0.0,
        "updated_at": 0,
        "db_path": str(settings.paper_state_db),
        "error": "",
    }
    if not settings.paper_state_db.exists():
        result["error"] = "paper state db not found"
        return result
    try:
        with sqlite_connect(settings.paper_state_db) as conn:
            if not table_exists(conn, "balance_history"):
                result["error"] = "balance_history not found"
                return result
            rows = conn.execute(
                """
                SELECT timestamp, balance, pnl
                FROM balance_history
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (since,),
            ).fetchall()
            if not rows:
                rows = conn.execute(
                    """
                    SELECT timestamp, balance, pnl
                    FROM balance_history
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """
                ).fetchall()
                rows = list(reversed(rows))
            sampled = downsample_rows(rows, max_points=max_points)
            points = [
                {
                    "time": int(row["timestamp"] or 0),
                    "label": equity_point_label(int(row["timestamp"] or 0), timeframe_seconds),
                    "balance": round(float(row["balance"] or 0.0), 4),
                    "pnl": round(float(row["pnl"] or 0.0), 4),
                }
                for row in sampled
            ]
        if points:
            result["points"] = points
            result["current"] = float(points[-1]["balance"])
            result["delta"] = round(float(points[-1]["balance"]) - float(points[0]["balance"]), 4)
            result["updated_at"] = int(points[-1]["time"])
        return result
    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)
        return result


def normalize_timeframe_seconds(value: int) -> int:
    allowed = (5 * 60, 30 * 60, 60 * 60, 12 * 3600, 24 * 3600, 48 * 3600, 7 * 24 * 3600)
    requested = int(value or allowed[0])
    return min(allowed, key=lambda item: abs(item - requested))


def downsample_rows(rows: list[sqlite3.Row], *, max_points: int) -> list[sqlite3.Row]:
    if len(rows) <= max_points:
        return rows
    step = max(1, len(rows) // max_points)
    sampled = rows[::step]
    if sampled[-1] != rows[-1]:
        sampled.append(rows[-1])
    return sampled[-max_points:]


def equity_point_label(timestamp: int, timeframe_seconds: int) -> str:
    if timestamp <= 0:
        return ""
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    if timeframe_seconds <= 3600:
        return dt.strftime("%H:%M:%S")
    return dt.strftime("%m-%d %H:%M")


def trade_log_snapshot(
    settings: ServerSettings,
    *,
    limit: int = 250,
    market: str = "",
    date_from: int = 0,
    date_to: int = 0,
) -> dict[str, Any]:
    limit = max(1, min(1000, int(limit or 250)))
    result: dict[str, Any] = {
        "rows": [],
        "total": 0,
        "db_path": str(settings.main_db),
        "paper_state_db": str(settings.paper_state_db),
        "error": "",
    }
    try:
        rows: list[dict[str, Any]] = []
        rows.extend(main_trade_log_rows(settings.main_db, limit=limit * 2, market=market, date_from=date_from, date_to=date_to))
        rows.extend(consensus_reject_rows(settings.main_db, limit=limit, market=market, date_from=date_from, date_to=date_to))
        if not rows:
            rows.extend(paper_state_trade_log_rows(settings.paper_state_db, limit=limit, market=market, date_from=date_from, date_to=date_to))
        rows.sort(key=lambda item: int(item.get("timestamp") or 0), reverse=True)
        result["rows"] = rows[:limit]
        result["total"] = len(rows)
        return result
    except Exception as exc:  # noqa: BLE001 - dashboard can show partial data.
        result["error"] = str(exc)
        return result


def main_trade_log_rows(
    db_path: Path,
    *,
    limit: int,
    market: str = "",
    date_from: int = 0,
    date_to: int = 0,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    filters = ["event_type IN ('OPENED', 'CLOSED', 'BLOCKED')"]
    params: list[Any] = []
    append_trade_log_filters(filters, params, market=market, date_column="event_at", date_from=date_from, date_to=date_to)
    where = " AND ".join(filters)
    with sqlite_connect(db_path) as conn:
        if not table_exists(conn, "paper_events"):
            return []
        rows = conn.execute(
            f"""
            SELECT
                event_id,
                event_at,
                event_type,
                signal_id,
                position_id,
                wallet,
                condition_id,
                asset,
                outcome,
                title,
                reason,
                price,
                size_usdc,
                pnl,
                metadata_json
            FROM paper_events
            WHERE {where}
            ORDER BY event_at DESC
            LIMIT ?
            """,
            (*params, max(1, int(limit))),
        ).fetchall()
    return [normalize_paper_event_row(row) for row in rows]


def consensus_reject_rows(
    db_path: Path,
    *,
    limit: int,
    market: str = "",
    date_from: int = 0,
    date_to: int = 0,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    filters = ["final_decision = 'REJECT'"]
    params: list[Any] = []
    append_trade_log_filters(filters, params, market=market, date_column="decided_at", date_from=date_from, date_to=date_to)
    where = " AND ".join(filters)
    with sqlite_connect(db_path) as conn:
        if not table_exists(conn, "consensus_votes"):
            return []
        rows = conn.execute(
            f"""
            SELECT
                signal_id,
                decided_at,
                action,
                wallet,
                condition_id,
                asset,
                outcome,
                title,
                suggested_price,
                size_usdc,
                skeptic_reason,
                reason
            FROM consensus_votes
            WHERE {where}
            ORDER BY decided_at DESC
            LIMIT ?
            """,
            (*params, max(1, int(limit))),
        ).fetchall()
    return [normalize_consensus_reject_row(row) for row in rows]


def paper_state_trade_log_rows(
    db_path: Path,
    *,
    limit: int,
    market: str = "",
    date_from: int = 0,
    date_to: int = 0,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    filters = ["1 = 1"]
    params: list[Any] = []
    market_text = str(market or "").strip().lower()
    if market_text:
        filters.append("(LOWER(COALESCE(market_id, '')) LIKE ? OR LOWER(COALESCE(id, '')) LIKE ?)")
        like = f"%{market_text}%"
        params.extend([like, like])
    if int(date_from or 0) > 0:
        filters.append("opened_at >= ?")
        params.append(int(date_from))
    if int(date_to or 0) > 0:
        filters.append("opened_at <= ?")
        params.append(int(date_to))
    where = " AND ".join(filters)
    with sqlite_connect(db_path) as conn:
        if not table_exists(conn, "positions"):
            return []
        rows = conn.execute(
            f"""
            SELECT id, market_id, side, size, entry_price, status, opened_at, closed_at, pnl
            FROM positions
            WHERE {where}
            ORDER BY opened_at DESC
            LIMIT ?
            """,
            (*params, max(1, int(limit))),
        ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": f"state-open-{row['id']}",
                "timestamp": int(row["opened_at"] or 0),
                "market": str(row["market_id"] or ""),
                "action": str(row["side"] or "BUY").upper(),
                "price": float(row["entry_price"] or 0.0),
                "size": float(row["size"] or 0.0),
                "reason": "opened",
                "pnl": 0.0,
                "status": str(row["status"] or ""),
                "source": "paper_state",
                "asset": "",
                "condition_id": str(row["market_id"] or ""),
            }
        )
        if str(row["status"] or "").upper() == "CLOSED":
            result.append(
                {
                    "id": f"state-close-{row['id']}",
                    "timestamp": int(row["closed_at"] or row["opened_at"] or 0),
                    "market": str(row["market_id"] or ""),
                    "action": "SELL",
                    "price": 0.0,
                    "size": float(row["size"] or 0.0),
                    "reason": "closed",
                    "pnl": float(row["pnl"] or 0.0),
                    "status": "CLOSED",
                    "source": "paper_state",
                    "asset": "",
                    "condition_id": str(row["market_id"] or ""),
                }
            )
    return result


def append_trade_log_filters(
    filters: list[str],
    params: list[Any],
    *,
    market: str,
    date_column: str,
    date_from: int,
    date_to: int,
) -> None:
    market_text = str(market or "").strip().lower()
    if market_text:
        filters.append(
            "(LOWER(COALESCE(title, '')) LIKE ? OR LOWER(COALESCE(asset, '')) LIKE ? OR LOWER(COALESCE(condition_id, '')) LIKE ?)"
        )
        like = f"%{market_text}%"
        params.extend([like, like, like])
    if int(date_from or 0) > 0:
        filters.append(f"{date_column} >= ?")
        params.append(int(date_from))
    if int(date_to or 0) > 0:
        filters.append(f"{date_column} <= ?")
        params.append(int(date_to))


def normalize_paper_event_row(row: sqlite3.Row) -> dict[str, Any]:
    event_type = str(row["event_type"] or "").upper()
    reason = str(row["reason"] or "")
    action = {
        "OPENED": "BUY",
        "CLOSED": "SELL",
        "BLOCKED": "REJECT",
    }.get(event_type, event_type)
    if event_type == "CLOSED" and not reason:
        reason = "closed"
    return {
        "id": str(row["event_id"] or row["signal_id"] or ""),
        "timestamp": int(row["event_at"] or 0),
        "market": str(row["title"] or row["condition_id"] or row["asset"] or ""),
        "action": action,
        "price": float(row["price"] or 0.0),
        "size": float(row["size_usdc"] or 0.0),
        "reason": trade_reason_label(reason, event_type=event_type),
        "pnl": float(row["pnl"] or 0.0),
        "event_type": event_type,
        "source": "paper_events",
        "wallet": str(row["wallet"] or ""),
        "asset": str(row["asset"] or ""),
        "condition_id": str(row["condition_id"] or ""),
        "outcome": str(row["outcome"] or ""),
    }


def normalize_consensus_reject_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": f"consensus-{row['signal_id']}",
        "timestamp": int(row["decided_at"] or 0),
        "market": str(row["title"] or row["condition_id"] or row["asset"] or ""),
        "action": "REJECT",
        "price": float(row["suggested_price"] or 0.0),
        "size": float(row["size_usdc"] or 0.0),
        "reason": trade_reason_label(str(row["skeptic_reason"] or row["reason"] or "consensus"), event_type="CONSENSUS"),
        "pnl": 0.0,
        "event_type": "CONSENSUS_REJECT",
        "source": "consensus_votes",
        "wallet": str(row["wallet"] or ""),
        "asset": str(row["asset"] or ""),
        "condition_id": str(row["condition_id"] or ""),
        "outcome": str(row["outcome"] or ""),
    }


def trade_reason_label(reason: str, *, event_type: str = "") -> str:
    text = str(reason or "").strip()
    lower = text.lower()
    if event_type == "CONSENSUS" or "consensus" in lower or "skeptic" in lower:
        return f"Consensus: {text}" if text else "Consensus"
    if "take_profit" in lower or lower == "tp":
        return "TP"
    if "stop_loss" in lower or lower == "sl":
        return "SL"
    if "exit_model" in lower or "early_exit" in lower:
        return "Exit Model"
    if "risk_trim" in lower:
        return "Risk Trim"
    if "cohort_auto_policy" in lower:
        return "Cohort Policy"
    if "duplicate_asset" in lower:
        return "Duplicate Asset"
    if event_type == "BLOCKED":
        return text or "Blocked"
    return text or "-"


def paper_status_snapshot(settings: ServerSettings) -> dict[str, Any]:
    spec = component_specs(settings)["live_paper"]
    status = component_status(spec, discover=True)
    positions = positions_snapshot(settings)
    main_summary = main_paper_summary(settings)
    return {
        **status,
        "balance": positions["balance"] or main_summary.get("balance", 0.0),
        "pnl": positions["pnl"] or main_summary.get("total_pnl", 0.0),
        "daily_pnl": positions["daily_pnl"],
        "open_positions": positions["open_positions_count"] or main_summary.get("open_positions", 0),
        "total_positions": positions["total_positions"] or main_summary.get("total_positions", 0),
        "dry_run": status.get("dry_run", settings.dry_run),
    }


def live_payload(settings: ServerSettings) -> dict[str, Any]:
    now = time.time()
    with LIVE_PAYLOAD_LOCK:
        cached = LIVE_PAYLOAD_CACHE.get("payload")
        if cached is not None and float(LIVE_PAYLOAD_CACHE.get("expires_at") or 0) > now:
            return dict(cached)
    payload = build_live_payload(settings)
    with LIVE_PAYLOAD_LOCK:
        LIVE_PAYLOAD_CACHE["payload"] = dict(payload)
        LIVE_PAYLOAD_CACHE["expires_at"] = now + 3.0
    return payload


def build_live_payload(settings: ServerSettings) -> dict[str, Any]:
    metrics = indexer_metrics(settings, include_process=False)
    positions = positions_snapshot(settings)
    main_summary = main_paper_summary(settings)
    return {
        "raw_events": metrics["raw_events"],
        "last_block": metrics["last_block"],
        "indexer_speed": metrics["blocks_per_second"],
        "progress": metrics["progress"],
        "balance": positions["balance"] or main_summary.get("balance", 0.0),
        "pnl": positions["pnl"] or main_summary.get("total_pnl", 0.0),
        "open_positions": positions["open_positions_count"] or main_summary.get("open_positions", 0),
        "signals_count": signals_count(settings.main_db),
        "components": registry_component_snapshot(settings),
        "time": int(time.time()),
    }


def registry_component_snapshot(settings: ServerSettings) -> dict[str, dict[str, Any]]:
    specs = component_specs(settings)
    statuses = cached_component_statuses(settings)
    snapshot: dict[str, dict[str, Any]] = {}
    for key, spec in specs.items():
        status = statuses.get(key) or {}
        snapshot[key] = {
            "running": bool(status.get("running")),
            "pid": int(status.get("pid") or 0),
            "name": spec.label,
        }
    return snapshot


def main_paper_summary(settings: ServerSettings) -> dict[str, Any]:
    if not settings.main_db.exists():
        return {}
    try:
        with Store(settings.main_db) as store:
            store.init_schema()
            return _portfolio_snapshot(store, bankroll=200.0)
    except Exception:
        return {}


def signals_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    try:
        with sqlite_connect(db_path) as conn:
            if not table_exists(conn, "signals"):
                return 0
            return int(conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] or 0)
    except Exception:
        return 0


def consensus_snapshot(settings: ServerSettings, *, limit: int = 25) -> dict[str, Any]:
    result: dict[str, Any] = {
        "votes": [],
        "counts": {"EXECUTE": 0, "REJECT": 0},
        "total": 0,
        "db_path": str(settings.main_db),
        "error": "",
    }
    if not settings.main_db.exists():
        result["error"] = "main db not found"
        return result
    try:
        with sqlite_connect(settings.main_db) as conn:
            if not table_exists(conn, "consensus_votes"):
                return result
            count_rows = conn.execute(
                """
                SELECT final_decision, COUNT(*) AS votes
                FROM consensus_votes
                GROUP BY final_decision
                """
            ).fetchall()
            counts = {str(row["final_decision"]): int(row["votes"] or 0) for row in count_rows}
            result["counts"] = {"EXECUTE": counts.get("EXECUTE", 0), "REJECT": counts.get("REJECT", 0)}
            result["total"] = int(sum(counts.values()))
            rows = conn.execute(
                """
                SELECT *
                FROM consensus_votes
                ORDER BY decided_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            result["votes"] = [consensus_row_to_dict(row) for row in rows]
        return result
    except Exception as exc:  # noqa: BLE001 - dashboard can show partial data.
        result["error"] = str(exc)
        return result


def top_wallet_rows(conn: sqlite3.Connection, *, include_cohorts: bool) -> list[dict[str, Any]]:
    columns = table_columns(conn, "scored_wallets")
    wallet_expr = scored_wallet_expression(columns, alias="s")
    computed_expr = (
        "COALESCE(NULLIF(s.calculated_at, 0), s.computed_at)"
        if "calculated_at" in columns
        else "s.computed_at"
    )
    avg_hold_expr = "s.avg_hold_time" if "avg_hold_time" in columns else "0.0"
    consistency_expr = "s.consistency" if "consistency" in columns else "0.0"
    if include_cohorts:
        cohort_wallet_expr = cohort_wallet_expression(conn, alias="c")
        cohort_status_expr = cohort_status_expression(conn, alias="c")
        query = f"""
            WITH top_scored AS (
                SELECT
                    {wallet_expr} AS user_address,
                    {wallet_expr} AS wallet,
                    s.score,
                    s.pnl,
                    s.sharpe,
                    s.volume,
                    s.trade_count,
                    s.win_rate,
                    s.profit_factor,
                    s.max_drawdown,
                    {avg_hold_expr} AS avg_hold_time,
                    {consistency_expr} AS consistency,
                    {computed_expr} AS computed_at
                FROM scored_wallets s
                ORDER BY s.sharpe DESC, s.pnl DESC, s.volume DESC
                LIMIT 50
            )
            SELECT
                top_scored.user_address,
                top_scored.wallet,
                COALESCE({cohort_status_expr}, '') AS cohort,
                COALESCE({cohort_status_expr}, '') AS status,
                top_scored.score,
                top_scored.pnl,
                top_scored.sharpe,
                top_scored.volume,
                top_scored.trade_count,
                top_scored.win_rate,
                top_scored.profit_factor,
                top_scored.max_drawdown,
                top_scored.avg_hold_time,
                top_scored.consistency,
                top_scored.computed_at
            FROM top_scored
            LEFT JOIN wallet_cohorts c ON {cohort_wallet_expr} = top_scored.user_address
            ORDER BY top_scored.sharpe DESC, top_scored.pnl DESC, top_scored.volume DESC
        """
    else:
        query = f"""
            SELECT
                {wallet_expr} AS user_address,
                {wallet_expr} AS wallet,
                '' AS cohort,
                '' AS status,
                s.score,
                s.pnl,
                s.sharpe,
                s.volume,
                s.trade_count,
                s.win_rate,
                s.profit_factor,
                s.max_drawdown,
                {avg_hold_expr} AS avg_hold_time,
                {consistency_expr} AS consistency,
                {computed_expr} AS computed_at
            FROM scored_wallets s
            ORDER BY sharpe DESC, pnl DESC, volume DESC
            LIMIT 50
        """
    rows = conn.execute(query).fetchall()
    return [dict(row) for row in rows]


def cohort_wallet_rows(conn: sqlite3.Connection, *, limit: int = 10) -> dict[str, list[dict[str, Any]]]:
    if not table_exists(conn, "wallet_cohorts"):
        return {}
    columns = table_columns(conn, "wallet_cohorts")
    wallet_expr = cohort_wallet_expression(conn, alias="c")
    cohort_expr = cohort_status_expression(conn, alias="c")
    stability_expr = "c.stability_score" if "stability_score" in columns else "0.0"
    score_expr = "c.score" if "score" in columns else stability_expr
    pnl_expr = "c.pnl" if "pnl" in columns else "0.0"
    sharpe_expr = "c.sharpe" if "sharpe" in columns else "0.0"
    volume_expr = "c.volume" if "volume" in columns else "0.0"
    trade_count_expr = "c.trade_count" if "trade_count" in columns else "0"
    win_rate_expr = "c.win_rate" if "win_rate" in columns else "0.0"
    profit_factor_expr = "c.profit_factor" if "profit_factor" in columns else "0.0"
    max_drawdown_expr = "c.max_drawdown" if "max_drawdown" in columns else "0.0"
    avg_hold_expr = "c.avg_hold_time" if "avg_hold_time" in columns else "0.0"
    updated_expr = "c.updated_at" if "updated_at" in columns else "0"
    query = f"""
        SELECT
            {wallet_expr} AS user_address,
            {wallet_expr} AS wallet,
            {cohort_expr} AS cohort,
            {cohort_expr} AS status,
            {stability_expr} AS stability_score,
            {score_expr} AS score,
            {pnl_expr} AS pnl,
            {sharpe_expr} AS sharpe,
            {volume_expr} AS volume,
            CAST({trade_count_expr} AS INTEGER) AS trade_count,
            {win_rate_expr} AS win_rate,
            {profit_factor_expr} AS profit_factor,
            {max_drawdown_expr} AS max_drawdown,
            {avg_hold_expr} AS avg_hold_time,
            {updated_expr} AS updated_at,
            {updated_expr} AS computed_at
        FROM wallet_cohorts c
        WHERE {cohort_expr} = ?
        ORDER BY {sharpe_expr} DESC, {pnl_expr} DESC, {volume_expr} DESC
        LIMIT ?
    """
    result: dict[str, list[dict[str, Any]]] = {}
    for cohort in ("STABLE", "CANDIDATE", "WATCH", "NOISE"):
        rows = conn.execute(query, (cohort, max(1, int(limit)))).fetchall()
        result[cohort] = [dict(row) for row in rows]
    return result


def scored_wallet_expression(columns: set[str], *, alias: str) -> str:
    prefix = f"{alias}." if alias else ""
    if {"user_address", "address", "wallet"}.issubset(columns):
        return f"COALESCE(NULLIF({prefix}user_address, ''), NULLIF({prefix}address, ''), {prefix}wallet)"
    if {"user_address", "wallet"}.issubset(columns):
        return f"COALESCE(NULLIF({prefix}user_address, ''), {prefix}wallet)"
    if {"address", "wallet"}.issubset(columns):
        return f"COALESCE(NULLIF({prefix}address, ''), {prefix}wallet)"
    if "user_address" in columns:
        return f"{prefix}user_address"
    if "address" in columns:
        return f"{prefix}address"
    return f"{prefix}wallet"


def cohort_wallet_expression(conn: sqlite3.Connection, *, alias: str) -> str:
    columns = table_columns(conn, "wallet_cohorts")
    prefix = f"{alias}." if alias else ""
    if {"user_address", "wallet"}.issubset(columns):
        return f"COALESCE(NULLIF({prefix}user_address, ''), {prefix}wallet)"
    if "user_address" in columns:
        return f"{prefix}user_address"
    return f"{prefix}wallet"


def cohort_status_expression(conn: sqlite3.Connection, *, alias: str) -> str:
    columns = table_columns(conn, "wallet_cohorts")
    prefix = f"{alias}." if alias else ""
    if {"cohort", "status"}.issubset(columns):
        return f"COALESCE(NULLIF({prefix}cohort, ''), {prefix}status)"
    if "cohort" in columns:
        return f"{prefix}cohort"
    if "status" in columns:
        return f"{prefix}status"
    return "'NOISE'"


def exit_examples_snapshot(path: Path, *, default_predicted_time: float = 0.0) -> dict[str, Any]:
    if not path.exists():
        return {"count": 0, "examples": [], "path": str(path), "updated_at": 0}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"count": 0, "examples": [], "path": str(path), "updated_at": 0, "error": str(exc)}
    examples = payload if isinstance(payload, list) else payload.get("examples", []) if isinstance(payload, dict) else []
    if not isinstance(examples, list):
        examples = []
    try:
        updated_at = int(path.stat().st_mtime)
    except OSError:
        updated_at = 0
    return {
        "count": len(examples),
        "examples": [
            normalize_exit_example(example, default_predicted_time=default_predicted_time)
            for example in examples[:10]
            if isinstance(example, dict)
        ],
        "path": str(path),
        "updated_at": updated_at,
    }


def normalize_exit_example(example: dict[str, Any], *, default_predicted_time: float = 0.0) -> dict[str, Any]:
    entry_time = _number_value(example.get("entry_time", example.get("entry_ts", 0)))
    exit_time = _number_value(example.get("exit_time", example.get("exit_ts", 0)))
    entry_price = _number_value(example.get("entry_price", 0.0))
    exit_price = _number_value(example.get("exit_price", 0.0))
    pnl_proxy = _number_value(example.get("pnl_proxy", 0.0))
    entry_notional = _number_value(example.get("entry_notional", 0.0))
    pnl_percent = example.get("pnl_percent")
    if pnl_percent is None:
        if entry_price:
            pnl_percent = (exit_price - entry_price) / entry_price * 100.0
        elif entry_notional:
            pnl_percent = pnl_proxy / entry_notional * 100.0
        else:
            pnl_percent = 0.0
    return {
        "whale_address": str(example.get("whale_address") or example.get("user_address") or example.get("wallet") or ""),
        "market_id": str(example.get("market_id") or ""),
        "entry_time": int(entry_time or 0),
        "exit_time": int(exit_time or 0),
        "pnl_percent": round(float(pnl_percent or 0.0), 4),
        "predicted_time": round(
            float(example.get("predicted_time") or example.get("predicted_hold_seconds") or default_predicted_time or 0.0),
            4,
        ),
    }


def exit_stats_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "updated_at": 0, "model_type": "", "median_hold_time": 0, "mae": None, "r2": None}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"path": str(path), "updated_at": 0, "model_type": "", "median_hold_time": 0, "mae": None, "r2": None, "error": str(exc)}
    try:
        updated_at = int(path.stat().st_mtime)
    except OSError:
        updated_at = 0
    return {
        "path": str(path),
        "updated_at": updated_at,
        "model_type": str(payload.get("model_type") or payload.get("type") or ""),
        "median_hold_time": _number_value(payload.get("median_hold_time", payload.get("median_hold_seconds", 0))),
        "mae": payload.get("mae", payload.get("mae_seconds")),
        "r2": payload.get("r2"),
        "train_examples": int(payload.get("train_examples") or 0),
        "test_examples": int(payload.get("test_examples") or 0),
        "fallback_reason": str(payload.get("fallback_reason") or ""),
    }


def _number_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def scoring_fallback_rows(db_path: Path) -> list[dict[str, Any]]:
    try:
        df = calculate_all(db_path, limit=10, min_trades=1)
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []
    rows: list[dict[str, Any]] = []
    for row in df.head(10).to_dict(orient="records"):
        rows.append({str(key): _json_value(value) for key, value in row.items()})
    return rows


def raw_transaction_count(conn: sqlite3.Connection) -> int:
    if not table_exists(conn, "raw_transactions"):
        return 0
    return int(conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0] or 0)


def block_speed(last_block: float) -> float:
    now = time.time()
    block = float(last_block)
    with METRICS_LOCK:
        previous_block = METRICS_CURSOR.get("last_block", 0.0)
        previous_at = METRICS_CURSOR.get("seen_at", 0.0)
        current_speed = METRICS_CURSOR.get("current_speed", 0.0)
        speed_at = METRICS_CURSOR.get("speed_at", 0.0)

        if previous_block <= 0 or previous_at <= 0 or block < previous_block:
            METRICS_CURSOR["last_block"] = block
            METRICS_CURSOR["seen_at"] = now
            METRICS_CURSOR["current_speed"] = 0.0
            METRICS_CURSOR["speed_at"] = 0.0
            return 0.0

        if block > previous_block:
            elapsed = max(0.001, now - previous_at)
            speed = round(max(0.0, (block - previous_block) / elapsed), 4)
            METRICS_CURSOR["last_block"] = block
            METRICS_CURSOR["seen_at"] = now
            METRICS_CURSOR["current_speed"] = speed
            METRICS_CURSOR["speed_at"] = now
            return speed

        if current_speed > 0 and speed_at > 0 and now - speed_at <= METRICS_SPEED_STALE_SECONDS:
            return round(current_speed, 4)

        METRICS_CURSOR["current_speed"] = 0.0
        return 0.0


@contextlib.contextmanager
def sqlite_connect(path: Path) -> Any:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
    finally:
        conn.close()


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def raw_events_count(conn: sqlite3.Connection) -> int:
    if table_exists(conn, "indexer_counters"):
        row = conn.execute(
            "SELECT value FROM indexer_counters WHERE name = 'raw_events' LIMIT 1"
        ).fetchone()
        if row is not None:
            return int(row["value"] or 0)
    if table_exists(conn, "raw_transactions"):
        # Fallback for old databases before the indexer initializes counters.
        row = conn.execute("SELECT MAX(rowid) FROM raw_transactions").fetchone()
        return int(row[0] or 0)
    return 0


def tail_file(path: Path, *, lines: int = 20) -> str:
    if not path.exists():
        return ""
    try:
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(content[-lines:])


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    if not raw:
        return default.resolve()
    path = Path(raw)
    return (ROOT / path).resolve() if not path.is_absolute() else path.resolve()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _json_value(value: Any) -> Any:
    try:
        if hasattr(value, "item"):
            return value.item()
    except Exception:
        pass
    return value


def _missing_dependency_message() -> str:
    return (
        "FastAPI server dependencies are missing. Install them with: "
        "python -m pip install -r requirements-server.txt"
    )


app = create_app()


def main() -> int:
    if FastAPI is None:
        print(_missing_dependency_message(), file=sys.stderr)
        return 2
    try:
        import uvicorn
    except ModuleNotFoundError:
        print(_missing_dependency_message(), file=sys.stderr)
        return 2
    settings = settings_from_env()
    uvicorn.run(app, host=settings.host, port=settings.port, ws="wsproto")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
