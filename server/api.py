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

from polymarket_signal_bot.cohorts import load_wallet_cohorts
from polymarket_signal_bot.live_paper_runner import _portfolio_snapshot
from polymarket_signal_bot.monitor import Monitor, MonitorConfig
from polymarket_signal_bot.scoring import calculate_all
from polymarket_signal_bot.storage import DEFAULT_DB_PATH, Store


TARGET_RAW_EVENTS = 86_000_000
COMPONENT_KEYS = ("indexer", "monitor", "live_paper")
SERVER_LOG_DIR = ROOT / "data" / "server_logs"
PROCESS_LOCK = threading.RLock()
PROCESS_REGISTRY: dict[str, "ManagedProcess"] = {}
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
LIVE_PAYLOAD_CACHE: dict[str, Any] = {"payload": None, "expires_at": 0.0}
LIVE_PAYLOAD_LOCK = threading.RLock()


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
        return {"components": component_statuses(settings), "time": int(time.time())}

    @api.get("/api/metrics")
    async def api_metrics() -> dict[str, Any]:
        return indexer_metrics(settings_from_env())

    @api.get("/api/wallets")
    async def api_wallets() -> dict[str, Any]:
        return wallet_metrics(settings_from_env())

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
        return training_status_snapshot(settings_from_env())

    @api.get("/api/positions")
    async def api_positions() -> dict[str, Any]:
        return positions_snapshot(settings_from_env())

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
        return paper_status_snapshot(settings_from_env())

    @api.websocket("/ws/live")
    async def ws_live(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                settings = settings_from_env()
                await websocket.send_json(live_payload(settings))
                await asyncio.sleep(0.5)
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

    return {
        "indexer": ComponentSpec(
            key="indexer",
            label="Indexer",
            command=(
                sys.executable,
                "-m",
                "src.indexer",
                "--sync",
                "--db",
                str(settings.indexer_db),
            ),
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
        running = component_status(spec)
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
        return {
            "name": spec.label,
            "running": True,
            "pid": process.pid,
            "started": True,
            "uptime_seconds": 0,
            "dry_run": spec.dry_run,
            "log_tail": tail_file(spec.log_path),
        }


def stop_component(spec: ComponentSpec, *, timeout_seconds: float = 10.0) -> dict[str, Any]:
    with PROCESS_LOCK:
        managed = PROCESS_REGISTRY.get(spec.key)
        pid = managed.process.pid if managed and managed.process.poll() is None else discover_pid(spec.patterns)
        if not pid:
            cleanup_component(spec.key)
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
        return {
            "name": spec.label,
            "running": not terminated,
            "pid": pid if not terminated else 0,
            "stopped": terminated,
            "message": "stopped" if terminated else "kill failed",
            "log_tail": tail_file(spec.log_path),
        }


def component_statuses(settings: ServerSettings) -> dict[str, dict[str, Any]]:
    return {key: component_status(spec) for key, spec in component_specs(settings).items()}


def component_status(spec: ComponentSpec) -> dict[str, Any]:
    with PROCESS_LOCK:
        managed = PROCESS_REGISTRY.get(spec.key)
        if managed and managed.process.poll() is None:
            pid = int(managed.process.pid)
            started_at = managed.started_at
            dry_run = managed.dry_run
        else:
            cleanup_component(spec.key)
            pid = discover_pid(spec.patterns)
            started_at = 0.0
            dry_run = spec.dry_run
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


def cleanup_component(key: str) -> None:
    managed = PROCESS_REGISTRY.get(key)
    if not managed:
        return
    if managed.process.poll() is None:
        return
    with contextlib.suppress(Exception):
        managed.log_handle.close()
    PROCESS_REGISTRY.pop(key, None)


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
    snapshot = {
        "raw_events": 0,
        "last_block": 0,
        "blocks_per_second": 0.0,
        "progress": 0.0,
        "target": TARGET_RAW_EVENTS,
        "updated_at": 0,
        "db_path": str(settings.indexer_db),
        "running": component_status(component_specs(settings)["indexer"])["running"] if include_process else False,
        "error": "",
    }
    if not settings.indexer_db.exists():
        snapshot["error"] = "indexer db not found"
        return snapshot
    try:
        with sqlite_connect(settings.indexer_db) as conn:
            if table_exists(conn, "raw_transactions"):
                snapshot["raw_events"] = int(conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0] or 0)
            if table_exists(conn, "indexer_state"):
                row = conn.execute(
                    "SELECT last_block, updated_at FROM indexer_state ORDER BY updated_at DESC LIMIT 1"
                ).fetchone()
                if row:
                    snapshot["last_block"] = int(row["last_block"] or 0)
                    snapshot["updated_at"] = int(row["updated_at"] or 0)
        snapshot["progress"] = min(1.0, float(snapshot["raw_events"]) / TARGET_RAW_EVENTS)
        snapshot["blocks_per_second"] = block_speed(float(snapshot["last_block"]))
        return snapshot
    except Exception as exc:  # noqa: BLE001 - API should stay up during SQLite writes.
        snapshot["error"] = str(exc)
        return snapshot


def wallet_metrics(settings: ServerSettings) -> dict[str, Any]:
    model_metrics = exit_stats_snapshot(settings.exit_stats_path)
    result: dict[str, Any] = {
        "counts": {},
        "top_wallets": [],
        "scored_wallet_rows": [],
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
    result = {
        "balance": 0.0,
        "pnl": 0.0,
        "daily_pnl": 0.0,
        "open_positions_count": 0,
        "open_positions": [],
        "total_positions": 0,
        "db_path": str(settings.paper_state_db),
        "error": "",
    }
    if not settings.paper_state_db.exists():
        result["error"] = "paper state db not found"
        return result
    try:
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
                result["open_positions"] = [dict(row) for row in rows]
                result["open_positions_count"] = len(rows)
                result["total_positions"] = int(conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0] or 0)
        return result
    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)
        return result


def paper_status_snapshot(settings: ServerSettings) -> dict[str, Any]:
    spec = component_specs(settings)["live_paper"]
    status = component_status(spec)
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
        LIVE_PAYLOAD_CACHE["expires_at"] = now + 1.0
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
    snapshot: dict[str, dict[str, Any]] = {}
    for key, spec in specs.items():
        status = component_status(spec)
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
            SELECT
                {wallet_expr} AS user_address,
                {wallet_expr} AS wallet,
                {cohort_status_expr} AS cohort,
                {cohort_status_expr} AS status,
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
            LEFT JOIN wallet_cohorts c ON {cohort_wallet_expr} = {wallet_expr}
            ORDER BY s.sharpe DESC, s.pnl DESC, s.volume DESC
            LIMIT 50
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
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
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
