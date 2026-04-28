from __future__ import annotations

"""FastAPI backend for PolySignal.

Run locally:

    python server/api.py

The server is intentionally a thin orchestration layer. Existing modules remain
the source of indexing, monitoring, scoring, cohorts, and paper trading logic;
this file starts those workers as subprocesses and exposes their SQLite-backed
state over HTTP/WebSocket for Streamlit today and a React control room later.
"""

import asyncio
import contextlib
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
METRICS_CURSOR: dict[str, float] = {"last_block": 0.0, "seen_at": 0.0}


@dataclass(frozen=True)
class ServerSettings:
    main_db: Path = ROOT / DEFAULT_DB_PATH
    indexer_db: Path = ROOT / "data" / "indexer.db"
    paper_state_db: Path = ROOT / "data" / "paper_state.db"
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


if isinstance(BaseModel, type) and BaseModel is not object:

    class PaperStartRequest(BaseModel):
        dry_run: bool | None = None

else:

    class PaperStartRequest:  # pragma: no cover - used only without pydantic installed.
        def __init__(self, dry_run: bool | None = None) -> None:
            self.dry_run = dry_run


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


def start_component(spec: ComponentSpec) -> dict[str, Any]:
    with PROCESS_LOCK:
        running = component_status(spec)
        if running["running"]:
            return {**running, "started": False, "message": "already running"}

        spec.log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = spec.log_path.open("ab")
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


def indexer_metrics(settings: ServerSettings) -> dict[str, Any]:
    snapshot = {
        "raw_events": 0,
        "last_block": 0,
        "blocks_per_second": 0.0,
        "progress": 0.0,
        "target": TARGET_RAW_EVENTS,
        "updated_at": 0,
        "db_path": str(settings.indexer_db),
        "running": component_status(component_specs(settings)["indexer"])["running"],
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
    result: dict[str, Any] = {
        "counts": {},
        "top_wallets": [],
        "scored_wallets": 0,
        "db_path": str(settings.indexer_db),
        "error": "",
    }
    if not settings.indexer_db.exists():
        result["error"] = "indexer db not found"
        return result
    try:
        # Prefer the production cohort loader; it is backed by wallet_cohorts.
        cohorts = load_wallet_cohorts(settings.indexer_db, limit=10)
        with sqlite_connect(settings.indexer_db) as conn:
            if table_exists(conn, "wallet_cohorts"):
                rows = conn.execute("SELECT status, COUNT(*) AS wallets FROM wallet_cohorts GROUP BY status").fetchall()
                result["counts"] = {str(row["status"]): int(row["wallets"] or 0) for row in rows}
            if table_exists(conn, "scored_wallets"):
                result["scored_wallets"] = int(conn.execute("SELECT COUNT(*) FROM scored_wallets").fetchone()[0] or 0)
                result["top_wallets"] = top_wallet_rows(conn, include_cohorts=table_exists(conn, "wallet_cohorts"))
            elif raw_transaction_count(conn) <= settings.scoring_fallback_max_rows:
                result["top_wallets"] = scoring_fallback_rows(settings.indexer_db)
        if not result["top_wallets"] and cohorts:
            result["top_wallets"] = list(cohorts.values())[:10]
        return result
    except Exception as exc:  # noqa: BLE001 - dashboard can show partial data.
        result["error"] = str(exc)
        return result


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
    metrics = indexer_metrics(settings)
    paper = paper_status_snapshot(settings)
    return {
        "raw_events": metrics["raw_events"],
        "last_block": metrics["last_block"],
        "indexer_speed": metrics["blocks_per_second"],
        "progress": metrics["progress"],
        "balance": paper["balance"],
        "pnl": paper["pnl"],
        "open_positions": paper["open_positions"],
        "signals_count": signals_count(settings.main_db),
        "components": {
            key: {"running": value["running"], "pid": value["pid"]}
            for key, value in component_statuses(settings).items()
        },
        "time": int(time.time()),
    }


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
    if include_cohorts:
        query = """
            SELECT
                s.wallet,
                COALESCE(c.status, '') AS status,
                s.score,
                s.pnl,
                s.volume,
                s.trade_count,
                s.win_rate,
                s.profit_factor,
                s.max_drawdown,
                s.computed_at
            FROM scored_wallets s
            LEFT JOIN wallet_cohorts c ON c.wallet = s.wallet
            ORDER BY s.score DESC, s.volume DESC
            LIMIT 10
        """
    else:
        query = """
            SELECT
                wallet,
                '' AS status,
                score,
                pnl,
                volume,
                trade_count,
                win_rate,
                profit_factor,
                max_drawdown,
                computed_at
            FROM scored_wallets
            ORDER BY score DESC, volume DESC
            LIMIT 10
        """
    rows = conn.execute(query).fetchall()
    return [dict(row) for row in rows]


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
    previous_block = METRICS_CURSOR.get("last_block", 0.0)
    previous_at = METRICS_CURSOR.get("seen_at", 0.0)
    METRICS_CURSOR["last_block"] = float(last_block)
    METRICS_CURSOR["seen_at"] = now
    if previous_block <= 0 or previous_at <= 0:
        return 0.0
    elapsed = max(0.001, now - previous_at)
    return round(max(0.0, (float(last_block) - previous_block) / elapsed), 4)


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
