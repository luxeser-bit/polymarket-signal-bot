from __future__ import annotations

"""SQLite maintenance helper for the Polygon indexer database.

This script is intended for manual or scheduled maintenance. For the safest
run, stop the indexer first, run this script, then start the indexer again.
"""

import argparse
import sqlite3
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "indexer.db"
DEFAULT_JOURNAL_SIZE_LIMIT = 20_000_000_000


def format_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(max(0, value))
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def database_files(db_path: Path) -> list[Path]:
    return [
        db_path,
        db_path.with_name(f"{db_path.name}-wal"),
        db_path.with_name(f"{db_path.name}-shm"),
    ]


def print_file_sizes(db_path: Path, *, title: str) -> None:
    print(title)
    for path in database_files(db_path):
        size = file_size(path)
        exists = "yes" if path.exists() else "no"
        print(f"  {path.name}: exists={exists} size={format_bytes(size)} bytes={size}")


def run_maintenance(db_path: Path, *, journal_size_limit: int, timeout_seconds: float) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    print(f"database: {db_path}")
    print(f"journal_size_limit: {journal_size_limit} bytes ({format_bytes(journal_size_limit)})")
    print_file_sizes(db_path, title="before:")

    start = time.time()
    conn = sqlite3.connect(db_path, timeout=timeout_seconds)
    try:
        conn.execute(f"PRAGMA busy_timeout={int(timeout_seconds * 1000)}")
        journal_limit_row = conn.execute(f"PRAGMA journal_size_limit={int(journal_size_limit)}").fetchone()
        checkpoint_rows = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        conn.commit()
    finally:
        conn.close()

    elapsed = time.time() - start
    print(f"journal_size_limit_result: {journal_limit_row}")
    print(f"wal_checkpoint_truncate_result: {checkpoint_rows}")
    print(f"elapsed_seconds: {elapsed:.2f}")
    print_file_sizes(db_path, title="after:")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run SQLite WAL maintenance for the indexer database.")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database. Default: {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--journal-size-limit",
        type=int,
        default=DEFAULT_JOURNAL_SIZE_LIMIT,
        help=(
            "SQLite journal_size_limit in bytes. Default is 20,000,000,000 "
            "bytes, approximately 20 GB. Use 20000000 for approximately 20 MB."
        ),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=300.0,
        help="SQLite busy timeout while waiting for writers/readers. Default: 300.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        run_maintenance(
            args.db.resolve(),
            journal_size_limit=max(0, int(args.journal_size_limit)),
            timeout_seconds=max(1.0, float(args.timeout_seconds)),
        )
    except Exception as exc:  # noqa: BLE001 - CLI should print clear operational errors.
        print(f"maintenance_failed: {type(exc).__name__}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
