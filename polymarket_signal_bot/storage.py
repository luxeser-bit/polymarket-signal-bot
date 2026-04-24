from __future__ import annotations

import sqlite3
import time
import json
from pathlib import Path
from typing import Iterable

from .models import OrderBookSnapshot, PaperEvent, PaperPosition, Signal, Trade, Wallet, WalletScore


DEFAULT_DB_PATH = Path("data/polysignal.db")


class Store:
    def __init__(self, path: str | Path = DEFAULT_DB_PATH) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA foreign_keys = ON")

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY,
                username TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT 'manual',
                pnl REAL NOT NULL DEFAULT 0,
                volume REAL NOT NULL DEFAULT 0,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                proxy_wallet TEXT NOT NULL,
                side TEXT NOT NULL,
                asset TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                size REAL NOT NULL,
                price REAL NOT NULL,
                timestamp INTEGER NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                slug TEXT NOT NULL DEFAULT '',
                event_slug TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                outcome_index INTEGER NOT NULL DEFAULT 0,
                transaction_hash TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT 'data-api',
                notional REAL NOT NULL,
                inserted_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_wallet_time ON trades(proxy_wallet, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_trades_asset_time ON trades(asset, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_trades_wallet_notional ON trades(proxy_wallet, notional);
            CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp DESC);

            CREATE TABLE IF NOT EXISTS wallet_scores (
                wallet TEXT PRIMARY KEY,
                score REAL NOT NULL,
                computed_at INTEGER NOT NULL,
                trade_count INTEGER NOT NULL,
                buy_count INTEGER NOT NULL,
                sell_count INTEGER NOT NULL,
                total_notional REAL NOT NULL,
                avg_notional REAL NOT NULL,
                active_days INTEGER NOT NULL,
                market_count INTEGER NOT NULL,
                leaderboard_pnl REAL NOT NULL,
                leaderboard_volume REAL NOT NULL,
                pnl_efficiency REAL NOT NULL,
                reason TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                generated_at INTEGER NOT NULL,
                action TEXT NOT NULL,
                wallet TEXT NOT NULL,
                wallet_score REAL NOT NULL,
                condition_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                outcome TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                observed_price REAL NOT NULL,
                suggested_price REAL NOT NULL,
                size_usdc REAL NOT NULL,
                confidence REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                expires_at INTEGER NOT NULL,
                source_trade_id TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_signals_time ON signals(generated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_signals_asset ON signals(asset, generated_at DESC);

            CREATE TABLE IF NOT EXISTS paper_positions (
                position_id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                opened_at INTEGER NOT NULL,
                wallet TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                outcome TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                entry_price REAL NOT NULL,
                size_usdc REAL NOT NULL,
                shares REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                status TEXT NOT NULL,
                closed_at INTEGER,
                exit_price REAL,
                realized_pnl REAL NOT NULL DEFAULT 0,
                close_reason TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_paper_status ON paper_positions(status, opened_at DESC);

            CREATE TABLE IF NOT EXISTS paper_events (
                event_id TEXT PRIMARY KEY,
                event_at INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                signal_id TEXT NOT NULL DEFAULT '',
                position_id TEXT NOT NULL DEFAULT '',
                wallet TEXT NOT NULL DEFAULT '',
                condition_id TEXT NOT NULL DEFAULT '',
                asset TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                policy_mode TEXT NOT NULL DEFAULT '',
                cohort_status TEXT NOT NULL DEFAULT '',
                risk_status TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                price REAL NOT NULL DEFAULT 0,
                size_usdc REAL NOT NULL DEFAULT 0,
                pnl REAL NOT NULL DEFAULT 0,
                confidence REAL NOT NULL DEFAULT 0,
                wallet_score REAL NOT NULL DEFAULT 0,
                hold_seconds INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_paper_events_time
                ON paper_events(event_at DESC);
            CREATE INDEX IF NOT EXISTS idx_paper_events_type_reason
                ON paper_events(event_type, reason, event_at DESC);
            CREATE INDEX IF NOT EXISTS idx_paper_events_wallet
                ON paper_events(wallet, event_at DESC);
            CREATE INDEX IF NOT EXISTS idx_paper_events_asset
                ON paper_events(asset, event_at DESC);

            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alert_events (
                alert_key TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                destination TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                sent_at INTEGER,
                error TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_alert_events_signal ON alert_events(signal_id);

            CREATE TABLE IF NOT EXISTS order_books_latest (
                asset TEXT PRIMARY KEY,
                market TEXT NOT NULL DEFAULT '',
                timestamp INTEGER NOT NULL DEFAULT 0,
                best_bid REAL NOT NULL DEFAULT 0,
                best_ask REAL NOT NULL DEFAULT 0,
                mid REAL NOT NULL DEFAULT 0,
                spread REAL NOT NULL DEFAULT 1,
                bid_depth_usdc REAL NOT NULL DEFAULT 0,
                ask_depth_usdc REAL NOT NULL DEFAULT 0,
                liquidity_score REAL NOT NULL DEFAULT 0,
                last_trade_price REAL NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_order_books_liquidity
                ON order_books_latest(liquidity_score DESC, spread ASC);

            CREATE TABLE IF NOT EXISTS signal_reviews (
                signal_id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'PENDING',
                note TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_signal_reviews_status
                ON signal_reviews(status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS wallet_sync_state (
                wallet TEXT PRIMARY KEY,
                last_synced_at INTEGER NOT NULL DEFAULT 0,
                last_trade_ts INTEGER NOT NULL DEFAULT 0,
                total_trades_seen INTEGER NOT NULL DEFAULT 0,
                total_inserted INTEGER NOT NULL DEFAULT 0,
                pages_synced INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT ''
            );
            """
        )
        self._ensure_column("wallet_scores", "repeatability_score", "REAL NOT NULL DEFAULT 0")
        self._ensure_column("wallet_scores", "drawdown_score", "REAL NOT NULL DEFAULT 0")
        self._ensure_column("paper_positions", "close_reason", "TEXT NOT NULL DEFAULT ''")
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {row["name"] for row in rows}
        if column not in existing:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def upsert_wallets(self, wallets: Iterable[Wallet]) -> int:
        now = int(time.time())
        count = 0
        for wallet in wallets:
            if not wallet.address:
                continue
            self.conn.execute(
                """
                INSERT INTO wallets(address, username, source, pnl, volume, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address) DO UPDATE SET
                    username = excluded.username,
                    source = excluded.source,
                    pnl = excluded.pnl,
                    volume = excluded.volume,
                    last_seen = excluded.last_seen
                """,
                (
                    wallet.address,
                    wallet.username,
                    wallet.source,
                    wallet.pnl,
                    wallet.volume,
                    now,
                    now,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def insert_wallets_ignore_existing(self, wallets: Iterable[Wallet]) -> int:
        now = int(time.time())
        count = 0
        for wallet in wallets:
            if not wallet.address:
                continue
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO wallets(address, username, source, pnl, volume, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    wallet.address,
                    wallet.username,
                    wallet.source,
                    wallet.pnl,
                    wallet.volume,
                    now,
                    now,
                ),
            )
            count += cursor.rowcount
        self.conn.commit()
        return count

    def insert_trades(self, trades: Iterable[Trade]) -> int:
        now = int(time.time())
        count = 0
        for trade in trades:
            if not trade.trade_id or not trade.proxy_wallet or not trade.asset:
                continue
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO trades(
                    trade_id, proxy_wallet, side, asset, condition_id, size, price,
                    timestamp, title, slug, event_slug, outcome, outcome_index,
                    transaction_hash, source, notional, inserted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.trade_id,
                    trade.proxy_wallet,
                    trade.side,
                    trade.asset,
                    trade.condition_id,
                    trade.size,
                    trade.price,
                    trade.timestamp,
                    trade.title,
                    trade.slug,
                    trade.event_slug,
                    trade.outcome,
                    trade.outcome_index,
                    trade.transaction_hash,
                    trade.source,
                    trade.notional,
                    now,
                ),
            )
            count += cursor.rowcount
        self.conn.commit()
        return count

    def fetch_wallets(self, limit: int | None = None) -> list[Wallet]:
        sql = "SELECT address, username, source, pnl, volume FROM wallets ORDER BY volume DESC"
        params: tuple[object, ...] = ()
        if limit is not None:
            sql += " LIMIT ?"
            params = (limit,)
        return [
            Wallet(
                address=row["address"],
                username=row["username"],
                source=row["source"],
                pnl=row["pnl"],
                volume=row["volume"],
            )
            for row in self.conn.execute(sql, params)
        ]

    def discover_wallets_from_trades(
        self,
        *,
        limit: int = 1000,
        min_notional: float = 0.0,
        min_trades: int = 1,
        since_ts: int | None = None,
        source: str = "trade-flow",
        exclude_existing: bool = True,
    ) -> list[Wallet]:
        sql = """
            SELECT LOWER(t.proxy_wallet) AS wallet, COUNT(*) AS trades, SUM(t.notional) AS volume
            FROM trades t
            WHERE t.proxy_wallet != ''
        """
        params: list[object] = []
        if since_ts is not None:
            sql += " AND t.timestamp >= ?"
            params.append(since_ts)
        if exclude_existing:
            sql += """
                AND NOT EXISTS (
                    SELECT 1
                    FROM wallets w
                    WHERE LOWER(w.address) = LOWER(t.proxy_wallet)
                )
            """
        sql += """
            GROUP BY LOWER(t.proxy_wallet)
            HAVING SUM(t.notional) >= ? AND COUNT(*) >= ?
            ORDER BY volume DESC
            LIMIT ?
        """
        params.extend([min_notional, min_trades, limit])
        rows = self.conn.execute(sql, params)
        return [
            Wallet(
                address=row["wallet"],
                username="",
                source=source,
                pnl=0.0,
                volume=float(row["volume"] or 0.0),
            )
            for row in rows
        ]

    def wallet_sync_state(self, wallet: str) -> dict[str, object]:
        row = self.conn.execute(
            "SELECT * FROM wallet_sync_state WHERE wallet = ?",
            (wallet.lower(),),
        ).fetchone()
        if row is None:
            return {
                "wallet": wallet.lower(),
                "last_synced_at": 0,
                "last_trade_ts": 0,
                "total_trades_seen": 0,
                "total_inserted": 0,
                "pages_synced": 0,
                "last_error": "",
            }
        return dict(row)

    def update_wallet_sync_state(
        self,
        wallet: str,
        *,
        last_trade_ts: int,
        trades_seen: int,
        inserted: int,
        pages_synced: int,
        error: str = "",
    ) -> None:
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO wallet_sync_state(
                wallet, last_synced_at, last_trade_ts, total_trades_seen,
                total_inserted, pages_synced, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                last_synced_at = excluded.last_synced_at,
                last_trade_ts = MAX(wallet_sync_state.last_trade_ts, excluded.last_trade_ts),
                total_trades_seen = wallet_sync_state.total_trades_seen + excluded.total_trades_seen,
                total_inserted = wallet_sync_state.total_inserted + excluded.total_inserted,
                pages_synced = wallet_sync_state.pages_synced + excluded.pages_synced,
                last_error = excluded.last_error
            """,
            (
                wallet.lower(),
                now,
                last_trade_ts,
                trades_seen,
                inserted,
                pages_synced,
                error,
            ),
        )
        self.conn.commit()

    def sync_progress(self) -> dict[str, int]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS wallets_with_state,
                COALESCE(SUM(total_trades_seen), 0) AS trades_seen,
                COALESCE(SUM(total_inserted), 0) AS inserted,
                COALESCE(SUM(pages_synced), 0) AS pages_synced
            FROM wallet_sync_state
            """
        ).fetchone()
        return {
            "wallets_with_state": int(row["wallets_with_state"] or 0),
            "trades_seen": int(row["trades_seen"] or 0),
            "inserted": int(row["inserted"] or 0),
            "pages_synced": int(row["pages_synced"] or 0),
        }

    def fetch_trades_for_wallet(self, wallet: str, since_ts: int | None = None) -> list[Trade]:
        sql = "SELECT * FROM trades WHERE proxy_wallet = ?"
        params: list[object] = [wallet.lower()]
        if since_ts is not None:
            sql += " AND timestamp >= ?"
            params.append(since_ts)
        sql += " ORDER BY timestamp DESC"
        return [trade_from_row(row) for row in self.conn.execute(sql, params)]

    def fetch_recent_trades(
        self,
        *,
        since_ts: int | None = None,
        min_notional: float = 0.0,
        limit: int = 1000,
    ) -> list[Trade]:
        sql = "SELECT * FROM trades WHERE notional >= ?"
        params: list[object] = [min_notional]
        if since_ts is not None:
            sql += " AND timestamp >= ?"
            params.append(since_ts)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        return [trade_from_row(row) for row in self.conn.execute(sql, params)]

    def fetch_trades_chronological(
        self,
        *,
        since_ts: int | None = None,
        limit: int = 100000,
    ) -> list[Trade]:
        sql = "SELECT * FROM trades WHERE timestamp > 0"
        params: list[object] = []
        if since_ts is not None:
            sql += " AND timestamp >= ?"
            params.append(since_ts)
        sql += " ORDER BY timestamp ASC LIMIT ?"
        params.append(limit)
        return [trade_from_row(row) for row in self.conn.execute(sql, params)]

    def latest_trade_price(self, asset: str) -> float | None:
        row = self.conn.execute(
            "SELECT price FROM trades WHERE asset = ? ORDER BY timestamp DESC LIMIT 1",
            (asset,),
        ).fetchone()
        return None if row is None else float(row["price"])

    def latest_trade_mark(self, asset: str) -> dict[str, float | int] | None:
        row = self.conn.execute(
            "SELECT price, timestamp FROM trades WHERE asset = ? ORDER BY timestamp DESC LIMIT 1",
            (asset,),
        ).fetchone()
        if row is None:
            return None
        return {"price": float(row["price"]), "timestamp": int(row["timestamp"] or 0)}

    def recent_assets(self, limit: int = 50, since_ts: int | None = None) -> list[str]:
        sql = "SELECT asset, MAX(timestamp) AS last_seen FROM trades WHERE asset != ''"
        params: list[object] = []
        if since_ts is not None:
            sql += " AND timestamp >= ?"
            params.append(since_ts)
        sql += " GROUP BY asset ORDER BY last_seen DESC LIMIT ?"
        params.append(limit)
        return [str(row["asset"]) for row in self.conn.execute(sql, params)]

    def upsert_order_books(
        self,
        books: Iterable[OrderBookSnapshot],
        *,
        raw_by_asset: dict[str, object] | None = None,
    ) -> int:
        now = int(time.time())
        count = 0
        raw_by_asset = raw_by_asset or {}
        for book in books:
            if not book.asset:
                continue
            raw_json = book.raw_json
            if not raw_json and book.asset in raw_by_asset:
                raw_json = json.dumps(raw_by_asset[book.asset], ensure_ascii=False, separators=(",", ":"))
            self.conn.execute(
                """
                INSERT INTO order_books_latest(
                    asset, market, timestamp, best_bid, best_ask, mid, spread,
                    bid_depth_usdc, ask_depth_usdc, liquidity_score,
                    last_trade_price, raw_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset) DO UPDATE SET
                    market = excluded.market,
                    timestamp = excluded.timestamp,
                    best_bid = excluded.best_bid,
                    best_ask = excluded.best_ask,
                    mid = excluded.mid,
                    spread = excluded.spread,
                    bid_depth_usdc = excluded.bid_depth_usdc,
                    ask_depth_usdc = excluded.ask_depth_usdc,
                    liquidity_score = excluded.liquidity_score,
                    last_trade_price = excluded.last_trade_price,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at
                """,
                (
                    book.asset,
                    book.market,
                    book.timestamp,
                    book.best_bid,
                    book.best_ask,
                    book.mid,
                    book.spread,
                    book.bid_depth_usdc,
                    book.ask_depth_usdc,
                    book.liquidity_score,
                    book.last_trade_price,
                    raw_json,
                    now,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def fetch_order_books(self, assets: list[str] | None = None, limit: int = 50) -> dict[str, OrderBookSnapshot]:
        if assets:
            unique_assets = list(dict.fromkeys(asset for asset in assets if asset))
            fetched: dict[str, OrderBookSnapshot] = {}
            for index in range(0, len(unique_assets), 500):
                chunk = unique_assets[index : index + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows = self.conn.execute(
                    f"SELECT * FROM order_books_latest WHERE asset IN ({placeholders})",
                    tuple(chunk),
                )
                fetched.update({row["asset"]: order_book_from_row(row) for row in rows})
            return fetched
        else:
            rows = self.conn.execute(
                """
                SELECT * FROM order_books_latest
                ORDER BY liquidity_score DESC, updated_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        return {row["asset"]: order_book_from_row(row) for row in rows}

    def latest_order_book(self, asset: str) -> OrderBookSnapshot | None:
        row = self.conn.execute(
            "SELECT * FROM order_books_latest WHERE asset = ?",
            (asset,),
        ).fetchone()
        return None if row is None else order_book_from_row(row)

    def upsert_scores(self, scores: Iterable[WalletScore]) -> int:
        count = 0
        for score in scores:
            self.conn.execute(
                """
                INSERT INTO wallet_scores(
                    wallet, score, computed_at, trade_count, buy_count, sell_count,
                    total_notional, avg_notional, active_days, market_count,
                    leaderboard_pnl, leaderboard_volume, pnl_efficiency, reason,
                    repeatability_score, drawdown_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    score = excluded.score,
                    computed_at = excluded.computed_at,
                    trade_count = excluded.trade_count,
                    buy_count = excluded.buy_count,
                    sell_count = excluded.sell_count,
                    total_notional = excluded.total_notional,
                    avg_notional = excluded.avg_notional,
                    active_days = excluded.active_days,
                    market_count = excluded.market_count,
                    leaderboard_pnl = excluded.leaderboard_pnl,
                    leaderboard_volume = excluded.leaderboard_volume,
                    pnl_efficiency = excluded.pnl_efficiency,
                    reason = excluded.reason,
                    repeatability_score = excluded.repeatability_score,
                    drawdown_score = excluded.drawdown_score
                """,
                (
                    score.wallet,
                    score.score,
                    score.computed_at,
                    score.trade_count,
                    score.buy_count,
                    score.sell_count,
                    score.total_notional,
                    score.avg_notional,
                    score.active_days,
                    score.market_count,
                    score.leaderboard_pnl,
                    score.leaderboard_volume,
                    score.pnl_efficiency,
                    score.reason,
                    score.repeatability_score,
                    score.drawdown_score,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def fetch_scores(self, min_score: float = 0.0) -> dict[str, WalletScore]:
        rows = self.conn.execute(
            "SELECT * FROM wallet_scores WHERE score >= ? ORDER BY score DESC",
            (min_score,),
        )
        return {row["wallet"]: score_from_row(row) for row in rows}

    def insert_signals(self, signals: Iterable[Signal]) -> int:
        count = 0
        for signal in signals:
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO signals(
                    signal_id, generated_at, action, wallet, wallet_score, condition_id,
                    asset, outcome, title, observed_price, suggested_price, size_usdc,
                    confidence, stop_loss, take_profit, expires_at, source_trade_id, reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
                    signal.generated_at,
                    signal.action,
                    signal.wallet,
                    signal.wallet_score,
                    signal.condition_id,
                    signal.asset,
                    signal.outcome,
                    signal.title,
                    signal.observed_price,
                    signal.suggested_price,
                    signal.size_usdc,
                    signal.confidence,
                    signal.stop_loss,
                    signal.take_profit,
                    signal.expires_at,
                    signal.source_trade_id,
                    signal.reason,
                ),
            )
            if cursor.rowcount:
                now = int(time.time())
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO signal_reviews(signal_id, status, note, created_at, updated_at)
                    VALUES (?, 'PENDING', '', ?, ?)
                    """,
                    (signal.signal_id, now, now),
                )
            count += cursor.rowcount
        self.conn.commit()
        return count

    def fetch_recent_signals(self, limit: int = 25) -> list[Signal]:
        rows = self.conn.execute("SELECT * FROM signals ORDER BY generated_at DESC LIMIT ?", (limit,))
        return [signal_from_row(row) for row in rows]

    def fetch_review_queue(self, status: str = "PENDING", limit: int = 25) -> list[dict[str, object]]:
        rows = self.conn.execute(
            """
            SELECT
                s.signal_id, s.generated_at, s.action, s.wallet, s.wallet_score,
                s.asset, s.outcome, s.title, s.suggested_price, s.size_usdc,
                s.confidence, s.reason, r.status, r.note, r.updated_at,
                CASE
                    WHEN s.reason LIKE '%cohort=STABLE%' THEN 10
                    WHEN s.reason LIKE '%cohort=CANDIDATE%' THEN 30
                    WHEN s.reason LIKE '%cohort=UNKNOWN%' THEN 65
                    WHEN s.reason LIKE '%cohort=WATCH%' THEN 70
                    WHEN s.reason LIKE '%cohort=NOISE%' THEN 90
                    ELSE 50
                END AS priority,
                CASE
                    WHEN s.reason LIKE '%cohort=STABLE%' THEN 'STABLE'
                    WHEN s.reason LIKE '%cohort=CANDIDATE%' THEN 'CANDIDATE'
                    WHEN s.reason LIKE '%cohort=UNKNOWN%' THEN 'UNKNOWN'
                    WHEN s.reason LIKE '%cohort=WATCH%' THEN 'WATCH'
                    WHEN s.reason LIKE '%cohort=NOISE%' THEN 'NOISE'
                    ELSE ''
                END AS cohort_status
            FROM signal_reviews r
            JOIN signals s ON s.signal_id = r.signal_id
            WHERE r.status = ?
            ORDER BY priority ASC, s.confidence DESC, s.generated_at DESC
            LIMIT ?
            """,
            (status, limit),
        )
        return [dict(row) for row in rows]

    def set_signal_review_status(self, signal_id: str, status: str, note: str = "") -> bool:
        now = int(time.time())
        cursor = self.conn.execute(
            """
            UPDATE signal_reviews
            SET status = ?, note = ?, updated_at = ?
            WHERE signal_id = ?
            """,
            (status, note, now, signal_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def fetch_approved_unopened_signals(self, limit: int = 25) -> list[Signal]:
        rows = self.conn.execute(
            """
            SELECT s.*
            FROM signals s
            JOIN signal_reviews r ON r.signal_id = s.signal_id
            LEFT JOIN paper_positions p ON p.signal_id = s.signal_id
            WHERE r.status = 'APPROVED' AND p.signal_id IS NULL
            ORDER BY
                CASE
                    WHEN s.reason LIKE '%cohort=STABLE%' THEN 10
                    WHEN s.reason LIKE '%cohort=CANDIDATE%' THEN 30
                    WHEN s.reason LIKE '%cohort=UNKNOWN%' THEN 65
                    WHEN s.reason LIKE '%cohort=WATCH%' THEN 70
                    WHEN s.reason LIKE '%cohort=NOISE%' THEN 90
                    ELSE 50
                END ASC,
                r.updated_at ASC
            LIMIT ?
            """,
            (limit,),
        )
        return [signal_from_row(row) for row in rows]

    def fetch_unsent_signals_for_destination(self, destination: str, limit: int = 25) -> list[Signal]:
        rows = self.conn.execute(
            """
            SELECT s.*
            FROM signals s
            LEFT JOIN alert_events a
                ON a.signal_id = s.signal_id AND a.destination = ?
            WHERE a.signal_id IS NULL
            ORDER BY s.generated_at DESC
            LIMIT ?
            """,
            (destination, limit),
        )
        return [signal_from_row(row) for row in rows]

    def record_alert_event(
        self,
        *,
        signal_id: str,
        destination: str,
        status: str,
        message: str,
        error: str = "",
        sent_at: int | None = None,
    ) -> bool:
        now = int(time.time())
        alert_key = f"{destination}:{signal_id}"
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO alert_events(
                alert_key, signal_id, destination, status, message, created_at, sent_at, error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (alert_key, signal_id, destination, status, message, now, sent_at, error),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def update_alert_event_status(
        self,
        *,
        signal_id: str,
        destination: str,
        status: str,
        error: str = "",
        sent_at: int | None = None,
    ) -> None:
        if sent_at is None and status == "SENT":
            sent_at = int(time.time())
        self.conn.execute(
            """
            UPDATE alert_events
            SET status = ?, error = ?, sent_at = COALESCE(?, sent_at)
            WHERE alert_key = ?
            """,
            (status, error, sent_at, f"{destination}:{signal_id}"),
        )
        self.conn.commit()

    def fetch_recent_alerts(self, limit: int = 10) -> list[dict[str, object]]:
        rows = self.conn.execute(
            """
            SELECT signal_id, destination, status, message, created_at, sent_at, error
            FROM alert_events
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]

    def set_runtime_state(self, key: str, value: str) -> None:
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO runtime_state(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )
        self.conn.commit()

    def runtime_state(self) -> dict[str, dict[str, object]]:
        rows = self.conn.execute("SELECT key, value, updated_at FROM runtime_state")
        return {
            row["key"]: {"value": row["value"], "updated_at": row["updated_at"]}
            for row in rows
        }

    def has_open_position_for_asset(self, asset: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM paper_positions WHERE asset = ? AND status = 'OPEN' LIMIT 1",
            (asset,),
        ).fetchone()
        return row is not None

    def insert_paper_positions(self, positions: Iterable[PaperPosition]) -> int:
        count = 0
        for position in positions:
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO paper_positions(
                    position_id, signal_id, opened_at, wallet, condition_id, asset,
                    outcome, title, entry_price, size_usdc, shares, stop_loss,
                    take_profit, status, closed_at, exit_price, realized_pnl, close_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.position_id,
                    position.signal_id,
                    position.opened_at,
                    position.wallet,
                    position.condition_id,
                    position.asset,
                    position.outcome,
                    position.title,
                    position.entry_price,
                    position.size_usdc,
                    position.shares,
                    position.stop_loss,
                    position.take_profit,
                    position.status,
                    position.closed_at,
                    position.exit_price,
                    position.realized_pnl,
                    position.close_reason,
                ),
            )
            count += cursor.rowcount
        self.conn.commit()
        return count

    def fetch_open_positions(self) -> list[PaperPosition]:
        rows = self.conn.execute(
            "SELECT * FROM paper_positions WHERE status = 'OPEN' ORDER BY opened_at DESC"
        )
        return [paper_position_from_row(row) for row in rows]

    def close_position(
        self,
        position_id: str,
        closed_at: int,
        exit_price: float,
        realized_pnl: float,
        close_reason: str = "",
    ) -> None:
        self.conn.execute(
            """
            UPDATE paper_positions
            SET status = 'CLOSED', closed_at = ?, exit_price = ?, realized_pnl = ?, close_reason = ?
            WHERE position_id = ?
            """,
            (closed_at, exit_price, realized_pnl, close_reason, position_id),
        )
        self.conn.commit()

    def fetch_recent_closed_positions(self, limit: int = 10) -> list[PaperPosition]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM paper_positions
            WHERE status = 'CLOSED'
            ORDER BY COALESCE(closed_at, opened_at) DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [paper_position_from_row(row) for row in rows]

    def paper_summary(self) -> dict[str, float]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total_positions,
                SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_positions,
                COALESCE(SUM(CASE WHEN status = 'OPEN' THEN size_usdc ELSE 0 END), 0) AS open_cost,
                COALESCE(SUM(realized_pnl), 0) AS realized_pnl
            FROM paper_positions
            """
        ).fetchone()
        return {
            "total_positions": float(row["total_positions"] or 0),
            "open_positions": float(row["open_positions"] or 0),
            "open_cost": float(row["open_cost"] or 0),
            "realized_pnl": float(row["realized_pnl"] or 0),
        }

    def realized_pnl_since(self, since_ts: int) -> float:
        row = self.conn.execute(
            """
            SELECT COALESCE(SUM(realized_pnl), 0) AS realized_pnl
            FROM paper_positions
            WHERE status = 'CLOSED' AND closed_at IS NOT NULL AND closed_at >= ?
            """,
            (since_ts,),
        ).fetchone()
        return float(row["realized_pnl"] or 0.0)

    def insert_paper_events(self, events: Iterable[PaperEvent]) -> int:
        count = 0
        for event in events:
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO paper_events(
                    event_id, event_at, event_type, signal_id, position_id, wallet,
                    condition_id, asset, outcome, title, policy_mode, cohort_status,
                    risk_status, reason, price, size_usdc, pnl, confidence,
                    wallet_score, hold_seconds, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.event_at,
                    event.event_type,
                    event.signal_id,
                    event.position_id,
                    event.wallet,
                    event.condition_id,
                    event.asset,
                    event.outcome,
                    event.title,
                    event.policy_mode,
                    event.cohort_status,
                    event.risk_status,
                    event.reason,
                    event.price,
                    event.size_usdc,
                    event.pnl,
                    event.confidence,
                    event.wallet_score,
                    event.hold_seconds,
                    event.metadata_json,
                ),
            )
            count += cursor.rowcount
        self.conn.commit()
        return count

    def fetch_recent_paper_events(
        self,
        limit: int = 20,
        *,
        since_ts: int | None = None,
    ) -> list[PaperEvent]:
        params: list[object] = []
        where = ""
        if since_ts is not None:
            where = "WHERE event_at >= ?"
            params.append(since_ts)
        params.append(limit)
        rows = self.conn.execute(
            f"""
            SELECT *
            FROM paper_events
            {where}
            ORDER BY event_at DESC,
                CASE event_type
                    WHEN 'CLOSED' THEN 1
                    WHEN 'BLOCKED' THEN 2
                    WHEN 'OPENED' THEN 3
                    WHEN 'SIGNAL_CREATED' THEN 4
                    ELSE 5
                END ASC
            LIMIT ?
            """,
            tuple(params),
        )
        return [paper_event_from_row(row) for row in rows]

    def paper_event_summary(
        self,
        *,
        since_ts: int | None = None,
        limit: int = 20,
    ) -> dict[str, object]:
        params: list[object] = []
        where = ""
        if since_ts is not None:
            where = "WHERE event_at >= ?"
            params.append(since_ts)
        total = self.conn.execute(
            f"""
            SELECT COUNT(*) AS events,
                   COALESCE(SUM(pnl), 0) AS pnl,
                   COALESCE(AVG(confidence), 0) AS avg_confidence
            FROM paper_events
            {where}
            """,
            tuple(params),
        ).fetchone()
        by_reason = self.conn.execute(
            f"""
            SELECT event_type,
                   COALESCE(NULLIF(reason, ''), 'none') AS reason,
                   COUNT(*) AS events,
                   COALESCE(SUM(pnl), 0) AS pnl,
                   COALESCE(AVG(confidence), 0) AS avg_confidence
            FROM paper_events
            {where}
            GROUP BY event_type, COALESCE(NULLIF(reason, ''), 'none')
            ORDER BY events DESC, event_type ASC, reason ASC
            LIMIT ?
            """,
            tuple([*params, limit]),
        )
        by_wallet = self.conn.execute(
            f"""
            SELECT wallet,
                   COUNT(*) AS events,
                   COALESCE(SUM(pnl), 0) AS pnl,
                   COALESCE(AVG(wallet_score), 0) AS avg_wallet_score
            FROM paper_events
            {where}
            WHERE wallet != ''
            GROUP BY wallet
            ORDER BY events DESC, pnl DESC
            LIMIT ?
            """
            if not where
            else f"""
            SELECT wallet,
                   COUNT(*) AS events,
                   COALESCE(SUM(pnl), 0) AS pnl,
                   COALESCE(AVG(wallet_score), 0) AS avg_wallet_score
            FROM paper_events
            {where} AND wallet != ''
            GROUP BY wallet
            ORDER BY events DESC, pnl DESC
            LIMIT ?
            """,
            tuple([*params, limit]),
        )
        return {
            "total_events": int(total["events"] or 0),
            "pnl": float(total["pnl"] or 0.0),
            "avg_confidence": float(total["avg_confidence"] or 0.0),
            "by_reason": [dict(row) for row in by_reason],
            "by_wallet": [dict(row) for row in by_wallet],
            "recent": self.fetch_recent_paper_events(limit=limit, since_ts=since_ts),
        }


def trade_from_row(row: sqlite3.Row) -> Trade:
    return Trade(
        trade_id=row["trade_id"],
        proxy_wallet=row["proxy_wallet"],
        side=row["side"],
        asset=row["asset"],
        condition_id=row["condition_id"],
        size=row["size"],
        price=row["price"],
        timestamp=row["timestamp"],
        title=row["title"],
        slug=row["slug"],
        event_slug=row["event_slug"],
        outcome=row["outcome"],
        outcome_index=row["outcome_index"],
        transaction_hash=row["transaction_hash"],
        source=row["source"],
    )


def score_from_row(row: sqlite3.Row) -> WalletScore:
    return WalletScore(
        wallet=row["wallet"],
        score=row["score"],
        computed_at=row["computed_at"],
        trade_count=row["trade_count"],
        buy_count=row["buy_count"],
        sell_count=row["sell_count"],
        total_notional=row["total_notional"],
        avg_notional=row["avg_notional"],
        active_days=row["active_days"],
        market_count=row["market_count"],
        leaderboard_pnl=row["leaderboard_pnl"],
        leaderboard_volume=row["leaderboard_volume"],
        pnl_efficiency=row["pnl_efficiency"],
        reason=row["reason"],
        repeatability_score=row["repeatability_score"],
        drawdown_score=row["drawdown_score"],
    )


def signal_from_row(row: sqlite3.Row) -> Signal:
    return Signal(
        signal_id=row["signal_id"],
        generated_at=row["generated_at"],
        action=row["action"],
        wallet=row["wallet"],
        wallet_score=row["wallet_score"],
        condition_id=row["condition_id"],
        asset=row["asset"],
        outcome=row["outcome"],
        title=row["title"],
        observed_price=row["observed_price"],
        suggested_price=row["suggested_price"],
        size_usdc=row["size_usdc"],
        confidence=row["confidence"],
        stop_loss=row["stop_loss"],
        take_profit=row["take_profit"],
        expires_at=row["expires_at"],
        source_trade_id=row["source_trade_id"],
        reason=row["reason"],
    )


def paper_position_from_row(row: sqlite3.Row) -> PaperPosition:
    return PaperPosition(
        position_id=row["position_id"],
        signal_id=row["signal_id"],
        opened_at=row["opened_at"],
        wallet=row["wallet"],
        condition_id=row["condition_id"],
        asset=row["asset"],
        outcome=row["outcome"],
        title=row["title"],
        entry_price=row["entry_price"],
        size_usdc=row["size_usdc"],
        shares=row["shares"],
        stop_loss=row["stop_loss"],
        take_profit=row["take_profit"],
        status=row["status"],
        closed_at=row["closed_at"],
        exit_price=row["exit_price"],
        realized_pnl=row["realized_pnl"],
        close_reason=row["close_reason"],
    )


def paper_event_from_row(row: sqlite3.Row) -> PaperEvent:
    return PaperEvent(
        event_id=row["event_id"],
        event_at=row["event_at"],
        event_type=row["event_type"],
        signal_id=row["signal_id"],
        position_id=row["position_id"],
        wallet=row["wallet"],
        condition_id=row["condition_id"],
        asset=row["asset"],
        outcome=row["outcome"],
        title=row["title"],
        policy_mode=row["policy_mode"],
        cohort_status=row["cohort_status"],
        risk_status=row["risk_status"],
        reason=row["reason"],
        price=row["price"],
        size_usdc=row["size_usdc"],
        pnl=row["pnl"],
        confidence=row["confidence"],
        wallet_score=row["wallet_score"],
        hold_seconds=row["hold_seconds"],
        metadata_json=row["metadata_json"],
    )


def order_book_from_row(row: sqlite3.Row) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        asset=row["asset"],
        market=row["market"],
        timestamp=row["timestamp"],
        best_bid=row["best_bid"],
        best_ask=row["best_ask"],
        mid=row["mid"],
        spread=row["spread"],
        bid_depth_usdc=row["bid_depth_usdc"],
        ask_depth_usdc=row["ask_depth_usdc"],
        liquidity_score=row["liquidity_score"],
        last_trade_price=row["last_trade_price"],
        raw_json=row["raw_json"],
    )
