from __future__ import annotations

import time
from dataclasses import dataclass

from .api import ApiError, PolymarketClient
from .models import Trade, Wallet
from .storage import Store


TARGET_TRADES = 86_000_000
TARGET_WALLETS = 14_000


@dataclass(frozen=True)
class BulkSyncConfig:
    wallet_limit: int = 100
    page_size: int = 500
    max_pages_per_wallet: int = 2
    days: int = 30
    overlap_seconds: int = 3600
    sleep_seconds: float = 0.15
    stop_on_error: bool = False


class BulkSync:
    def __init__(
        self,
        *,
        store: Store,
        client: PolymarketClient | None = None,
        config: BulkSyncConfig | None = None,
    ) -> None:
        self.store = store
        self.client = client or PolymarketClient()
        self.config = config or BulkSyncConfig()

    def run_once(self) -> dict[str, int]:
        self.store.init_schema()
        wallets = self.store.fetch_wallets(limit=self.config.wallet_limit)
        total_seen = 0
        total_inserted = 0
        total_pages = 0
        total_errors = 0
        for index, wallet in enumerate(wallets, start=1):
            try:
                result = self.sync_wallet(wallet)
                total_seen += result["seen"]
                total_inserted += result["inserted"]
                total_pages += result["pages"]
                self.store.set_runtime_state(
                    "bulk_last_wallet",
                    f"{index}/{len(wallets)} {wallet.address} inserted={result['inserted']}",
                )
                print(
                    f"[{index}/{len(wallets)}] {wallet.address} "
                    f"seen={result['seen']} inserted={result['inserted']} pages={result['pages']}"
                )
            except ApiError as exc:
                total_errors += 1
                self.store.update_wallet_sync_state(
                    wallet.address,
                    last_trade_ts=0,
                    trades_seen=0,
                    inserted=0,
                    pages_synced=0,
                    error=str(exc)[:300],
                )
                if self.config.stop_on_error:
                    raise
                print(f"[{index}/{len(wallets)}] {wallet.address} error={str(exc)[:120]}")
        summary = {
            "wallets": len(wallets),
            "seen": total_seen,
            "inserted": total_inserted,
            "pages": total_pages,
            "errors": total_errors,
        }
        self.store.set_runtime_state(
            "bulk_last_summary",
            (
                f"wallets={summary['wallets']} seen={summary['seen']} "
                f"inserted={summary['inserted']} pages={summary['pages']} errors={summary['errors']}"
            ),
        )
        return summary

    def sync_wallet(self, wallet: Wallet) -> dict[str, int]:
        state = self.store.wallet_sync_state(wallet.address)
        now = int(time.time())
        start = now - max(1, self.config.days) * 86400
        last_trade_ts = int(state.get("last_trade_ts") or 0)
        if last_trade_ts > 0:
            start = max(start, last_trade_ts - self.config.overlap_seconds)

        total_seen = 0
        total_inserted = 0
        pages = 0
        max_trade_ts = last_trade_ts
        for page in range(self.config.max_pages_per_wallet):
            rows = self.client.user_activity(
                wallet.address,
                limit=min(500, self.config.page_size),
                offset=page * self.config.page_size,
                start=start,
                types=["TRADE"],
            )
            if not rows:
                break
            trades = [Trade.from_api(row, source="bulk-sync") for row in rows]
            total_seen += len(trades)
            total_inserted += self.store.insert_trades(trades)
            pages += 1
            for trade in trades:
                max_trade_ts = max(max_trade_ts, trade.timestamp)
            if len(rows) < self.config.page_size:
                break
            if self.config.sleep_seconds > 0:
                time.sleep(self.config.sleep_seconds)

        self.store.update_wallet_sync_state(
            wallet.address,
            last_trade_ts=max_trade_ts,
            trades_seen=total_seen,
            inserted=total_inserted,
            pages_synced=pages,
        )
        return {"seen": total_seen, "inserted": total_inserted, "pages": pages}
