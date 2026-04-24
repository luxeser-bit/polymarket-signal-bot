from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


class ApiError(RuntimeError):
    """Raised when a Polymarket API request fails."""


@dataclass(frozen=True)
class ApiConfig:
    data_base: str = "https://data-api.polymarket.com"
    gamma_base: str = "https://gamma-api.polymarket.com"
    clob_base: str = "https://clob.polymarket.com"
    timeout_seconds: int = 20
    user_agent: str = "polymarket-signal-bot/0.1"
    min_delay_seconds: float = 0.15


class PolymarketClient:
    def __init__(self, config: ApiConfig | None = None) -> None:
        self.config = config or ApiConfig()
        self._last_request_at = 0.0

    def leaderboard(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        category: str = "OVERALL",
        time_period: str = "WEEK",
        order_by: str = "PNL",
    ) -> list[dict[str, Any]]:
        params = {
            "limit": limit,
            "offset": offset,
            "category": category,
            "timePeriod": time_period,
            "orderBy": order_by,
        }
        return self._get(f"{self.config.data_base}/v1/leaderboard", params)

    def user_activity(
        self,
        user: str,
        *,
        limit: int = 500,
        offset: int = 0,
        start: int | None = None,
        end: int | None = None,
        types: list[str] | None = None,
        side: str | None = None,
        sort_direction: str = "DESC",
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "user": user,
            "limit": limit,
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": sort_direction,
        }
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end
        if types:
            params["type"] = ",".join(types)
        if side:
            params["side"] = side
        return self._get(f"{self.config.data_base}/activity", params)

    def trades(
        self,
        *,
        user: str | None = None,
        market: str | list[str] | None = None,
        limit: int = 500,
        offset: int = 0,
        side: str | None = None,
        taker_only: bool = True,
        filter_type: str | None = None,
        filter_amount: float | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "takerOnly": str(taker_only).lower(),
        }
        if user:
            params["user"] = user
        if market:
            params["market"] = market
        if side:
            params["side"] = side
        if filter_type and filter_amount is not None:
            params["filterType"] = filter_type
            params["filterAmount"] = filter_amount
        return self._get(f"{self.config.data_base}/trades", params)

    def positions(
        self,
        user: str,
        *,
        limit: int = 500,
        offset: int = 0,
        size_threshold: float = 1.0,
    ) -> list[dict[str, Any]]:
        params = {
            "user": user,
            "limit": limit,
            "offset": offset,
            "sizeThreshold": size_threshold,
        }
        return self._get(f"{self.config.data_base}/positions", params)

    def markets(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool = False,
        order: str = "volume24hr",
        ascending: bool = False,
        volume_num_min: float | None = None,
        liquidity_num_min: float | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "closed": str(closed).lower(),
            "order": order,
            "ascending": str(ascending).lower(),
        }
        if volume_num_min is not None:
            params["volume_num_min"] = volume_num_min
        if liquidity_num_min is not None:
            params["liquidity_num_min"] = liquidity_num_min
        return self._get(f"{self.config.gamma_base}/markets", params)

    def market_price(self, token_id: str, side: str) -> float | None:
        params = {"token_id": token_id, "side": side.upper()}
        payload = self._get(f"{self.config.clob_base}/price", params)
        return _to_optional_float(payload.get("price"))

    def midpoint(self, token_id: str) -> float | None:
        payload = self._get(f"{self.config.clob_base}/midpoint", {"token_id": token_id})
        return _to_optional_float(payload.get("mid_price"))

    def order_book(self, token_id: str) -> dict[str, Any]:
        return self._get(f"{self.config.clob_base}/book", {"token_id": token_id})

    def order_books(self, token_ids: list[str]) -> list[dict[str, Any]]:
        if not token_ids:
            return []
        payload = [{"token_id": token_id} for token_id in token_ids]
        response = self._post(f"{self.config.clob_base}/books", payload)
        if not isinstance(response, list):
            raise ApiError("Unexpected /books response shape")
        return response

    def batch_prices(self, requests: list[dict[str, str]]) -> dict[str, dict[str, float]]:
        payload = self._post(f"{self.config.clob_base}/prices", requests)
        if not isinstance(payload, dict):
            raise ApiError("Unexpected /prices response shape")
        return payload

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        if params:
            query = urllib.parse.urlencode(params, doseq=True)
            url = f"{url}?{query}"
        return self._request("GET", url)

    def _post(self, url: str, payload: Any) -> Any:
        body = json.dumps(payload).encode("utf-8")
        return self._request("POST", url, body=body)

    def _request(self, method: str, url: str, body: bytes | None = None) -> Any:
        self._respect_delay()
        headers = {
            "Accept": "application/json",
            "User-Agent": self.config.user_agent,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.config.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            raise ApiError(f"HTTP {exc.code} for {url}: {text[:300]}") from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"Network error for {url}: {exc}") from exc
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ApiError(f"Non-JSON response for {url}: {raw[:300]}") from exc

    def _respect_delay(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        delay = self.config.min_delay_seconds - elapsed
        if delay > 0:
            time.sleep(delay)
        self._last_request_at = time.monotonic()


def _to_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
