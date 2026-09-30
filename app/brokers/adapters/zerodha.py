"""Zerodha (Kite Connect v3). The only adapter with a live implementation.

Auth flow: user logs in at https://kite.zerodha.com/connect/login?v=3&api_key=KEY,
Kite redirects back with a one-time `request_token`. We exchange it at
POST /session/token with checksum = SHA256(api_key + request_token + api_secret)
and receive an `access_token` (valid until ~6 AM next day; it cannot be refreshed).
"""
from __future__ import annotations

import hashlib
from typing import Optional

from app.brokers.registry import register_broker
from app.brokers.simulated import SimulatedBrokerAdapter
from app.core.exceptions import (AuthenticationError, OrderRejectedError, RateLimitError,
                                 TransientBrokerError)
from app.domain.models import BrokerOrder, BrokerOrderStatus

BASE = "https://api.kite.trade"


@register_broker
class ZerodhaAdapter(SimulatedBrokerAdapter):
    name = "zerodha"
    required_credentials = ("api_key", "api_secret", "request_token")
    rate_limit_per_sec = 10.0          # Kite: 10 orders/sec

    def native_order(self, o: BrokerOrder) -> dict:
        return {
            "tradingsymbol": o.symbol, "exchange": o.exchange,
            "transaction_type": o.action.value, "order_type": o.order_type,
            "quantity": o.quantity, "product": o.product, "validity": "DAY",
            "tag": o.tag,                                   # max 20 alphanumeric chars
        }

    # ---------------- live mode ----------------
    def _headers(self) -> dict:
        return {"X-Kite-Version": "3", "Authorization": f"token {self._api_key}:{self._access_token}"}

    async def _request(self, method: str, path: str, **kw) -> dict:
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.request(method, BASE + path, headers=self._headers() if getattr(self, "_access_token", None) else {"X-Kite-Version": "3"}, **kw)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise TransientBrokerError(str(e)) from e
        if r.status_code == 429:
            raise RateLimitError(retry_after=float(r.headers.get("Retry-After", 1)))
        if r.status_code in (401, 403):
            raise AuthenticationError(r.text)
        if r.status_code >= 500:
            raise TransientBrokerError(f"{r.status_code} {r.text[:200]}")
        body = r.json()
        if r.status_code >= 400:
            raise OrderRejectedError(body.get("error_type", "REJECTED"), body.get("message", r.text))
        return body

    async def _live_authenticate(self, cred: dict) -> str:
        checksum = hashlib.sha256((cred["api_key"] + cred["request_token"] + cred["api_secret"]).encode()).hexdigest()
        self._api_key = cred["api_key"]
        body = await self._request("POST", "/session/token",
                                   data={"api_key": cred["api_key"], "request_token": cred["request_token"], "checksum": checksum})
        self._access_token = body["data"]["access_token"]
        return body["data"].get("user_id", cred["api_key"])

    async def _live_get_holdings(self) -> dict[str, int]:
        data = (await self._request("GET", "/portfolio/holdings"))["data"]
        out: dict[str, int] = {}
        for h in data:   # t1_quantity = bought today, not yet settled into `quantity`
            q = int(h.get("quantity", 0)) + int(h.get("t1_quantity", 0))
            if q > 0:
                out[h["tradingsymbol"].upper()] = q
        return out

    async def _live_place_order(self, payload: dict) -> str:
        return (await self._request("POST", "/orders/regular", data=payload))["data"]["order_id"]

    async def _live_get_order_status(self, order_id: str) -> BrokerOrderStatus:
        history = (await self._request("GET", f"/orders/{order_id}"))["data"]
        last = history[-1]
        state = {"COMPLETE": "COMPLETE", "REJECTED": "REJECTED", "CANCELLED": "CANCELLED"}.get(last["status"], "OPEN")
        return BrokerOrderStatus(order_id, state, int(last.get("filled_quantity", 0)),
                                 float(last.get("average_price", 0)), last.get("status_message") or "")

    async def _live_find_order_by_tag(self, tag: str) -> Optional[str]:
        for o in (await self._request("GET", "/orders"))["data"]:
            if o.get("tag") == tag:
                return o["order_id"]
        return None
