"""Shared plumbing so each concrete adapter is tiny.

A concrete adapter declares (1) its credentials, (2) how it translates our
normalised BrokerOrder into its broker-native payload, and (optionally)
(3) live-mode HTTP calls. In MOCK mode the native payload is built (so the
translation is real and testable) and then executed against MockExchange.
"""
from __future__ import annotations

from abc import abstractmethod
from typing import Optional

from app.core.exceptions import AuthenticationError, LiveModeNotImplemented
from app.domain.models import BrokerOrder, BrokerOrderStatus
from .base import BrokerAdapter
from .mock_exchange import MockExchange, get_exchange


class SimulatedBrokerAdapter(BrokerAdapter):
    def __init__(self, mode: str = "mock"):
        self.mode = mode
        self._exchange: Optional[MockExchange] = None
        self._client_id: Optional[str] = None
        self.sent_payloads: list[dict] = []     # native payloads, for inspection/tests

    # ---- subclass hooks ----
    @abstractmethod
    def native_order(self, order: BrokerOrder) -> dict:
        """Translate to this broker's order-placement payload."""

    async def _live_authenticate(self, credentials: dict) -> str:
        raise LiveModeNotImplemented(f"{self.name}: live mode not implemented, see README 'Going live'")

    async def _live_get_holdings(self) -> dict[str, int]:
        raise LiveModeNotImplemented(self.name)

    async def _live_place_order(self, payload: dict) -> str:
        raise LiveModeNotImplemented(self.name)

    async def _live_get_order_status(self, order_id: str) -> BrokerOrderStatus:
        raise LiveModeNotImplemented(self.name)

    async def _live_find_order_by_tag(self, tag: str) -> Optional[str]:
        return None

    # ---- BrokerAdapter ----
    @property
    def client_id(self) -> str:
        if not self._client_id:
            raise AuthenticationError("not authenticated")
        return self._client_id

    async def authenticate(self, credentials: dict) -> None:
        missing = [k for k in self.required_credentials if not str(credentials.get(k, "")).strip()]
        if missing:
            raise AuthenticationError(f"missing credentials: {', '.join(missing)}")
        if self.mode == "mock":
            self._client_id = str(credentials[self.required_credentials[0]])
            self._exchange = get_exchange(f"{self.name}:{self._client_id}", credentials.get("mock"))
        else:
            self._client_id = await self._live_authenticate(credentials)

    async def get_holdings(self) -> dict[str, int]:
        if self.mode == "mock":
            return {s: q for s, q in self._ex().holdings.items() if q > 0}
        return await self._live_get_holdings()

    async def place_order(self, order: BrokerOrder) -> str:
        payload = self.native_order(order)
        self.sent_payloads.append(payload)
        if self.mode == "mock":
            return self._ex().place(order)
        return await self._live_place_order(payload)

    async def get_order_status(self, order_id: str) -> BrokerOrderStatus:
        if self.mode == "mock":
            return self._ex().status(order_id)
        return await self._live_get_order_status(order_id)

    async def find_order_by_tag(self, tag: str) -> Optional[str]:
        if self.mode == "mock":
            return self._ex().by_tag.get(tag)
        return await self._live_find_order_by_tag(tag)

    def _ex(self) -> MockExchange:
        if self._exchange is None:
            raise AuthenticationError("not authenticated")
        return self._exchange
