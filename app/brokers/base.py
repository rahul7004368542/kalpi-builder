"""The broker port. The engine depends ONLY on this interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, Optional

from app.domain.models import BrokerOrder, BrokerOrderStatus


class BrokerAdapter(ABC):
    name: ClassVar[str]                              # registry key, e.g. "zerodha"
    required_credentials: ClassVar[tuple[str, ...]]  # what the UI/API must collect
    rate_limit_per_sec: ClassVar[float] = 5.0        # order-placement throttle

    @abstractmethod
    async def authenticate(self, credentials: dict) -> None:
        """Exchange credentials (request token, TOTP, ...) for a session.
        Raise AuthenticationError on failure."""

    @property
    @abstractmethod
    def client_id(self) -> str:
        """Stable identifier of the authenticated account."""

    @abstractmethod
    async def get_holdings(self) -> dict[str, int]:
        """Return {SYMBOL: quantity} for positive delivery holdings."""

    @abstractmethod
    async def place_order(self, order: BrokerOrder) -> str:
        """Place the order; return the broker order id.
        Raises RateLimitError / TransientBrokerError / OrderRejectedError."""

    @abstractmethod
    async def get_order_status(self, order_id: str) -> BrokerOrderStatus:
        ...

    async def find_order_by_tag(self, tag: str) -> Optional[str]:
        """Look up an order by our client tag. Used to reconcile after an ambiguous
        timeout BEFORE retrying, so a retry can never double-place. Default: unknown."""
        return None

    async def close(self) -> None:
        return None
