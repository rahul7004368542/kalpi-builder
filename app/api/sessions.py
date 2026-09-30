"""In-memory broker sessions. Credentials are used once to authenticate and are
never persisted; the authenticated adapter lives here until it expires."""
from __future__ import annotations

import secrets
import time

from app.brokers.base import BrokerAdapter
from app.core.exceptions import NotFoundError


class SessionStore:
    def __init__(self, ttl_seconds: int):
        self.ttl = ttl_seconds
        self._s: dict[str, tuple[BrokerAdapter, float]] = {}

    def create(self, adapter: BrokerAdapter) -> str:
        self._purge()
        sid = "ses_" + secrets.token_urlsafe(16)
        self._s[sid] = (adapter, time.monotonic() + self.ttl)
        return sid

    def get(self, sid: str) -> BrokerAdapter:
        self._purge()
        if sid not in self._s:
            raise NotFoundError("unknown or expired session; connect the broker again", code="SESSION_NOT_FOUND")
        return self._s[sid][0]

    def _purge(self) -> None:
        now = time.monotonic()
        for k in [k for k, (_, exp) in self._s.items() if exp < now]:
            del self._s[k]
