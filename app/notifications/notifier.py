from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from abc import ABC, abstractmethod

log = logging.getLogger("kalpi.notify")


class Notifier(ABC):
    @abstractmethod
    async def notify(self, payload: dict) -> None:
        """Raise on failure; the engine records it but never fails the execution."""


class ConsoleNotifier(Notifier):
    async def notify(self, payload: dict) -> None:
        s = payload["summary"]
        log.info("EXECUTION %s %s | executed=%s failed=%s pending=%s",
                 payload["execution_id"], payload["status"], s["executed"], s["failed"], s["pending"])
        for o in payload["orders"]:
            line = f"  {o['action']:<4} {o['symbol']:<10} x{o['quantity']:<6} {o['status']}"
            if o["error_code"]:
                line += f"  [{o['error_code']}] {o['error_message']}"
            log.info(line)


class WebhookNotifier(Notifier):
    """POSTs the summary as JSON, signed with HMAC-SHA256 in X-Kalpi-Signature."""

    def __init__(self, url: str, secret: str, attempts: int = 3, backoff: float = 0.5):
        self.url, self.secret, self.attempts, self.backoff = url, secret.encode(), attempts, backoff

    async def notify(self, payload: dict) -> None:
        import httpx
        body = json.dumps(payload, default=str).encode()
        sig = hmac.new(self.secret, body, hashlib.sha256).hexdigest()
        headers = {"Content-Type": "application/json", "X-Kalpi-Signature": sig,
                   "X-Kalpi-Event": payload["event"]}
        last = None
        for n in range(self.attempts):
            try:
                async with httpx.AsyncClient(timeout=5) as c:
                    r = await c.post(self.url, content=body, headers=headers)
                if r.status_code < 300:
                    return
                last = RuntimeError(f"webhook returned {r.status_code}")
            except Exception as e:  # noqa: BLE001
                last = e
            await asyncio.sleep(self.backoff * 2 ** n)
        raise last  # type: ignore[misc]


class CompositeNotifier(Notifier):
    def __init__(self, *notifiers: Notifier):
        self.notifiers = notifiers

    async def notify(self, payload: dict) -> None:
        errors = []
        for n in self.notifiers:
            try:
                await n.notify(payload)
            except Exception as e:  # noqa: BLE001
                errors.append(e)
        if errors:
            raise errors[0]


class InMemoryNotifier(Notifier):
    def __init__(self):
        self.events: list[dict] = []

    async def notify(self, payload: dict) -> None:
        self.events.append(payload)
