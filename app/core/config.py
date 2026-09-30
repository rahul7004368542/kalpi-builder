"""Runtime configuration, read from environment variables (12-factor style)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    broker_mode: str = "mock"            # "mock" (default) | "live"
    db_path: str = "data/kalpi.db"
    webhook_url: str | None = None       # consumer callback; None => console only
    webhook_secret: str = "dev-secret"   # HMAC key used to sign webhook bodies
    retry_max_attempts: int = 4
    retry_base_delay: float = 0.5        # seconds; doubles every attempt
    session_ttl_seconds: int = 1800

    @classmethod
    def from_env(cls) -> "Settings":
        e = os.environ.get
        return cls(
            broker_mode=e("BROKER_MODE", "mock").lower(),
            db_path=e("DB_PATH", "data/kalpi.db"),
            webhook_url=e("WEBHOOK_URL") or None,
            webhook_secret=e("WEBHOOK_SECRET", "dev-secret"),
            retry_max_attempts=int(e("RETRY_MAX_ATTEMPTS", "4")),
            retry_base_delay=float(e("RETRY_BASE_DELAY", "0.5")),
            session_ttl_seconds=int(e("SESSION_TTL_SECONDS", "1800")),
        )
