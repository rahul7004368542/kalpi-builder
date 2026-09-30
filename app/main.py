from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.routes import router
from app.api.sessions import SessionStore
from app.core.config import Settings
from app.core.exceptions import KalpiError
from app.execution.engine import ExecutionEngine
from app.execution.executor import RetryPolicy
from app.notifications.notifier import CompositeNotifier, ConsoleNotifier, WebhookNotifier
from app.repositories.executions import ExecutionRepository


@dataclass
class Container:
    settings: Settings
    repo: ExecutionRepository
    engine: ExecutionEngine
    sessions: SessionStore


def build_container(settings: Settings) -> Container:
    repo = ExecutionRepository(settings.db_path)
    notifiers = [ConsoleNotifier()]
    if settings.webhook_url:
        notifiers.append(WebhookNotifier(settings.webhook_url, settings.webhook_secret))
    engine = ExecutionEngine(repo, CompositeNotifier(*notifiers),
                             RetryPolicy(max_attempts=settings.retry_max_attempts, base_delay=settings.retry_base_delay))
    return Container(settings, repo, engine, SessionStore(settings.session_ttl_seconds))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    app.state.container = build_container(Settings.from_env())
    yield
    await app.state.container.engine.drain()


app = FastAPI(title="Kalpi Portfolio Trade Execution Engine", version="1.0.0", lifespan=lifespan)
app.include_router(router)


@app.exception_handler(KalpiError)
async def kalpi_error_handler(_: Request, exc: KalpiError):
    return JSONResponse({"error": {"code": exc.code, "message": exc.message}}, status_code=exc.status_code)
