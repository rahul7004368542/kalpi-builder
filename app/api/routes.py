from __future__ import annotations

import hashlib
import hmac
from collections import deque

from fastapi import APIRouter, Header, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from app.brokers.registry import create_adapter, supported_brokers
from app.core.exceptions import NotFoundError
from .schemas import ConnectIn, ExecuteIn

router = APIRouter()
_sink: deque = deque(maxlen=50)     # demo webhook consumer


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/api/v1/brokers")
async def list_brokers():
    return [{"name": n, "required_credentials": list(c.required_credentials)}
            for n, c in sorted(supported_brokers().items())]


@router.post("/api/v1/brokers/{broker}/connect")
async def connect(broker: str, body: ConnectIn, request: Request):
    c = request.app.state.container
    adapter = create_adapter(broker, mode=c.settings.broker_mode)
    await adapter.authenticate(body.credentials)
    return {"session_id": c.sessions.create(adapter), "broker": adapter.name,
            "client_id": adapter.client_id, "mode": c.settings.broker_mode,
            "expires_in": c.settings.session_ttl_seconds}


@router.get("/api/v1/sessions/{session_id}/holdings")
async def holdings(session_id: str, request: Request):
    adapter = request.app.state.container.sessions.get(session_id)
    return {"broker": adapter.name, "holdings": await adapter.get_holdings()}


@router.post("/api/v1/executions")
async def execute(body: ExecuteIn, request: Request, wait: bool = True,
                  idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    """One-click execution. Send an Idempotency-Key header to make double-clicks/retries safe.
    wait=false returns 202 immediately; poll GET /api/v1/executions/{id}."""
    c = request.app.state.container
    adapter = c.sessions.get(body.session_id)
    result = await c.engine.execute(body.to_domain(), adapter, idempotency_key=idempotency_key, wait=wait)
    return JSONResponse(jsonable_encoder(result), status_code=200 if wait or result["status"] != "RUNNING" else 202)


@router.get("/api/v1/executions")
async def list_executions(request: Request, limit: int = 50):
    return await request.app.state.container.repo.list(min(limit, 200))


@router.get("/api/v1/executions/{execution_id}")
async def get_execution(execution_id: str, request: Request):
    rec = await request.app.state.container.repo.get(execution_id)
    if not rec:
        raise NotFoundError("execution not found")
    return rec


# ---- demo webhook consumer: verifies the HMAC signature like a real consumer would ----
@router.post("/webhooks/sink")
async def webhook_sink(request: Request):
    body = await request.body()
    secret = request.app.state.container.settings.webhook_secret.encode()
    expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
    ok = hmac.compare_digest(expected, request.headers.get("X-Kalpi-Signature", ""))
    if not ok:
        return JSONResponse({"error": {"code": "BAD_SIGNATURE", "message": "signature mismatch"}}, status_code=401)
    _sink.appendleft(await request.json())
    return {"received": True}


@router.get("/webhooks/sink")
async def webhook_sink_list():
    return list(_sink)
