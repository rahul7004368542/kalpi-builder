"""Orchestrates: idempotency -> holdings -> plan -> persist -> execute -> notify."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from typing import Awaitable, Callable, Optional

from app.brokers.base import BrokerAdapter
from app.brokers.rate_limit import TokenBucket
from app.core.exceptions import ConflictError
from app.domain.enums import ExecutionStatus, OrderStatus
from app.domain.models import ExecutionRecord, ExecutionRequest, PlannedOrder, OrderResult, utcnow
from app.notifications.notifier import Notifier
from app.repositories.executions import DuplicateIdempotencyKey, ExecutionRepository
from .executor import OrderExecutor, RetryPolicy
from .planner import build_plan, normalise

log = logging.getLogger("kalpi.engine")


def _hash_request(req: ExecutionRequest) -> str:
    canon = [(o.symbol, o.action.value, o.quantity, o.exchange) for o in sorted(req.orders, key=lambda o: o.symbol)]
    return hashlib.sha256(json.dumps([req.mode.value, canon]).encode()).hexdigest()


def final_status(orders: list[OrderResult]) -> ExecutionStatus:
    ok = sum(o.status == OrderStatus.SUCCESS for o in orders)
    bad = sum(o.status == OrderStatus.FAILED for o in orders)
    if bad == 0 and ok == len(orders):
        return ExecutionStatus.SUCCESS
    if bad == len(orders):
        return ExecutionStatus.FAILED
    return ExecutionStatus.PARTIAL_SUCCESS


class ExecutionEngine:
    def __init__(self, repo: ExecutionRepository, notifier: Notifier, policy: Optional[RetryPolicy] = None,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self.repo, self.notifier = repo, notifier
        self.policy = policy or RetryPolicy()
        self.sleep = sleep
        self._tasks: set[asyncio.Task] = set()

    async def execute(self, request: ExecutionRequest, adapter: BrokerAdapter,
                      idempotency_key: Optional[str] = None, wait: bool = True) -> dict:
        request = normalise(request)
        client_ref = f"{adapter.name}:{adapter.client_id}"
        scoped_key = f"{client_ref}:{idempotency_key}" if idempotency_key else None
        req_hash = _hash_request(request)

        if scoped_key:                                   # 1. replay protection
            existing = await self.repo.get_by_key(scoped_key)
            if existing:
                return self._replay(existing, req_hash)

        holdings = await adapter.get_holdings()          # 2. current state
        exec_id = uuid.uuid4().hex
        plan = build_plan(request, holdings, exec_id[:12])   # 3. validate + plan

        record = ExecutionRecord(id=f"exe_{exec_id}", broker=adapter.name, client_ref=client_ref,
                                 mode=request.mode, idempotency_key=scoped_key, request_hash=req_hash)
        try:                                             # 4. persist BEFORE touching the broker
            await self.repo.create(record)
        except DuplicateIdempotencyKey:
            return self._replay(await self.repo.get_by_key(scoped_key), req_hash)

        if wait:
            await self._run(record, plan, adapter)
        else:
            t = asyncio.create_task(self._run(record, plan, adapter))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)
        return record.to_dict()

    @staticmethod
    def _replay(existing: tuple[dict, str], req_hash: str) -> dict:
        payload, stored_hash = existing
        if stored_hash != req_hash:
            raise ConflictError("Idempotency-Key was already used with a different payload",
                                code="IDEMPOTENCY_KEY_REUSED")
        payload["replayed"] = True
        return payload

    async def _run(self, record: ExecutionRecord, plan: list[PlannedOrder], adapter: BrokerAdapter) -> None:
        executor = OrderExecutor(adapter, TokenBucket(adapter.rate_limit_per_sec), self.policy, self.sleep)
        for p in plan:                                   # SELLs first, then BUYs (planner order)
            try:
                result = await executor.execute(p)
            except Exception as e:                       # noqa: BLE001  one bad order never aborts the rest
                log.exception("unexpected error on %s", p.instruction.symbol)
                i = p.instruction
                result = OrderResult(p.idx, i.symbol, i.action, i.quantity, p.category, OrderStatus.FAILED,
                                     error_code="INTERNAL_ERROR", error_message=str(e))
            record.orders.append(result)
            await self.repo.update(record)               # durable progress

        record.status = final_status(record.orders)
        record.completed_at = utcnow()
        await self.repo.update(record)

        try:                                             # notification failure != execution failure
            await self.notifier.notify(self._payload(record))
            record.notification_status = "SENT"
        except Exception as e:                           # noqa: BLE001
            log.error("notification failed: %s", e)
            record.notification_status = "FAILED"
        await self.repo.update(record)

    @staticmethod
    def _payload(r: ExecutionRecord) -> dict:
        d = r.to_dict()
        return {"event": "execution.completed", "execution_id": r.id, "broker": r.broker,
                "mode": r.mode.value, "status": r.status.value, "summary": d["summary"],
                "completed_at": r.completed_at, "orders": d["orders"]}

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
