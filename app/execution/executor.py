"""Executes ONE planned order with throttling, retries and reconciliation."""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from app.brokers.base import BrokerAdapter
from app.brokers.rate_limit import TokenBucket
from app.core.exceptions import (AuthenticationError, BrokerError, OrderRejectedError,
                                 RateLimitError, TransientBrokerError)
from app.domain.enums import OrderStatus
from app.domain.models import BrokerOrder, OrderResult, PlannedOrder

log = logging.getLogger("kalpi.executor")


@dataclass
class RetryPolicy:
    max_attempts: int = 4
    base_delay: float = 0.5
    max_delay: float = 8.0
    jitter: float = 0.2
    poll_attempts: int = 5
    poll_interval: float = 0.5

    def delay(self, attempt: int, retry_after: Optional[float] = None) -> float:
        d = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        d += random.uniform(0, d * self.jitter)
        return max(d, retry_after or 0.0)


class OrderExecutor:
    def __init__(self, adapter: BrokerAdapter, throttle: TokenBucket, policy: RetryPolicy,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self.adapter, self.throttle, self.policy, self.sleep = adapter, throttle, policy, sleep

    def _result(self, p: PlannedOrder, status: OrderStatus, **kw) -> OrderResult:
        i = p.instruction
        return OrderResult(idx=p.idx, symbol=i.symbol, action=i.action, quantity=i.quantity,
                           category=p.category, status=status, **kw)

    async def execute(self, p: PlannedOrder) -> OrderResult:
        if p.precheck_error:
            code, msg = p.precheck_error
            return self._result(p, OrderStatus.FAILED, error_code=code, error_message=msg)

        i = p.instruction
        order = BrokerOrder(symbol=i.symbol, exchange=i.exchange, action=i.action,
                            quantity=i.quantity, tag=p.tag)
        order_id, attempts = None, 0

        while order_id is None:
            attempts += 1
            try:
                await self.throttle.acquire()
                order_id = await self.adapter.place_order(order)
            except OrderRejectedError as e:                      # definitive: never retry
                return self._result(p, OrderStatus.FAILED, attempts=attempts, error_code=e.code, error_message=e.message)
            except AuthenticationError as e:                     # session expired: retry is pointless
                return self._result(p, OrderStatus.FAILED, attempts=attempts, error_code=e.code, error_message=e.message)
            except (RateLimitError, TransientBrokerError) as e:
                if isinstance(e, TransientBrokerError):
                    # Ambiguous outcome: the order may already exist. Reconcile BEFORE retrying.
                    try:
                        order_id = await self.adapter.find_order_by_tag(p.tag)
                    except BrokerError:
                        order_id = None
                    if order_id:
                        log.warning("order %s found at broker after timeout; not re-sending", p.tag)
                        break
                if attempts >= self.policy.max_attempts:
                    return self._result(p, OrderStatus.FAILED, attempts=attempts, error_code=e.code,
                                        error_message=f"gave up after {attempts} attempts: {e.message}")
                await self.sleep(self.policy.delay(attempts, getattr(e, "retry_after", None)))

        return await self._confirm(p, order_id, attempts)

    async def _confirm(self, p: PlannedOrder, order_id: str, attempts: int) -> OrderResult:
        st = None
        for n in range(self.policy.poll_attempts):
            try:
                st = await self.adapter.get_order_status(order_id)
            except BrokerError as e:
                log.warning("status poll failed for %s: %s", order_id, e.message)
            if st and st.terminal:
                break
            await self.sleep(self.policy.poll_interval)

        common = dict(broker_order_id=order_id, attempts=attempts)
        if st is None or not st.terminal:
            return self._result(p, OrderStatus.SUBMITTED, **common)
        if st.state == "COMPLETE":
            return self._result(p, OrderStatus.SUCCESS, filled_quantity=st.filled_quantity, avg_price=st.avg_price, **common)
        return self._result(p, OrderStatus.FAILED, error_code=f"BROKER_{st.state}",
                            error_message=st.message or f"order {st.state.lower()} at broker", **common)
