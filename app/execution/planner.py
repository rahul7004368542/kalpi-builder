"""Turns a validated request + current holdings into an ordered execution plan.

The engine does NOT compute target-vs-current deltas (the payload already
contains explicit instructions), but it still:
  * validates the payload,
  * classifies each order (NEW / EXIT / ADJUST) against real holdings,
  * pre-checks SELLs against holdings so obviously-bad orders never hit the broker,
  * orders SELLs before BUYs so sale proceeds can fund purchases.
"""
from __future__ import annotations

import re
from dataclasses import replace

from app.core.exceptions import ConflictError, ValidationError
from app.domain.enums import Action, Category, Mode
from app.domain.models import ExecutionRequest, OrderInstruction, PlannedOrder

_SYMBOL = re.compile(r"^[A-Z0-9&\-]{1,20}$")


def normalise(request: ExecutionRequest) -> ExecutionRequest:
    orders, seen = [], set()
    for o in request.orders:
        sym = o.symbol.strip().upper()
        if not _SYMBOL.match(sym):
            raise ValidationError(f"invalid symbol '{o.symbol}'")
        if not isinstance(o.quantity, int) or isinstance(o.quantity, bool) or o.quantity <= 0:
            raise ValidationError(f"{sym}: quantity must be a positive integer")
        if sym in seen:
            raise ValidationError(f"{sym} appears more than once; send one net instruction per symbol")
        seen.add(sym)
        orders.append(replace(o, symbol=sym, exchange=o.exchange.upper()))
    if not orders:
        raise ValidationError("no orders supplied")
    return ExecutionRequest(mode=request.mode, orders=orders)


def _classify(o: OrderInstruction, held: int) -> Category:
    if o.category:
        return o.category
    if o.action == Action.BUY:
        return Category.ADJUST if held > 0 else Category.NEW
    return Category.EXIT if o.quantity >= held else Category.ADJUST


def build_plan(request: ExecutionRequest, holdings: dict[str, int], exec_token: str) -> list[PlannedOrder]:
    request = normalise(request)

    if request.mode == Mode.FIRST_TIME:
        if any(o.action == Action.SELL for o in request.orders):
            raise ValidationError("FIRST_TIME portfolios can only contain BUY orders")
        if any(q > 0 for q in holdings.values()):
            raise ConflictError("account already has holdings; use mode=REBALANCE", code="HOLDINGS_EXIST")

    planned = []
    for o in request.orders:
        held = holdings.get(o.symbol, 0)
        pre = None
        if o.action == Action.SELL and o.quantity > held:
            pre = ("INSUFFICIENT_HOLDINGS", f"cannot sell {o.quantity} {o.symbol}; hold {held}")
        planned.append((o, _classify(o, held), pre))

    # stable sort: SELLs first, each group keeps the caller's order
    planned.sort(key=lambda t: 0 if t[0].action == Action.SELL else 1)
    return [
        PlannedOrder(idx=i, instruction=o, category=c, tag=f"{exec_token}{i:03d}", precheck_error=pre)
        for i, (o, c, pre) in enumerate(planned)
    ]
