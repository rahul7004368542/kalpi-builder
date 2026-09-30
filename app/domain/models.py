"""Plain dataclasses: the engine has no dependency on FastAPI/pydantic."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

from .enums import Action, Category, ExecutionStatus, Mode, OrderStatus


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- input ----------
@dataclass
class OrderInstruction:
    symbol: str
    action: Action
    quantity: int
    category: Optional[Category] = None   # optional hint; inferred if omitted
    exchange: str = "NSE"


@dataclass
class ExecutionRequest:
    mode: Mode
    orders: list[OrderInstruction]


# ---------- planning ----------
@dataclass
class PlannedOrder:
    idx: int                              # position in execution order (SELLs first)
    instruction: OrderInstruction
    category: Category
    tag: str                              # client order id (<=20 alnum chars, broker-safe)
    precheck_error: Optional[tuple[str, str]] = None   # (code, message) => never sent


# ---------- broker-facing ----------
@dataclass
class BrokerOrder:
    symbol: str
    exchange: str
    action: Action
    quantity: int
    tag: str
    order_type: str = "MARKET"
    product: str = "CNC"                  # delivery


@dataclass
class BrokerOrderStatus:
    order_id: str
    state: str                            # OPEN | COMPLETE | REJECTED | CANCELLED
    filled_quantity: int = 0
    avg_price: float = 0.0
    message: str = ""

    @property
    def terminal(self) -> bool:
        return self.state in ("COMPLETE", "REJECTED", "CANCELLED")


# ---------- output ----------
@dataclass
class OrderResult:
    idx: int
    symbol: str
    action: Action
    quantity: int
    category: Category
    status: OrderStatus
    broker_order_id: Optional[str] = None
    filled_quantity: int = 0
    avg_price: float = 0.0
    attempts: int = 0
    error_code: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class ExecutionRecord:
    id: str
    broker: str
    client_ref: str
    mode: Mode
    status: ExecutionStatus = ExecutionStatus.RUNNING
    idempotency_key: Optional[str] = None
    request_hash: str = ""
    created_at: str = field(default_factory=utcnow)
    completed_at: Optional[str] = None
    notification_status: str = "PENDING"
    orders: list[OrderResult] = field(default_factory=list)

    def summary(self) -> dict:
        ok = sum(o.status == OrderStatus.SUCCESS for o in self.orders)
        pend = sum(o.status == OrderStatus.SUBMITTED for o in self.orders)
        bad = sum(o.status == OrderStatus.FAILED for o in self.orders)
        return {"total": len(self.orders), "executed": ok, "pending": pend, "failed": bad}

    def to_dict(self) -> dict:
        d = asdict(self)
        d["summary"] = self.summary()
        d.pop("request_hash", None)
        return d
