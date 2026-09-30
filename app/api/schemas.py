from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from app.domain.enums import Action, Category, Mode
from app.domain.models import ExecutionRequest, OrderInstruction


class ConnectIn(BaseModel):
    credentials: dict[str, Any] = Field(
        ..., description="Broker-specific fields (see GET /api/v1/brokers). In mock mode you may add a 'mock' "
                         "object to seed holdings/cash or inject failures.")


class OrderIn(BaseModel):
    symbol: str = Field(min_length=1, max_length=20, examples=["RELIANCE"])
    action: Action
    quantity: int = Field(gt=0, le=10_000_000)
    category: Optional[Category] = Field(None, description="Optional hint; inferred from holdings if omitted")
    exchange: str = "NSE"


class ExecuteIn(BaseModel):
    session_id: str
    mode: Mode
    orders: list[OrderIn] = Field(min_length=1, max_length=500)

    def to_domain(self) -> ExecutionRequest:
        return ExecutionRequest(
            mode=self.mode,
            orders=[OrderInstruction(o.symbol, o.action, o.quantity, o.category, o.exchange) for o in self.orders])
