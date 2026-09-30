"""In-memory broker back-end used by every adapter in MOCK mode.

It simulates what matters for testing an execution engine: cash/holdings,
deterministic prices, broker-side rejections, 429s, and the nasty case of a
timeout AFTER the order was actually accepted (ambiguous failure).

Scenario knobs (passed as credentials["mock"]):
  holdings            {"RELIANCE": 50}     seed holdings
  cash                1000000              seed cash
  fail_symbols        ["INFY"]             orders rejected by RMS
  flaky_symbols       ["TCS"]              1st attempt: order accepted, then timeout
  rate_limit_first_n  2                    first N place_order calls raise 429
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field

from app.core.exceptions import OrderRejectedError, RateLimitError, TransientBrokerError
from app.domain.enums import Action
from app.domain.models import BrokerOrder, BrokerOrderStatus


def mock_price(symbol: str) -> float:
    return float(100 + (sum(ord(c) for c in symbol) * 37) % 2900)


@dataclass
class MockExchange:
    cash: float = 1_000_000.0
    holdings: dict[str, int] = field(default_factory=dict)
    fail_symbols: set[str] = field(default_factory=set)
    flaky_symbols: set[str] = field(default_factory=set)
    rate_limit_first_n: int = 0
    # bookkeeping
    orders: dict[str, BrokerOrderStatus] = field(default_factory=dict)
    by_tag: dict[str, str] = field(default_factory=dict)
    call_log: list[tuple[str, str, int]] = field(default_factory=list)  # (action, symbol, qty)
    _calls: int = 0
    _flaked: set[str] = field(default_factory=set)
    _ids: itertools.count = field(default_factory=lambda: itertools.count(1))

    @classmethod
    def from_config(cls, cfg: dict) -> "MockExchange":
        return cls(
            cash=float(cfg.get("cash", 1_000_000)),
            holdings={k.upper(): int(v) for k, v in cfg.get("holdings", {}).items()},
            fail_symbols={s.upper() for s in cfg.get("fail_symbols", [])},
            flaky_symbols={s.upper() for s in cfg.get("flaky_symbols", [])},
            rate_limit_first_n=int(cfg.get("rate_limit_first_n", 0)),
        )

    def place(self, o: BrokerOrder) -> str:
        self._calls += 1
        if self._calls <= self.rate_limit_first_n:
            raise RateLimitError("429 Too Many Requests", retry_after=0.05)

        if o.tag in self.by_tag:                      # broker-side dedupe on tag
            return self.by_tag[o.tag]

        if o.symbol in self.fail_symbols:
            raise OrderRejectedError("RMS_REJECTED", f"{o.symbol}: blocked by broker RMS")

        price = mock_price(o.symbol)
        if o.action == Action.BUY:
            if price * o.quantity > self.cash:
                raise OrderRejectedError("INSUFFICIENT_FUNDS", f"need {price * o.quantity:,.0f}, have {self.cash:,.0f}")
            self.cash -= price * o.quantity
            self.holdings[o.symbol] = self.holdings.get(o.symbol, 0) + o.quantity
        else:
            if self.holdings.get(o.symbol, 0) < o.quantity:
                raise OrderRejectedError("INSUFFICIENT_HOLDINGS", f"hold {self.holdings.get(o.symbol, 0)}, selling {o.quantity}")
            self.cash += price * o.quantity
            self.holdings[o.symbol] -= o.quantity
            if self.holdings[o.symbol] == 0:
                del self.holdings[o.symbol]

        oid = f"MOCK{next(self._ids):06d}"
        self.orders[oid] = BrokerOrderStatus(oid, "COMPLETE", o.quantity, price)
        self.by_tag[o.tag] = oid
        self.call_log.append((o.action.value, o.symbol, o.quantity))

        if o.symbol in self.flaky_symbols and o.symbol not in self._flaked:
            self._flaked.add(o.symbol)                # accepted, but the response is lost
            raise TransientBrokerError("read timeout after order submission")
        return oid

    def status(self, oid: str) -> BrokerOrderStatus:
        return self.orders[oid]


# One exchange per (broker, account) so state survives across HTTP requests.
_STORE: dict[str, MockExchange] = {}


def get_exchange(key: str, config: dict | None) -> MockExchange:
    if config is not None or key not in _STORE:
        _STORE[key] = MockExchange.from_config(config or {})
    return _STORE[key]
