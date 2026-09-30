"""Broker registry with auto-discovery.

Adding broker #6 = drop ONE file into app/brokers/adapters/ that subclasses
BrokerAdapter and is decorated with @register_broker. No other file changes.
"""
from __future__ import annotations

import importlib
import pkgutil

from app.core.exceptions import UnsupportedBrokerError
from .base import BrokerAdapter

_REGISTRY: dict[str, type[BrokerAdapter]] = {}
_discovered = False


def register_broker(cls: type[BrokerAdapter]) -> type[BrokerAdapter]:
    _REGISTRY[cls.name] = cls
    return cls


def discover() -> None:
    global _discovered
    if _discovered:
        return
    import app.brokers.adapters as pkg
    for m in pkgutil.iter_modules(pkg.__path__):
        importlib.import_module(f"{pkg.__name__}.{m.name}")
    _discovered = True


def supported_brokers() -> dict[str, type[BrokerAdapter]]:
    discover()
    return dict(_REGISTRY)


def create_adapter(name: str, mode: str = "mock") -> BrokerAdapter:
    discover()
    cls = _REGISTRY.get(name.lower())
    if cls is None:
        raise UnsupportedBrokerError(f"unsupported broker '{name}'. Supported: {sorted(_REGISTRY)}")
    return cls(mode=mode) if _accepts_mode(cls) else cls()


def _accepts_mode(cls) -> bool:
    import inspect
    return "mode" in inspect.signature(cls.__init__).parameters
