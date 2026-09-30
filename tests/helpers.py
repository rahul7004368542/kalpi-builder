import itertools
from app.brokers.registry import create_adapter
from app.brokers.mock_exchange import _STORE
from app.domain.enums import Action, Mode
from app.domain.models import ExecutionRequest, OrderInstruction
from app.execution.engine import ExecutionEngine
from app.execution.executor import RetryPolicy
from app.notifications.notifier import InMemoryNotifier
from app.repositories.executions import ExecutionRepository

_ids = itertools.count(1)


async def no_sleep(_):
    return None


def make_engine(max_attempts=4):
    notifier = InMemoryNotifier()
    engine = ExecutionEngine(ExecutionRepository(":memory:"), notifier,
                             RetryPolicy(max_attempts=max_attempts), sleep=no_sleep)
    return engine, notifier


async def connect(broker="zerodha", mock=None):
    """Fresh account each time so tests don't share mock-exchange state."""
    a = create_adapter(broker)
    creds = {k: f"u{next(_ids)}" if i == 0 else "x" for i, k in enumerate(a.required_credentials)}
    creds["mock"] = mock or {}
    await a.authenticate(creds)
    return a


def req(mode, *orders):
    return ExecutionRequest(mode=Mode(mode), orders=[
        OrderInstruction(symbol=s, action=Action(a), quantity=q) for s, a, q in orders])
