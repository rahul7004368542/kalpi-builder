"""Groww Trade API. Auth: api key/secret -> bearer access token. Live mode not implemented.
NOTE: payload shape follows Groww's public docs as I understand them; verify against the
current docs before going live."""
from app.brokers.registry import register_broker
from app.brokers.simulated import SimulatedBrokerAdapter
from app.domain.models import BrokerOrder


@register_broker
class GrowwAdapter(SimulatedBrokerAdapter):
    name = "groww"
    required_credentials = ("api_key", "api_secret")
    rate_limit_per_sec = 10.0

    def native_order(self, o: BrokerOrder) -> dict:
        return {
            "trading_symbol": o.symbol, "exchange": o.exchange, "segment": "CASH",
            "transaction_type": o.action.value, "order_type": o.order_type,
            "product": "CNC", "quantity": o.quantity, "validity": "DAY",
            "order_reference_id": o.tag,
        }
