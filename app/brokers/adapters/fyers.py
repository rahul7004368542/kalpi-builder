"""Fyers API v3. Auth: app_id + auth_code (from login redirect) -> access_token,
used as header `Authorization: app_id:access_token`. Live mode not implemented."""
from app.brokers.registry import register_broker
from app.brokers.simulated import SimulatedBrokerAdapter
from app.domain.enums import Action
from app.domain.models import BrokerOrder


@register_broker
class FyersAdapter(SimulatedBrokerAdapter):
    name = "fyers"
    required_credentials = ("app_id", "secret_id", "auth_code")
    rate_limit_per_sec = 10.0

    def native_order(self, o: BrokerOrder) -> dict:
        return {
            "symbol": f"{o.exchange}:{o.symbol}-EQ",      # Fyers symbology
            "qty": o.quantity,
            "type": 2,                                    # 2 = MARKET
            "side": 1 if o.action == Action.BUY else -1,  # 1 = buy, -1 = sell
            "productType": "CNC", "limitPrice": 0, "stopPrice": 0,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False,
            "orderTag": o.tag,
        }
