"""Angel One SmartAPI. Auth: client_code + PIN + TOTP -> jwtToken (+ refreshToken),
with the api_key in `X-PrivateKey`. Orders need an instrument `symboltoken`, which
comes from the public instrument-master JSON. Live mode not implemented."""
from app.brokers.registry import register_broker
from app.brokers.simulated import SimulatedBrokerAdapter
from app.domain.models import BrokerOrder

# Tiny excerpt of the instrument master (symbol -> token). Live mode would load the full file.
TOKENS = {"RELIANCE": "2885", "TCS": "11536", "INFY": "1594", "HDFCBANK": "1333",
          "ICICIBANK": "4963", "SBIN": "3045"}


@register_broker
class AngelOneAdapter(SimulatedBrokerAdapter):
    name = "angelone"
    required_credentials = ("client_code", "api_key", "pin", "totp")
    rate_limit_per_sec = 9.0

    def native_order(self, o: BrokerOrder) -> dict:
        return {
            "variety": "NORMAL",
            "tradingsymbol": f"{o.symbol}-EQ",
            "symboltoken": TOKENS.get(o.symbol, f"MOCK-{o.symbol}"),
            "transactiontype": o.action.value,
            "exchange": o.exchange,
            "ordertype": o.order_type,
            "producttype": "DELIVERY",
            "duration": "DAY",
            "price": "0",
            "quantity": str(o.quantity),                  # SmartAPI wants strings
            "ordertag": o.tag,
        }
