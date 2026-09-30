"""Upstox API v2. Auth: OAuth2 authorization code -> access_token (bearer). Orders use
`instrument_token` = "NSE_EQ|<ISIN>". Live mode not implemented."""
from app.brokers.registry import register_broker
from app.brokers.simulated import SimulatedBrokerAdapter
from app.domain.models import BrokerOrder

ISIN = {"RELIANCE": "INE002A01018", "TCS": "INE467B01029", "INFY": "INE009A01021",
        "HDFCBANK": "INE040A01034", "ICICIBANK": "INE090A01021", "SBIN": "INE062A01020"}


@register_broker
class UpstoxAdapter(SimulatedBrokerAdapter):
    name = "upstox"
    required_credentials = ("api_key", "api_secret", "auth_code")
    rate_limit_per_sec = 25.0

    def native_order(self, o: BrokerOrder) -> dict:
        seg = "NSE_EQ" if o.exchange == "NSE" else "BSE_EQ"
        return {
            "instrument_token": f"{seg}|{ISIN.get(o.symbol, 'MOCK-' + o.symbol)}",
            "transaction_type": o.action.value, "order_type": o.order_type,
            "product": "D",                                # D = delivery
            "quantity": o.quantity, "validity": "DAY", "price": 0,
            "trigger_price": 0, "disclosed_quantity": 0, "is_amo": False,
            "tag": o.tag,
        }
