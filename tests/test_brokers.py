import unittest

from app.brokers.base import BrokerAdapter
from app.brokers.registry import create_adapter, register_broker, supported_brokers
from app.brokers.simulated import SimulatedBrokerAdapter
from app.core.exceptions import AuthenticationError, UnsupportedBrokerError
from app.domain.enums import Action
from app.domain.models import BrokerOrder
from tests.helpers import connect, make_engine, req

ORDER = BrokerOrder("RELIANCE", "NSE", Action.SELL, 7, tag="abc123def456003")


class Brokers(unittest.IsolatedAsyncioTestCase):
    def test_five_brokers_registered(self):
        self.assertEqual(set(supported_brokers()), {"zerodha", "fyers", "angelone", "groww", "upstox"})

    def test_unknown_broker(self):
        with self.assertRaises(UnsupportedBrokerError):
            create_adapter("robinhood")

    def test_native_payloads(self):
        z = create_adapter("zerodha").native_order(ORDER)
        self.assertEqual((z["tradingsymbol"], z["transaction_type"], z["product"]), ("RELIANCE", "SELL", "CNC"))
        f = create_adapter("fyers").native_order(ORDER)
        self.assertEqual((f["symbol"], f["side"]), ("NSE:RELIANCE-EQ", -1))
        a = create_adapter("angelone").native_order(ORDER)
        self.assertEqual((a["symboltoken"], a["quantity"]), ("2885", "7"))
        g = create_adapter("groww").native_order(ORDER)
        self.assertEqual((g["segment"], g["trading_symbol"]), ("CASH", "RELIANCE"))
        u = create_adapter("upstox").native_order(ORDER)
        self.assertEqual(u["instrument_token"], "NSE_EQ|INE002A01018")

    def test_tag_fits_zerodha_limit(self):
        from app.execution.planner import build_plan
        plan = build_plan(req("FIRST_TIME", ("TCS", "BUY", 1)), {}, "a" * 12)
        self.assertLessEqual(len(plan[0].tag), 20)

    async def test_missing_credentials(self):
        with self.assertRaises(AuthenticationError):
            await create_adapter("zerodha").authenticate({"api_key": "k"})

    async def test_live_mode_is_explicit_about_not_being_implemented(self):
        from app.core.exceptions import LiveModeNotImplemented
        a = create_adapter("fyers", mode="live")
        with self.assertRaises(LiveModeNotImplemented):
            await a.authenticate({"app_id": "a", "secret_id": "b", "auth_code": "c"})

    async def test_every_broker_runs_the_same_flow(self):
        for name in supported_brokers():
            with self.subTest(broker=name):
                engine, _ = make_engine()
                a = await connect(name, mock={"holdings": {"RELIANCE": 10}})
                r = await engine.execute(req("REBALANCE", ("RELIANCE", "SELL", 10), ("TCS", "BUY", 3)), a)
                self.assertEqual(r["status"], "SUCCESS")
                self.assertEqual(len(a.sent_payloads), 2)

    async def test_sixth_broker_is_one_class(self):
        @register_broker
        class DhanAdapter(SimulatedBrokerAdapter):
            name = "dhan"
            required_credentials = ("client_id", "access_token")

            def native_order(self, o):
                return {"securityId": o.symbol, "transactionType": o.action.value, "quantity": o.quantity}

        engine, _ = make_engine()
        a = await connect("dhan")
        r = await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 1)), a)
        self.assertEqual(r["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
