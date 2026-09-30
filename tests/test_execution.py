import unittest

from app.core.exceptions import ConflictError, ValidationError
from tests.helpers import connect, make_engine, req


class FirstTime(unittest.IsolatedAsyncioTestCase):
    async def test_buys_all(self):
        engine, notifier = make_engine()
        a = await connect()
        r = await engine.execute(req("FIRST_TIME", ("RELIANCE", "BUY", 10), ("TCS", "BUY", 5)), a)
        self.assertEqual(r["status"], "SUCCESS")
        self.assertEqual(r["summary"], {"total": 2, "executed": 2, "pending": 0, "failed": 0})
        self.assertEqual(await a.get_holdings(), {"RELIANCE": 10, "TCS": 5})

    async def test_rejects_sell(self):
        engine, _ = make_engine()
        with self.assertRaises(ValidationError):
            await engine.execute(req("FIRST_TIME", ("TCS", "SELL", 1)), await connect())

    async def test_rejects_when_holdings_exist(self):
        engine, _ = make_engine()
        a = await connect(mock={"holdings": {"TCS": 3}})
        with self.assertRaises(ConflictError) as cm:
            await engine.execute(req("FIRST_TIME", ("INFY", "BUY", 1)), a)
        self.assertEqual(cm.exception.code, "HOLDINGS_EXIST")


class Rebalance(unittest.IsolatedAsyncioTestCase):
    async def test_sells_execute_before_buys(self):
        engine, _ = make_engine()
        a = await connect(mock={"holdings": {"RELIANCE": 20, "TCS": 10}})
        r = await engine.execute(req("REBALANCE", ("INFY", "BUY", 8), ("RELIANCE", "SELL", 20),
                                     ("TCS", "BUY", 5)), a)
        self.assertEqual(r["status"], "SUCCESS")
        self.assertEqual([o["action"] for o in r["orders"]], ["SELL", "BUY", "BUY"])
        self.assertEqual([o["symbol"] for o in r["orders"]], ["RELIANCE", "INFY", "TCS"])  # caller order kept per leg

    async def test_categories_inferred(self):
        engine, _ = make_engine()
        a = await connect(mock={"holdings": {"RELIANCE": 20, "TCS": 10, "SBIN": 4}})
        r = await engine.execute(req("REBALANCE", ("RELIANCE", "SELL", 20), ("TCS", "BUY", 5),
                                     ("SBIN", "SELL", 1), ("INFY", "BUY", 2)), a)
        cats = {o["symbol"]: o["category"] for o in r["orders"]}
        self.assertEqual(cats, {"RELIANCE": "EXIT", "TCS": "ADJUST", "SBIN": "ADJUST", "INFY": "NEW"})
        self.assertEqual(await a.get_holdings(), {"TCS": 15, "SBIN": 3, "INFY": 2})

    async def test_sale_proceeds_fund_buys(self):
        # cash too low for the buy on its own -> only works because SELL runs first
        engine, _ = make_engine()
        a = await connect(mock={"cash": 0, "holdings": {"RELIANCE": 100}})
        r = await engine.execute(req("REBALANCE", ("TCS", "BUY", 2), ("RELIANCE", "SELL", 100)), a)
        self.assertEqual(r["status"], "SUCCESS")

    async def test_partial_failure(self):
        engine, notifier = make_engine()
        a = await connect(mock={"holdings": {"RELIANCE": 20}, "fail_symbols": ["INFY"]})
        r = await engine.execute(req("REBALANCE", ("RELIANCE", "SELL", 10), ("TCS", "BUY", 5), ("INFY", "BUY", 8)), a)
        self.assertEqual(r["status"], "PARTIAL_SUCCESS")
        self.assertEqual((r["summary"]["executed"], r["summary"]["failed"]), (2, 1))
        bad = [o for o in r["orders"] if o["status"] == "FAILED"][0]
        self.assertEqual((bad["symbol"], bad["error_code"], bad["attempts"]), ("INFY", "RMS_REJECTED", 1))  # not retried
        # notification carries both successes and the failure
        ev = notifier.events[0]
        self.assertEqual(ev["status"], "PARTIAL_SUCCESS")
        self.assertEqual(len(ev["orders"]), 3)
        self.assertEqual(r["notification_status"], "SENT")
        self.assertEqual((await engine.repo.get(r["id"]))["notification_status"], "SENT")

    async def test_oversell_is_prechecked_and_not_sent(self):
        engine, _ = make_engine()
        a = await connect(mock={"holdings": {"RELIANCE": 5}})
        r = await engine.execute(req("REBALANCE", ("RELIANCE", "SELL", 50), ("TCS", "BUY", 1)), a)
        self.assertEqual(r["status"], "PARTIAL_SUCCESS")
        o = r["orders"][0]
        self.assertEqual((o["error_code"], o["attempts"]), ("INSUFFICIENT_HOLDINGS", 0))
        self.assertEqual(a._ex().call_log, [("BUY", "TCS", 1)])

    async def test_all_failed(self):
        engine, _ = make_engine()
        a = await connect(mock={"fail_symbols": ["TCS", "INFY"]})
        r = await engine.execute(req("REBALANCE", ("TCS", "BUY", 1), ("INFY", "BUY", 1)), a)
        self.assertEqual(r["status"], "FAILED")

    async def test_duplicate_symbol_rejected(self):
        engine, _ = make_engine()
        with self.assertRaises(ValidationError):
            await engine.execute(req("REBALANCE", ("TCS", "BUY", 1), ("tcs", "SELL", 1)), await connect())

    async def test_bad_quantity_rejected(self):
        engine, _ = make_engine()
        with self.assertRaises(ValidationError):
            await engine.execute(req("REBALANCE", ("TCS", "BUY", 0)), await connect())


class Resilience(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limit_is_retried(self):
        engine, _ = make_engine(max_attempts=4)
        a = await connect(mock={"rate_limit_first_n": 2})
        r = await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 1)), a)
        self.assertEqual(r["status"], "SUCCESS")
        self.assertEqual(r["orders"][0]["attempts"], 3)

    async def test_rate_limit_exhausted(self):
        engine, _ = make_engine(max_attempts=2)
        a = await connect(mock={"rate_limit_first_n": 99})
        r = await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 1)), a)
        self.assertEqual(r["status"], "FAILED")
        self.assertEqual(r["orders"][0]["error_code"], "RATE_LIMITED")

    async def test_ambiguous_timeout_never_double_places(self):
        # broker accepts the order but the response is lost; we must reconcile, not re-send
        engine, _ = make_engine()
        a = await connect(mock={"flaky_symbols": ["TCS"]})
        r = await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 5)), a)
        self.assertEqual(r["status"], "SUCCESS")
        self.assertEqual(await a.get_holdings(), {"TCS": 5})        # 5, not 10
        self.assertEqual(len(a._ex().call_log), 1)

    async def test_idempotency_replay(self):
        engine, _ = make_engine()
        a = await connect()
        rq = req("FIRST_TIME", ("TCS", "BUY", 5))
        r1 = await engine.execute(rq, a, idempotency_key="k1")
        r2 = await engine.execute(rq, a, idempotency_key="k1")      # double click
        self.assertEqual(r1["id"], r2["id"])
        self.assertTrue(r2["replayed"])
        self.assertEqual(await a.get_holdings(), {"TCS": 5})

    async def test_idempotency_key_reuse_with_other_payload(self):
        engine, _ = make_engine()
        a = await connect()
        await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 5)), a, idempotency_key="k1")
        with self.assertRaises(ConflictError) as cm:
            await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 6)), a, idempotency_key="k1")
        self.assertEqual(cm.exception.code, "IDEMPOTENCY_KEY_REUSED")

    async def test_failing_notifier_does_not_fail_execution(self):
        from app.notifications.notifier import Notifier

        class Boom(Notifier):
            async def notify(self, payload):
                raise RuntimeError("consumer down")

        engine, _ = make_engine()
        engine.notifier = Boom()
        r = await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 1)), await connect())
        self.assertEqual(r["status"], "SUCCESS")
        self.assertEqual((await engine.repo.get(r["id"]))["notification_status"], "FAILED")

    async def test_no_wait_mode(self):
        engine, notifier = make_engine()
        r = await engine.execute(req("FIRST_TIME", ("TCS", "BUY", 1)), await connect(), wait=False)
        self.assertEqual(r["status"], "RUNNING")
        await engine.drain()
        self.assertEqual((await engine.repo.get(r["id"]))["status"], "SUCCESS")
        self.assertEqual(len(notifier.events), 1)


if __name__ == "__main__":
    unittest.main()
