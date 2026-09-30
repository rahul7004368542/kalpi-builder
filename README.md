# Kalpi Portfolio Trade Execution Engine


**Problem 2 design document:** [docs/financial_data_platform.md](docs/financial_data_platform.md)

A FastAPI service that takes a portfolio instruction set (first-time buy or explicit rebalance), authenticates
with a user's broker, executes the orders in a single call, and notifies the consumer with a summary of
executed and failed trades.

**Supported brokers:** Zerodha, Fyers, Angel One, Groww, Upstox, behind one `BrokerAdapter` interface.
**Default mode is `mock`** (a simulated exchange), so the whole flow is demonstrable without real accounts or money.

## Quick start

```bash
docker compose up --build          # API on http://localhost:8000  (Swagger UI: /docs)
docker compose run --rm tests      # run the test suite in a container
```

Without Docker: `pip install -r requirements.txt && uvicorn app.main:app --reload` and `python -m pytest -q`.

## Try it (mock mode)

```bash
# 1. Connect a broker. The optional "mock" block seeds holdings and injects a failure for INFY.
curl -s localhost:8000/api/v1/brokers/zerodha/connect -H 'content-type: application/json' -d '{
  "credentials": {"api_key":"demo","api_secret":"s","request_token":"t",
    "mock": {"holdings":{"RELIANCE":20,"SBIN":10}, "fail_symbols":["INFY"]}}}'
# -> {"session_id":"ses_...", ...}

# 2. One-click rebalance
curl -s localhost:8000/api/v1/executions -H 'content-type: application/json' \
  -H 'Idempotency-Key: rebalance-2026-10-01' -d '{
  "session_id":"ses_...", "mode":"REBALANCE", "orders":[
    {"symbol":"RELIANCE","action":"SELL","quantity":20},
    {"symbol":"SBIN","action":"SELL","quantity":4},
    {"symbol":"TCS","action":"BUY","quantity":5},
    {"symbol":"INFY","action":"BUY","quantity":8}]}'
```

Result: `status: PARTIAL_SUCCESS`, summary `executed: 3, failed: 1`, with per-order status, broker order id, attempts and
error code. The same payload is POSTed (HMAC-signed) to the webhook and logged to the console. View received webhooks at
`GET /webhooks/sink`.

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/brokers` | Supported brokers and the credentials each needs |
| `POST /api/v1/brokers/{broker}/connect` | Authenticate; returns a `session_id` (credentials are never stored) |
| `GET /api/v1/sessions/{id}/holdings` | Current holdings |
| `POST /api/v1/executions` | Execute. `Idempotency-Key` header supported; `?wait=false` returns 202 and runs in the background |
| `GET /api/v1/executions[/{id}]` | Execution history / status |

`FIRST_TIME` payload: `{"mode":"FIRST_TIME","orders":[{"symbol":"TCS","action":"BUY","quantity":10}, ...]}`

## Architecture

```
HTTP (FastAPI)  ->  ExecutionEngine  ->  Planner  ->  OrderExecutor  ->  BrokerAdapter (port)
 api/                execution/          execution/    execution/          brokers/
                         |                                                     |-- ZerodhaAdapter
                         |-- ExecutionRepository (SQLite)                      |-- FyersAdapter ... (auto-discovered)
                         '-- Notifier (console + signed webhook)               '-- MockExchange (mock mode)
```

- **Separation of concerns.** `domain/` and `execution/` are plain Python with no FastAPI import; the API layer is a thin
  shell. The engine depends only on the `BrokerAdapter` abstraction.
- **Adapter pattern + auto-discovery.** A broker is one file in `app/brokers/adapters/` with `@register_broker`:
  declare required credentials, implement `native_order()` (normalised order -> broker payload), optionally the live calls.
  The registry imports everything in that package, so **adding broker #6 changes zero existing files**
  (`tests/test_brokers.py::test_sixth_broker_is_one_class` proves it).
- **Why no third-party broker library.** I wrote thin adapters rather than depending on a normalisation layer like OpenAlgo.
  The brokers' order APIs are small, I can explain every line (auth handshake, symbol mapping, payload shape, error
  mapping), and the engine's correctness properties (idempotency, reconciliation, retry classification) need control of
  the error taxonomy that a wrapper would blur. Trade-off: I maintain the adapters.

## How rebalancing works

The payload is already explicit, so the engine does **not** compute target-vs-current deltas. It does:

1. **Validate**: symbols, positive integer quantities, one net instruction per symbol (a duplicate is rejected as ambiguous).
2. **Fetch holdings** and classify each order as `NEW` (buy unheld), `EXIT` (sell whole position) or `ADJUST` (partial
   buy/sell of a held stock). An explicit `category` in the payload overrides inference.
3. **Pre-check SELLs** against holdings. An oversell is marked `FAILED: INSUFFICIENT_HOLDINGS` and never sent to the broker.
4. **Order the plan SELLs first, then BUYs**, so sale proceeds fund purchases (test: `test_sale_proceeds_fund_buys`).
5. **Persist** the execution, then execute order by order, updating the record after each one (crash-auditable).
6. **Aggregate**: `SUCCESS` (all filled), `PARTIAL_SUCCESS`, or `FAILED` (nothing went through). One failed order never aborts the rest.
7. **Notify**. A notification failure is recorded (`notification_status`) but never turns a successful trade into a failure.

`FIRST_TIME` mode accepts only BUYs and returns `409 HOLDINGS_EXIST` if the account already holds stocks.

## Reliability choices

| Concern | Handling |
|---|---|
| Broker rate limits | Per-broker token bucket before every call; HTTP 429 retried with exponential backoff + jitter, honouring `Retry-After`. |
| Definitive rejections (RMS, funds, blocked scrip) | Never retried; recorded with the broker's error code. |
| **Ambiguous timeouts** (order accepted, response lost) | Before retrying, look the order up by our client tag (`find_order_by_tag`). If it exists, use it instead of re-sending, so a timeout can never double-buy. (`test_ambiguous_timeout_never_double_places`) |
| Double-click / client retry | `Idempotency-Key` header, scoped per broker account. Replay returns the original result; same key with a different payload returns `409`. |
| Crash mid-rebalance | Record persisted before the first order and after each order. |
| Credentials | Used once to authenticate; only the session token lives in memory (TTL-bound). Never written to the DB or logs. |

## Going live

Zerodha has a live implementation (`BROKER_MODE=live`): checksum-based token exchange, holdings, regular orders, status,
and tag lookup, with HTTP errors mapped to the same exception taxonomy. **It has not been run against a real account**
(no credentials were available). The other four adapters implement and test the broker-native payload translation and
document their auth flows; their live calls raise `LiveModeNotImplemented` rather than pretending. Payload shapes are from
each broker's public docs and should be re-verified before real use (Groww especially).

## Known limitations / next steps

- SQLite + in-memory sessions = single instance. For horizontal scale: Postgres for the repository and Redis for sessions.
- Orders are sent sequentially per execution (simple and rate-limit-friendly); concurrency within the SELL and BUY legs is a straightforward extension.
- Market orders only; limit orders, price protection and a funds pre-check against live margin would be next for production.
- Webhook delivery is in-process with retries; a production system would use a transactional outbox.
- Tests use `unittest.IsolatedAsyncioTestCase`, runnable with plain `python -m unittest discover -s tests -t .` or pytest.
