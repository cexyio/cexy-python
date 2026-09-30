# cexy: Python SDK for CEXY.io

Typed Python client for the [CEXY.io](https://cexy.io) exchange REST and WebSocket API.

- Sync (`cexy.Client`) and asyncio (`cexy.AsyncClient`) clients
- Pydantic v2 models generated from the public OpenAPI spec; every amount is a `Decimal`
- Safe retries (backoff, `Retry-After`, `client_order_id` for orders, idempotency keys for pools)
- Client-side rate limiting, cursor pagination
- WebSocket client with heartbeat, reconnect and a self-syncing order book

> Status: **0.1.0.dev9, pre-release.** The API may change before 1.0 (see [Versioning](#versioning)).
> Pre-releases need `--pre`: `pip install --pre cexy`.

## Install

```bash
pip install --pre cexy    # Python 3.9+ (--pre while releases are 0.1.0.devN)
```

## Quickstart: public market data

No API key is needed for market data.

```python
from cexy import Client

with Client() as client:
    print(client.time().iso)
    for market in client.markets.list():
        print(market.symbol, market.last_price, market.status)

    book = client.markets.orderbook("BTC/USDT", depth=10)
    best_bid_price, best_bid_qty = book.bids[0]          # Decimal, Decimal
    candles = client.markets.candles("BTC/USDT", "1h", limit=24)
```

Also available: `client.markets.get(symbol)`, `client.markets.trades(symbol)`,
`client.assets.list()/get()`, `client.networks.list()`, `client.fees.list()`,
`client.config()`, `client.pools.list()/get()`.

## Quickstart: your account

Create an API key in your account settings. Keep the secret out of source code:

```bash
export CEXY_API_KEY=ak_your_key_here
export CEXY_API_SECRET=your_secret_here
```

```python
import os
from decimal import Decimal
from cexy import Client

client = Client(api_key=os.environ["CEXY_API_KEY"], api_secret=os.environ["CEXY_API_SECRET"])

for balance in client.account.balances():
    print(balance.asset, balance.available)

# A sub-account's balances (parent account only; same shape, incl. held_incoming):
for balance in client.account.sub_account_balances("sub-account-id"):
    print(balance.asset, balance.available)

# Places a REAL order (needs the `trade` scope). Amounts: Decimal or str, never float.
placed = client.trading.place_order(
    "BTC/USDT", "buy", "limit", quantity=Decimal("0.001"), price="30000", time_in_force="post_only"
)
client.trading.cancel_order(placed.order.id)
```

Both `api_key` and `api_secret` are required together; passing only one raises
`ValueError` immediately.

| Resource | Methods |
|---|---|
| `client.account` | `balances()`, `balance(asset)`, `ledger()`, `notifications()`, `sub_accounts()`, `sub_account_balances(id)`, `api_keys()` |
| `client.wallet` | `deposits()`, `deposit(id)`, `withdrawals()`, `withdrawal(id)`, `withdrawal_addresses()`, `deposit_address(asset, network)` |
| `client.trading` | `open_orders()`, `order(id)`, `order_by_client_id(id)`, `order_history()`, `trades()`, `place_order(...)`, `cancel_order(id)`, `cancel_all(symbol=...)` |
| `client.exports` | `deposits()`, `ledger()`, `orders()`, `trades()`, `withdrawals()` (CSV bytes) |
| `client.pools` | `join(symbol, base_amount=, quote_amount=)`, `exit(symbol, shares=)` |

Note: `wallet.deposit_address()` **creates** a deposit address the first time it is called
for an asset/network pair, then returns the same address on later calls.

`cancel_all` requires the `symbol` keyword; pass `symbol=None` explicitly to cancel orders
in every market. It also cancels stop orders that have not triggered yet (`pending_trigger`),
so nothing fires into the market afterwards. The server allows 30 cancel-all calls per minute per account.

One `cancel_all` call handles at most 500 orders and waits up to 500 ms for orders still being
placed. The result lists `cancelled`, `already_closed` (orders that closed on their own; not an
error) and `failed`, with a reason per order in `failures`; `has_more=True` means call again.
The server allows 30 calls a minute per account. To keep going until nothing is left, opt in:

```python
res = client.trading.cancel_all(symbol=None, until_done=True)   # max_rounds=20, time_budget=120.0
res.cancelled, res.already_closed, res.failed, res.failures      # merged: each order's latest state
res.stopped                                                      # "done", "max_rounds" or "time_budget"
res.last_error_code                                              # e.g. "RATE_LIMITED" if the last round failed
```

The loop repeats while `has_more` is true or an order is still being placed (`INVALID_STATE`) or
unreadable (`SERVICE_UNAVAILABLE`), and waits 1, 2, 4, 8, then 15 s after a round without progress.
Every round is exactly one HTTP request (the transport does not retry inside the loop), so it never
sends more than `max_rounds` requests. A retryable error (429, 5xx, network) counts as a round
without progress; after a 429 it waits the server's `Retry-After` exactly. A wait that would reach
`time_budget` is not taken: the loop stops with `stopped == "time_budget"`. A non-retryable error is
raised with the merged result so far in `err.partial`.

The async client has the same methods:

```python
import asyncio
from cexy import AsyncClient

async def main() -> None:
    async with AsyncClient() as client:
        markets = await client.markets.list()

asyncio.run(main())
```

## Amounts

The API sends every amount as a decimal string. The SDK parses them into
`decimal.Decimal`, and sends amounts as strings. Passing a `float` raises `TypeError`
before anything is sent, because a float cannot represent most decimal amounts exactly.

## Held incoming transfers

Every balance has `held_incoming`: incoming internal transfers still held, each with
`transfer_id`, `amount` (Decimal) and `available_at`. Their sum is **already included in
`locked`**, so never add it to `locked` or `total` again. There are at most 100 entries, soonest
`available_at` first (millisecond precision), with no sender identity. An entry disappears once
the transfer is released (the amount moves to `available`) or cancelled by the exchange. It is
always a list (`[]` when none, including from servers that predate the field).

## Ledger references

`LedgerEntryResponse.reference` says what caused an entry. It is a union told apart by `type`:
`LedgerReferenceDeposit`, `LedgerReferenceWithdrawal`, `LedgerReferenceOrder`, `LedgerReferenceTrade`,
`LedgerReferenceTransfer`, `LedgerReferenceAdjustment`, `LedgerReferencePool`,
`LedgerReferenceFuturesTransfer` and `LedgerReferenceSystem` (all in `cexy.models`). A `type` this
SDK version does not know yet, or a reference whose fields don't match its `type` (a missing
field, or one of the wrong type), decodes to
`LedgerReferenceUnknown` with every field kept, so a new server-side cause never breaks decoding.

```python
from cexy.models import LedgerReferenceOrder, LedgerReferenceUnknown

for entry in client.account.ledger().auto_paging_iter():
    ref = entry.reference
    if isinstance(ref, LedgerReferenceOrder):
        print(entry.kind, "order", ref.order_id)
    elif isinstance(ref, LedgerReferenceUnknown):
        print(entry.kind, "unrecognised cause", ref.model_dump())
```

Ids (`order_id`, `pool_id`, ...) are plain strings; the SDK does not check their format.

## Errors

Every API error raises `cexy.CexyApiError` (or a subclass) with `code`, `message`,
`details`, `fields`, `request_id`, `retryable` and `status`. Branch on `code`, never on the
message.

| Exception | When |
|---|---|
| `ValidationError` | 400, e.g. `VALIDATION_FAILED`, `PRECISION_EXCEEDED` (see `fields`) |
| `AuthenticationError` | 401, e.g. `UNAUTHENTICATED`, `INVALID_CREDENTIALS` |
| `ForbiddenError` | 403: `FORBIDDEN` (key lacks a scope), `API_KEY_NOT_ALLOWED` (session-only endpoint) |
| `JurisdictionBlockedError` | 451: `JURISDICTION_BLOCKED` (not available in the caller's jurisdiction); a `ForbiddenError` subclass |
| `NotFoundError` | 404 |
| `ConflictError` | 409, e.g. `ALREADY_EXISTS`, `IDEMPOTENCY_KEY_CONFLICT` |
| `UnprocessableError` | 422, e.g. `INSUFFICIENT_FUNDS`, `MARKET_UNAVAILABLE` |
| `RateLimitError` | 429; `.retry_after` gives the server's requested wait |
| `ServerError` | 5xx, e.g. `UNDER_MAINTENANCE`, `ENGINE_OVERLOADED` |

An error code this SDK version does not know is raised as the base `CexyApiError`: it never
crashes the client. Network failures after retries raise `CexyConnectionError`. Calling a
private endpoint on a client without keys raises `MissingCredentialsError` locally.

```python
from cexy import UnprocessableError

try:
    client.trading.place_order("BTC/USDT", "buy", "market", quote_quantity="50")
except UnprocessableError as err:
    if err.code == "INSUFFICIENT_FUNDS":
        print("need", err.details.get("required"))
```

## Retries and idempotency

`Client(max_retries=3, timeout=10)` retries with exponential backoff and full jitter:

- **GET** requests retry on network errors, 429, 502/503/504 and any error with
  `retryable: true`. A 4xx is never retried except 429 and 409 `CONCURRENT_MODIFICATION`, whatever
  its body says.
- **429** waits for the larger of the `Retry-After` header and `details.retry_after_seconds`.
  These server hints are untrusted: unparseable, negative or non-finite values are ignored, and a
  requested wait **above 120 s is never waited**: the call raises `RateLimitError` at once (its
  `.retry_after` still gives the server's value), so a bad header can't hang your program.
- **Orders: safety rests on `client_order_id`, not on `Idempotency-Key`.** The server
  does not honour `Idempotency-Key` on `POST /trading/orders`, `DELETE /trading/orders/{id}`
  or cancel-all, so the SDK does not send one there.
  - **`place_order`** always sends a `client_order_id` (a UUID unless you pass one). It is
    unique per account, and a repeat is refused before any funds move. After an ambiguous
    failure (timeout, dropped connection, 500/502/504) the SDK first looks the order up with
    `trading.order_by_client_id()`. If the order exists, it is returned. Otherwise the order
    is resent with the same `client_order_id`. If a retry is refused as a duplicate
    (`ALREADY_EXISTS`), the existing order is fetched and returned.
  - **`cancel_order`** is naturally repeatable and is retried. If a retry is refused with
    `INVALID_STATE` (an earlier attempt already cancelled the order, or it filled
    meanwhile), the SDK fetches and returns the order's current state, so check `status`.
  - **`cancel_all`** is retried: repeating it only cancels whatever is still open.
- **Pool join/exit** carry an automatically generated `Idempotency-Key` header, reused on
  every retry of the same call. A `409 CONCURRENT_MODIFICATION` (the same key still in
  flight) is retried with the same key. Pass `idempotency_key=` to use your own.

## Pagination

History endpoints return a `Page` with `items`, `next_cursor` and `has_more`:

```python
page = client.trading.order_history(symbol="BTC/USDT", limit=100)
for order in page.auto_paging_iter():      # fetches later pages lazily
    print(order.id, order.status)

# or one page at a time
next_page = page.next_page()               # None on the last page
```

With `AsyncClient`: `async for order in page.auto_paging_iter(): ...`.

## Rate limits

Server-side limits:

- **Anonymous (public) requests:** about **120 requests/minute per IP address**.
- **API-key requests:** each key has its own limit, about **600 requests/minute per key**
  (configurable server-side; this applies after the upcoming key release).

The client keeps itself below these with a token bucket: **100/min by default without a
key, 300/min with a key**. Override it with `Client(rate_limit_per_minute=...)`. When
responses carry `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`
(the number of **seconds** until the window resets), the client slows down to match them, but it
never blocks longer than 120 s because of these headers. A 429 is retried after the server's
`Retry-After` (at most 120 s; see above).

## WebSocket

```python
import asyncio
from cexy.ws import WebSocketClient, BOOK_STALE, RECONNECTED

async def main() -> None:
    async with WebSocketClient() as ws:             # wss://api.cexy.io/api/v1/ws
        await ws.subscribe("ticker:BTC/USDT", "trades:BTC/USDT")
        book = await ws.order_book("BTC/USDT")
        async for event in ws:
            if event.type == "orderbook.update":
                print(book.sequence, book.best_bid(), book.best_ask())
            elif event.type == BOOK_STALE:
                print("gap detected; book is stale until the next update")

asyncio.run(main())
```

The client handles the protocol rules for you:

- It sends `{"op":"ping"}` every 30 s (required: the server closes idle connections), accepts
  the server's unsolicited `pong` frames, and reconnects if nothing arrives for 75 s.
- It reconnects with exponential backoff and jitter, then re-authenticates, re-subscribes
  and emits a `reconnected` event.
- It refuses more than 100 subscriptions locally (reported in `SubscribeResult.refused`)
  and keeps its own send rate under 200 messages per minute.
- Every request carries an `id` and waits for the server's acknowledgement with that id
  (`authenticated`, `subscribed`, `unsubscribed`, `pong`). An `error` acknowledgement, or none
  within `request_timeout`, raises `cexy.ws.WebSocketError`. `await ws.ping()` returns the
  round-trip time.
- Unknown event types are ignored; an unknown `protocol_version` logs one warning.
- You can register callbacks with `ws.on("trade.new", handler)` instead of iterating.

**Order-book rules** (implemented by `ws.order_book()`):

1. Subscribe to `orderbook:{symbol}` first, then take a REST snapshot at sequence `S`.
2. Drop updates with `sequence <= S`.
3. Every `orderbook.update` carries the complete top 50 of both sides (`"full": true`) and
   replaces the book outright; there are no deltas. REST levels deeper than 50 are never
   merged into the live book.
4. A sequence gap marks the book `stale` (and emits `book_stale`) until the next update heals
   it. There is no forced resync.
5. After every reconnect a fresh snapshot is taken: sequences reset when the server
   restarts and are never compared across connections.
6. A `CONCURRENT_MODIFICATION` error frame (messages were dropped) triggers a fresh snapshot
   of every book and a `resync` event, so you can refresh other state too.

**Private channels** (`orders`, `balances`, `deposits`, `withdrawals`, `account`) need
`await ws.auth(token)` with a session access token; it returns once the server sends
`authenticated`. **API-key authentication on the
WebSocket is not available yet**; with an API key, poll the REST endpoints for private
state. If the session is revoked, the `account` channel delivers `session.revoked` and the
client emits `auth_lost`; the socket stays open for public channels.

The server ends private subscriptions, without any frame, when `auth()` succeeds as another
user, when an `auth()` fails (any error signs the connection out), or when this connection's own
session is revoked (`session.revoked` with `current: true`). The client emits `auth_changed`
(`data`: `reason` = `user_changed`, `auth_failed` or `session_revoked`, plus the `dropped`
channels) and re-subscribes those channels itself: at once for another user, after the next
successful `auth()` otherwise, followed by `resync` with `{"reason": "reauth"}` (refetch private
state).

The server can also sign a connection out by itself: `signed_out` (a planned server frame; this
SDK already handles it). Reason `expired` becomes `auth_changed` `token_expired`, reason `revoked`
becomes `session_revoked` plus `auth_lost`, and any other reason becomes `signed_out` with the raw
value in `code`. Re-send `auth()` with the fresh token on every token refresh; that keeps the
private subscriptions.

**Missed private events.** Every private frame carries a per-connection `sequence`. When numbers
are skipped (after a short reorder window, `reorder_window=0.25` seconds by default), the client
emits `sequence_gap` and `resync` with `{"reason": "sequence_gap", "channel": ...}`: refetch that
channel's state over REST. `balances.resync`, `deposits.resync` and `withdrawals.resync` (the last
two planned) emit `resync` with `balances_resync`, `deposits_resync` or `withdrawals_resync`.

### Live balances

```python
async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET) as rest:
    async with WebSocketClient(rest=rest) as ws:
        await ws.auth(session_token)
        balances = await ws.live_balances()
        balances.on("update", lambda e: print(e.data["asset"], e.data["balance"]))
        print(balances.get("USDT"), balances.stale, balances.last_error)
```

`live_balances()` subscribes `balances`, takes a REST snapshot and applies newer `balance.updated`
events (only when their `sequence` is greater than the one it holds; a total of 0 removes the row).
It refetches by itself on a missed event, `balances.resync`, `CONCURRENT_MODIFICATION`, a reconnect
or an account change, at most every `min_snapshot_interval` seconds (default 2; `0` means no minimum),
and never because a balance's own sequence skipped values. A failed snapshot or owner lookup is
retried after `retry` seconds (default 1), doubling up to 30. At the start and after every account change it checks that the REST key's account
(`account.id()`) is the WebSocket's authenticated user: otherwise nothing is merged and
`last_error.code` is `ACCOUNT_MISMATCH`. `stale` is true while a refetch is pending. With your own
`snapshot` source, also pass its owner (`owner_id` or `account_id`); without one,
`live_balances()` raises a `CONFIG` error.

## Security notes

- **API keys can never withdraw or transfer funds**, whatever their scopes.
- Use a `read`-only key unless you trade, and restrict keys to your IPs with `allowed_ips`.
- Keys are sent only as headers, only to endpoints that need them, and never in a URL.
  Never put secrets in URLs or source code.
- The SDK redacts keys from `repr()`, logs and exception messages, and never sends an
  `Authorization` header. If a server response echoes the key or secret, it is replaced
  with `***` in the exception's message, details, fields and request id.
- The SDK **never follows HTTP redirects**, even on an `http_client` you created with
  `follow_redirects=True`. A 3xx answer raises `CexyApiError` with code `UNEXPECTED_REDIRECT`
  (not retried), so keys are never re-sent to another host and an order is never re-posted.
- Connections use `https://` and `wss://` only. `allow_insecure=True` permits `http://` or
  `ws://` for a loopback host (localhost, 127.0.0.1, ::1) only, for local testing.
- Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## Authentication extension point

Credentials are applied by an `cexy.auth.Authenticator` (`apply(method, url, headers, body)`).
Today's implementation, `HeaderKeyAuth`, sends the static key headers. HMAC request signing
is planned before 1.0 and will plug in as another authenticator without changing the
resource API.

## Versioning

The SDK stays at **0.x** until API request signing ships, then moves to 1.x, which targets
`/api/v1`. Additive API changes produce minor releases. Each release notes the spec snapshot
it was built from in [CHANGELOG.md](CHANGELOG.md).

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev,codegen]"
pytest                               # offline: mocked HTTP and a local fake WebSocket server
CEXY_LIVE_TESTS=1 pytest -m live     # opt-in: 3 public, unauthenticated GETs to api.cexy.io
ruff check . && ruff format --check . && mypy
scripts/generate.sh                  # regenerate models (cexy/_generated) and sync code (cexy/_sync)
scripts/sync_spec.sh ../cexy-api-spec   # refresh the vendored spec and conformance fixtures
python tools/scan_internal.py .      # extra local patterns: $CEXY_SCAN_PATTERNS_FILE
```

`tools/scan_internal.py` is a verbatim copy from cexy-api-spec. Infrastructure-specific
patterns are not in the repository: they are read from the file named by
`CEXY_SCAN_PATTERNS_FILE` (default `~/.config/cexy/scan-patterns.txt`), which CI writes from
a repository secret.

- `spec/openapi.sdk.json` and `tests/fixtures/conformance/` are vendored from
  [cexy-api-spec](https://github.com/cexyio/cexy-api-spec); CI checks they are in sync.
- Type checking: mypy runs with `python_version = "3.10"` (current mypy no longer accepts
  3.9 as a target), while the CI test matrix still runs the suite on Python 3.9 to 3.13,
  so 3.9 runtime compatibility is covered by the tests.
- `cexy/_generated/` is generated with datamodel-code-generator; `cexy/_sync/` is generated
  from `cexy/_async/`. Edit the sources, then run `scripts/generate.sh`; CI fails on drift.

## License

MIT, see [LICENSE](LICENSE).
