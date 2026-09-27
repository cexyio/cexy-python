# Changelog

All notable changes to this project are documented here. The SDK stays at 0.x until API
request signing (HMAC) ships; see "Versioning" in README.md.

## 0.1.0.dev4 (unreleased)

### Added
- `cancel_all` follows the API's cancel-all update (backend release 4d1b7f2): the response adds
  `already_closed` (orders that closed on their own; not an error), `failures` (`order_id`,
  `code`, `message` per failed order) and `has_more` (more than 500 open orders: call again).
  An unknown `symbol` raises `NotFoundError`; the 30-calls-a-minute limit raises `RateLimitError`.
- `cancel_all(symbol=..., until_done=True, max_rounds=20, time_budget=120.0)`: repeats the call
  while `has_more` or while orders are still being placed (`INVALID_STATE`) or unreadable
  (`SERVICE_UNAVAILABLE`), backs off 1-2-4-8-15 s after rounds without progress, honours a
  429's `Retry-After` within the budget, and returns a merged `CancelAllResult` (`rounds`,
  `stopped`). Implements the shared `conformance/trading/cancel_all_until_done.json` cases. The
  default stays a single call.

### Changed
- The transport accepts a `deadline`: a retry whose wait (the server's full Retry-After) would
  reach it is not attempted.

## 0.1.0.dev3 (2026-09-27)

### Security
- **Redirects are never followed**, including on a caller-supplied `http_client` created with
  `follow_redirects=True`. Earlier versions set `follow_redirects=False` only on the client they
  created themselves, so a caller's following client re-sent `X-API-Key` and `X-API-Secret` to a
  redirect target (httpx strips only `Authorization`), and a 307/308 re-posted an order. A 3xx
  was also treated as success. Every request now passes `follow_redirects=False`, and a 3xx
  raises `CexyApiError` with code `UNEXPECTED_REDIRECT`, which is not retried. Upgrade from
  0.1.0.dev2 or earlier if you pass your own `http_client`.

## 0.1.0.dev2 (2026-09-27)

- `JurisdictionBlockedError` (a `ForbiddenError` subclass) for `JURISDICTION_BLOCKED` / HTTP 451.
- README: pre-releases install with `pip install --pre cexy` (the PyPI page for 0.1.0.dev1 still says `pip install cexy`, which does not install a pre-release).
- `spec/errors.yaml` is now generated upstream from the spec's `ErrorCode` (same 44 codes).

## 0.1.0.dev1 (2026-09-27)

First published pre-release. 0.1.0.dev0 was tagged but never reached PyPI: the pinned publish
action (v1.12.4, twine 6.1.0) rejected wheel metadata version 2.5. The publish action is now
v1.14.2 (twine 7.0.0), and the build job runs `twine check --strict` with the same twine.

## 0.1.0.dev0 (not published)

Built from the `cexy-api-spec` snapshot (implementation notes stripped from descriptions) `spec/openapi.sdk.json` (public spec `info.version`
1.0.0, 40 allowlisted operations).

- Sync `Client` and asyncio `AsyncClient` covering all 40 SDK operations: public market
  data, account, exports, wallet reads, trading and liquidity pools.
- Typed pydantic v2 models generated from the spec; every amount is a `decimal.Decimal`,
  requests send amounts as strings and reject floats.
- API-key header authentication behind an `Authenticator` extension point (for HMAC
  signing later); secrets redacted from reprs, logs and exceptions.
- Error hierarchy from `errors.yaml` (provisional), unknown codes tolerated.
- Retries with exponential backoff and jitter and `Retry-After` handling. Order safety
  rests on `client_order_id` plus a by-client-id lookup (the server ignores
  `Idempotency-Key` on order endpoints). A cancel retried into `INVALID_STATE` returns the
  order's current state. Pool join/exit send an automatic `Idempotency-Key`.
- Client-side token-bucket rate limiter (100/min without a key, 300/min with one) adapting
  to `X-RateLimit-*` (`X-RateLimit-Reset` in seconds).
- Cursor pagination with `auto_paging_iter()`.
- WebSocket client (`cexy.ws`) with id-correlated acknowledgements (auth waits for
  `authenticated`), heartbeat, reconnect/resubscribe and an `OrderBook`
  helper that follows the snapshot/sequence rules.
