# Changelog

All notable changes to this project are documented here. The SDK stays at 0.x until API
request signing (HMAC) ships; see "Versioning" in README.md.

## 0.1.0.dev6 (2026-09-28)

Synced with the API's H-1 release.

### Added
- `LedgerEntryResponse.reference` is typed: a union told apart by `type` (`LedgerReferenceDeposit`,
  `LedgerReferenceWithdrawal`, `LedgerReferenceOrder`, `LedgerReferenceTrade`,
  `LedgerReferenceTransfer`, `LedgerReferenceAdjustment`, `LedgerReferencePool`,
  `LedgerReferenceFuturesTransfer`, `LedgerReferenceSystem`), exported with the `LedgerReference`
  alias from `cexy.models`. An unknown `type`, a reference missing a required field or with a field
  of the wrong type, or a value that is not an object decodes to `LedgerReferenceUnknown` (all
  fields kept) instead of failing: decoding a reference never raises.
- Id aliases `DepositId`, `FuturesTransferId`, `OrderId`, `PoolId`, `TradeId`, `UserId`,
  `WithdrawalId`: plain `str`, with no client-side format check.
- Error code `PRICE_UNAVAILABLE` (HTTP 422, raised as `UnprocessableError`).
- `WithdrawalStatus.REVERTED`, a withdrawal that failed on chain and was refunded. New
  `LedgerEntryKind` values: `transfer_in_held`, `transfer_release`, `transfer_reversal`,
  `withdrawal_refund`, `withdrawal_fee_revenue_reversal`.

### Changed
- `JoinPoolRequest.max_ratio_deviation_percent` is an amount in the spec now; it was already a
  `Decimal` here.
- `cancel_all` also cancels stop orders that have not triggered yet (`pending_trigger`), releasing
  their reservations (server behaviour since H-1; docstring and README say so).

### Fixed
- `cancel_all(until_done=True)`: a wait imposed by the client rate limiter (for example after a
  response with `X-RateLimit-Remaining: 0` and a `X-RateLimit-Reset`) now counts against
  `time_budget`. If it would reach the budget, the loop stops with `stopped="time_budget"` and
  `last_error_code="RATE_LIMITED"` instead of making the next request late.

### CI
- New `consumer` job, also run by the publish build job: the wheel and the sdist are each
  installed into a fresh venv (no extras, no dev dependencies) and smoke-tested from outside the
  repository (`ci/consumer/`), so a packaging or dependency gap can't hide behind the dev setup.

## 0.1.0.dev5 (2026-09-27)

### Fixed
- **Server-controlled waits are bounded.** `Retry-After` (seconds or HTTP-date),
  `details.retry_after_seconds` and `X-RateLimit-Reset` are treated as untrusted: unparseable,
  negative or non-finite values are ignored instead of raising, and a requested wait above 120 s is
  no longer taken (earlier versions waited up to 60 s and retried): the call raises `RateLimitError`
  at once, with `.retry_after` giving the server's value. The client-side rate limiter never blocks
  longer than 120 s because of a server header, ignores negative or non-finite counts, and no longer
  loses tokens when its clock goes backwards. Retry-After HTTP-dates are now understood.
- **`cancel_all(until_done=True)` owns its retries.** Each round is exactly one HTTP request:
  earlier versions let the transport retry inside a round, so 20 rounds could send up to 80
  requests. A retryable error (429, 5xx, network) is now a round without progress; a 429 waits the
  server's Retry-After exactly and a wait past `time_budget` is not taken. New
  `CancelAllResult.last_error_code`; a non-retryable error is raised with the merged result so far
  in `err.partial`.

## 0.1.0.dev4 (2026-09-27)

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
