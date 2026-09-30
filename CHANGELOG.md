# Changelog

All notable changes to this project are documented here. The SDK stays at 0.x until API
request signing (HMAC) ships; see "Versioning" in README.md.

## 0.1.0.dev9 (2026-09-30)

### Added
- `account.id()` (sync and async): the account id of the API key (`GET /api/v1/account/id`, read
  scope).
- `WebSocketClient.live_balances()` / `LiveBalances`: live balances from a REST snapshot plus
  `balance.updated` events. An event applies only when its `sequence` is greater than the stored one
  (a total of 0 removes the row, and an older snapshot row cannot bring it back); a refetch happens
  on a missed event, `balances.resync`, `CONCURRENT_MODIFICATION`, a reconnect or an account
  change, at most every `min_snapshot_interval` seconds (default 2), with retry backoff. Before every
  merge the REST key's account (`account.id()`) must be the WebSocket's user, otherwise nothing is
  merged (`AccountMismatchError`, `ACCOUNT_MISMATCH`). Events without `sequence` (older servers)
  always apply and log one warning. `stale`, `last_error`, `get()`, `all()`, `close()`, `on()` with
  `update`, `snapshot`, `error`.
- WebSocket: frame-sequence tracking on private channels. A gap that is not filled within
  `reorder_window` seconds (default 0.25; channels with several publishers can swap adjacent frames)
  emits `sequence_gap` and `resync` `{"reason": "sequence_gap", "channel": ...}`.
- WebSocket: `balances.resync` (and the planned `deposits.resync` / `withdrawals.resync`) are known
  events and emit `resync` with `balances_resync`, `deposits_resync` or `withdrawals_resync`.
- WebSocket: the planned `signed_out` server frame is handled as a server sign-out: `expired` gives
  `auth_changed` `token_expired`, `revoked` gives `session_revoked` plus `auth_lost` (data
  `{"session_id": None, "reason": "signed_out", "current": True}`), any other reason gives
  `signed_out` with the raw reason in `code`. The token is forgotten.
- `BalanceResponse.sequence` (a missing value decodes as 0), `Clock` / `clock=` (test-only time
  source), `REAL_CLOCK`.

## 0.1.0.dev8 (2026-09-30)

### Fixed
- WebSocket: private channels no longer go silent after a server-side sign-out. The server ends
  every private subscription (without a frame) when `auth()` succeeds as another user, when an
  `auth()` fails, or when this connection's own session is revoked. The client kept those
  channels (on a revoked session and a failed auth) or never noticed the switch, so
  `subscribe()` for them sent nothing. It now drops them, emits the new `auth_changed` event
  (`data`: `reason`, `previous_user_id`, `user_id`, `code`, `dropped`) and re-subscribes them:
  at once after a switch to another user, after the next successful `auth()` otherwise,
  followed by `resync` with `{"reason": "reauth"}`. Re-authenticating as the same user changes
  nothing.
- WebSocket: a refused `auth()` token is forgotten, so it is not re-sent after a reconnect.
- WebSocket: `authenticated` and `user_id` are updated as the server's reply arrives.

### Changed
- WebSocket: `session.revoked` acts only when `data.current` is exactly `true` (this connection's
  own session). Another session's revocation (`current: false`) no longer emits `auth_lost` or
  forgets the token, and the connection stays authenticated. **Behaviour change.**

### Added
- `WebSocketClient.has_token`, the `AUTH_CHANGED` event type.
- Conformance: runs `conformance/ws/private_signout.json` (vendored) against a scripted server.

## 0.1.0.dev7 (2026-09-29)

### Added
- `account.sub_account_balances(id)` (sync and async): a sub-account's balances, read by its parent
  account (`GET /account/sub-accounts/{id}/balances`, read scope). Same shape as `balances()`,
  including `held_incoming`. An id that is not the caller's sub-account raises `NotFoundError`
  (not retried); an empty id raises `ValueError` before any request.
- `BalanceResponse.held_incoming` (`HeldIncomingResponse`: `transfer_id`, `amount`, `available_at`):
  incoming internal transfers still held, at most 100, soonest first. Their sum is already included
  in `locked`: never add it again. A server that omits the field decodes as `[]`.

### Changed
- A 4xx response is never retried except 429 and 409 `CONCURRENT_MODIFICATION`, even when its body
  says `retryable: true` (this includes 408). Those two are still retried only where they were before:
  a 429 always, a 409 `CONCURRENT_MODIFICATION` when the server marks it retryable. A mutation is retried only when it is repeat-safe: pool join/exit with their
  `Idempotency-Key`, `place_order` (through its `client_order_id`), `cancel_order` and `cancel_all`;
  any other mutation is sent once.

### Security
- Path values `"."` and `".."` are rejected with `ValueError`: previously they escaped their URL
  segment, so e.g. `sub_account_balances("..")` returned the parent's own balances and
  `order_by_client_id("..")` the open-orders list. A write request could only be redirected to a
  route that does not exist and is refused by the server; no write could reach a different operation.

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
