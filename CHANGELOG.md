# Changelog

All notable changes to this project are documented here. The SDK stays at 0.x until API
request signing (HMAC) ships; see "Versioning" in README.md.

## 0.1.0.dev12 (2026-10-02)

### Added
- **Futures data (read only)**, `client.futures` (sync and async), for the 9 futures operations
  in cexy-api-spec `0523905`. Public: `markets()`, `market(coin)`, `order_book(coin, depth=)`,
  `candles(coin, interval, before=)`, `trades(coin, limit=)`. Account (`read` key, signed):
  `positions()`, `open_orders()`, `fills(cursor=)`, `funding(cursor=)`. Responses carry `as_of`
  and `stale`; account reads answer `has_account=False` without a futures account.
- `futures.iter_fills()` / `futures.iter_funding()`: every row across all pages, following
  the shared conformance `futures/history_paging.json` (opaque cursor sent back verbatim, page
  until `next_cursor` is null, an empty page repeating the cursor is a busy provider: back off
  and retry the same cursor up to `max_busy_retries`, default 3, independent of the client's
  request `max_retries`; a retryable error between pages, e.g. 503 with `Retry-After`, is retried
  for the same cursor by the normal retry policy).
- `PagingError` with two local errors: `PagingStalledError` (`PAGING_STALLED`, retryable) when the
  provider stays busy past `max_busy_retries`, and `PagingCursorRepeatedError`
  (`PAGING_CURSOR_REPEATED`, not retryable) when a page with rows repeats the cursor that was sent
  (its rows are yielded first). Both carry the cursor in `details["cursor"]`.
- **Futures WebSocket channels** (`cexy.ws`, conformance `ws/futures.json`): the event types
  `futures.mids`, `futures.orderbook.update`, `futures.trades.new`, `futures.candle.update`,
  `futures.status`, `futures.positions`, `futures.orders` and `futures.resync` are delivered as
  events. Channel helpers `futures_mids()`, `futures_orderbook(coin)`, `futures_trades(coin)`,
  `futures_candles(coin, interval)`, `futures_status()`, `futures_account()` (and
  `futures_channel(kind, ...)`) validate locally (`ChannelNameError`, code `CONFIG`, a
  `ValueError`; nothing is sent); `subscribe()` applies the same check to `futures.*` strings.
- `futures.account` is a private channel: subscribed before `auth`/`auth_key` it is held
  (`SubscribeResult.held`, `ws.pending_channels`) until the connection is authenticated.
- `futures.resync` emits the event plus `resync` (`{"reason": "futures_resync", "channel": ...}`);
  on `futures.account` the client also unsubscribes and subscribes again (the server's poller
  stopped). A refusal of that subscribe drops the channel and is reported.
- `SubscribeResult.errors` (refused channel -> server error) and `.held`, the
  `subscribe_refused` event, `SubscribeRefusedError` and `WebSocketClient.pending_channels`.
- Generated models for the futures schemas (`PerpMarket`, `FuturesFill`, `FuturesCandle`,
  `FuturesPublicTrade`, `Funding`, `Position`, ...). The spec's `Fill`, `Candle` and `PublicTrade`
  are renamed `Futures*` so they are not mistaken for the spot models, the same names as in the
  other CEXY SDKs.

### Fixed
- **WebSocket subscribe refusals, every channel (spot and futures).** The server answers a
  partly refused `subscribe` with an `error` frame per refused channel (carrying the request id)
  BEFORE the single `subscribed` ack, and with no ack when nothing was accepted. The client used
  to fail the whole request on the first error frame and ignore the ack, so accepted channels
  were not recorded. It now collects the errors and completes on the ack, once every channel has
  been refused, or on a timeout after at least one error (all refused). `subscribe()` returns the
  accepted channels plus the refused ones with their errors (`refused`, `errors`; also a
  `subscribe_refused` event) and raises `SubscribeRefusedError` only when every channel sent was
  refused (`TIMEOUT`/`DISCONNECTED` when no error arrived). Errors pair with the channels missing
  from the ack in sent order (spot names as the server canonicalises them: case-insensitive, `_`
  read as `/` in the symbol; futures names exactly; the last error covers any rest). An error frame
  is attributed only to the request whose id it carries. Refused channels are not held and not retried. A batch is still sent as one
  frame.
- **WebSocket subscribe contract (cexy-api-spec `ace4a5e`).** Ack names now match with the channel
  KIND exact: only the spot market symbol is canonicalised (trimmed, upper-cased, `_` read as `/`),
  so `Ticker:BTC/USDT` is no longer taken for `ticker:BTC/USDT` (it used to case-fold the kind and
  could pair an ack name with the wrong sent channel). Two spellings of one channel in a request,
  or a channel already held under another spelling, are sent once; ack names are matched as a
  multiset (the ack can repeat a name) and each accepted name is returned once. Events that arrive
  before the ack and id-less error frames (`CONCURRENT_MODIFICATION`) are covered by conformance:
  events are delivered and id-less errors are never attributed to a subscribe.
- **Held channel names (cexy-api-spec `6cea8f0`, rule 13).** An accepted channel is held under the
  server's canonical name from the ack (`ticker:btc_usdt` is held as `ticker:BTC/USDT`), so held
  names match event channels. A channel held as sent after a `TIMEOUT` is now held under the
  canonical name once a re-subscribe is acked (it used to be held under both), and is re-sent under
  it. `unsubscribe` finds a held channel under any spelling the server canonicalises alike.
- **Re-subscribes after a reconnect or a re-auth:** every refusal is reported (`subscribe_refused`);
  a private channel refused `UNAUTHENTICATED` goes back to pending (subscribed after the next
  successful auth) and any other refusal drops the channel, instead of putting every channel back to pending (re-auth) or failing
  the whole restore (reconnect).

### Changed
- A WebSocket `subscribe` that gets no ack and no error within `request_timeout` still raises
  `TIMEOUT`, but its channels now stay held, so a reconnect sends them again.
- `WebSocketClient(ping_interval=...)` must be at most 60 s (the server closes a connection whose
  client has been silent for 90-120 s); a larger value raises `ValueError`.

## 0.1.0.dev11 (2026-10-01)

### Changed
- **Breaking: request signing is the default.** `Client(api_key, api_secret)` / `AsyncClient(...)` now sign
  every private request (`auth="hmac"`); the secret is never sent. The API is switching off the
  old `X-API-Secret` mode. `auth="headers"` still selects it, for servers that accept it. Earlier
  versions default to `headers` and stop working against the API once it refuses the secret,
  unless they pass `auth="hmac"`: upgrade.
- Keys issued before 2026-10-01 can't sign (`KEY_NOT_SIGNABLE`): create a new API key before
  upgrading.
- `SIGNATURE_REQUIRED` (400, the API refuses the secret header) is never retried and its message
  names the fix (`auth="hmac"`).

## 0.1.0.dev10 (2026-10-01)

### Added
- Error codes from the live API: `KEY_NOT_SIGNABLE`, `SIGNATURE_EXPIRED`, `NONCE_REUSED` and
  `SIGNATURE_REQUIRED` (`KNOWN_ERROR_CODES`). `SIGNATURE_REQUIRED` is reserved: the API will return it
  (400, not retryable) once header mode is switched off; switch to `hmac` before then.
- Request signing, accepted by the API since 2026-10-01 (opt-in; the default is unchanged):
  `Client(api_key, api_secret, auth="hmac")` / `AsyncClient(..., auth="hmac")` sign every private
  request (`CEXY-HMAC-SHA256-v1`: `X-API-Key`, `X-API-Timestamp`, `X-API-Nonce`,
  `X-API-Signature`) instead of sending `X-API-Secret`. Every attempt, retries included, is signed
  with a fresh timestamp and nonce. After `SIGNATURE_EXPIRED` the client adopts the server clock
  (at most 1 h away) and resends once. `KEY_NOT_SIGNABLE` (a key issued before signing) raises an
  error that names the fix; there is no fallback to `X-API-Secret`. Checked against the spec's
  signing vectors and a test server that verifies every signature from the raw request it received.
- WebSocket `auth_key()`: authenticates with the API key by signing the server's
  single-use challenge; re-signs the new challenge after each reconnect; stops automatic key
  re-auth after a refused key; `key_revoked` / `key_expired` sign-outs. The signer comes from
  `rest=AsyncClient(..., auth="hmac")` or `key_signer=`. `WebSocketClient.auth_kind` says how the
  connection is authenticated.

### Changed
- Query strings are built by the SDK with RFC 3986 encoding (`%20` for a space, `%2B` for a plus)
  instead of by httpx (`+` for a space). The server decodes both the same way; this makes a signed
  request exactly the sent one.
- WebSocket: the `resync` event for a `CONCURRENT_MODIFICATION` error frame now carries
  `{"reason": "concurrent_modification"}` (lowercase, like every other resync reason and the other
  SDKs) instead of `"CONCURRENT_MODIFICATION"`. **Behaviour change** for code that compared the
  uppercase value.

### Fixed
- `LiveBalances`: events that arrived while the owner lookup was in flight are dropped when the
  lookup ends in `ACCOUNT_MISMATCH` (they were kept until the next snapshot).

## 0.1.0.dev9 (2026-09-30)

### Added
- `account.id()` (sync and async): the account id of the API key (`GET /api/v1/account/id`, read
  scope).
- `WebSocketClient.live_balances()` / `LiveBalances`: live balances from a REST snapshot plus
  `balance.updated` events. An event applies only when its `sequence` is greater than the stored one
  (a total of 0 removes the row, and an older snapshot row cannot bring it back); a refetch happens
  on a missed event, `balances.resync`, `CONCURRENT_MODIFICATION`, a reconnect or an account
  change, at most every `min_snapshot_interval` seconds (default 2), with retry backoff. At the start and after every account change the REST key's account (`account.id()`) must be the WebSocket's user, otherwise nothing is
  merged (`AccountMismatchError`, `ACCOUNT_MISMATCH`). A custom `snapshot` source must name its owner (`owner_id` or
  `account_id`), otherwise `live_balances()` raises a `CONFIG` error. Events without `sequence` (older servers)
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
  `signed_out` with the raw reason in `code` (`"unknown"` when the frame has none). The token is
  forgotten.
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
- `cancel_all` follows the API's cancel-all update: the response adds
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
