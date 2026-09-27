# Changelog

All notable changes to this project are documented here. The SDK stays at 0.x until API
request signing (HMAC) ships; see "Versioning" in README.md.

## 0.1.0.dev2 (unreleased)

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
