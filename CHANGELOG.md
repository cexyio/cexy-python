# Changelog

All notable changes to this project are documented here. The SDK stays at 0.x until API
request signing (HMAC) ships; see "Versioning" in README.md.

## 0.1.0.dev0 (unreleased)

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
