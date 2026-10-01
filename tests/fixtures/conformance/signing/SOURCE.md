# Signing vectors: provenance

`vectors.json` is copied byte for byte from the exchange server's own vector generator at server
commit `6d40fb4` (regenerated from `1874f09`: two query cases added, the nine earlier `rest`
cases unchanged byte for byte). A server-side test fails if the file drifts from the signing code.

- It **must equal the server's `docs/signing-vectors.json` at the deployed signing commit**. If the
  server regenerates the vectors (any change to the canonical string), this file is replaced in
  lockstep before any SDK merges signing code.
- SHA-256: `a86957e5db8a5d5154ec7c5ea99cfffd27a7eac0257c2166105acd49e4047075`
- Every key id, secret, nonce, timestamp, connection id and challenge in it is a fixed test
  constant. None of them is, or derives from, a real key or server secret. The secret scanner
  exempts exactly this file (`.gitleaks.toml`).
- The vector key id `ak_vector0000000` is outside the issuable key-id space: real key ids are `ak_`
  plus 24 lowercase hex digits, and the server refuses any other id before any lookup, on every
  key path. The vector secret was never stored for any key. (Confirmed in writing by the exchange
  backend, 2026-09-30.)
- Status: the scheme is **planned**. The API does not accept signed requests or `auth_key` yet;
  SDKs keep sending `X-API-Key` / `X-API-Secret` until it is live.

Query rules the new cases pin down: the query is everything after the FIRST `?` (so a second `?`
is data and canonicalises to `%3F`), and empty `&`-separated parts are dropped. A `%` must be
followed by two hex digits; the server refuses anything else (400), so SDKs must never produce it
(encode a literal `%` as `%25`).

What SDKs must reproduce, for every entry in `rest`: `canonical_path`, `canonical_query`,
`body_sha256`, `canonical_request` and `headers` (the signature). For `ws`, the `message` and the
`auth_key` frame. For `negative`, only the right secret's signature may verify. The scheme
(canonical path and query rules; the timestamp window of 30 s behind and at most 5 s ahead of the
server clock, outside which the answer is `SIGNATURE_EXPIRED` with `details.server_time_ms`; the
retryable 503 `SERVICE_UNAVAILABLE` with `details.reason` `nonce_store_warming` and `Retry-After`
for a few seconds after the server's nonce store restarts empty; `NONCE_REUSED`;
`KEY_NOT_SIGNABLE`) will be described in `README.md` and `asyncapi.yaml` once it goes live.
