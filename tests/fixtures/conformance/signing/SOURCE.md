# Signing vectors: provenance

`vectors.json` is copied byte for byte from the exchange server's own vector generator at server
commit `1874f09`. A server-side test fails if the file drifts from the signing code.

- It **must equal the server's `docs/signing-vectors.json` at the deployed signing commit**. If the
  server regenerates the vectors (any change to the canonical string), this file is replaced in
  lockstep before any SDK merges signing code.
- SHA-256: `d9b2c155a8ee8887c6361ef80d9a571df3a35577f9fdb04d636fed14dfce262d`
- Every key id, secret, nonce, timestamp, connection id and challenge in it is a fixed test
  constant. None of them is, or derives from, a real key or server secret. The secret scanner
  exempts exactly this file (`.gitleaks.toml`).
- The vector key id `ak_vector0000000` is outside the issuable key-id space: real key ids are `ak_`
  plus 24 lowercase hex digits, and the server refuses any other id before any lookup, on every
  key path. The vector secret was never stored for any key. (Confirmed in writing by the exchange
  backend, 2026-09-30.)
- Status: the scheme is **planned**. The API does not accept signed requests or `auth_key` yet;
  SDKs keep sending `X-API-Key` / `X-API-Secret` until it is live.

What SDKs must reproduce, for every entry in `rest`: `canonical_path`, `canonical_query`,
`body_sha256`, `canonical_request` and `headers` (the signature). For `ws`, the `message` and the
`auth_key` frame. For `negative`, only the right secret's signature may verify. The scheme
(canonical path and query rules, the ±30 s window, `SIGNATURE_EXPIRED`, `NONCE_REUSED`,
`KEY_NOT_SIGNABLE`) will be described in `README.md` and `asyncapi.yaml` once it goes live.
