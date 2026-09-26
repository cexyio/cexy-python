# Security policy

Please report security issues privately to **security@cexy.io**. Do not open a public issue for a vulnerability.

Include what you found, how to reproduce it, and its impact. We will acknowledge your report
and keep you informed while we fix it.

Never include real API keys or secrets in a report.

## Using this SDK safely

- API keys can never withdraw or transfer funds, but a `trade` key can place orders. Use a
  `read`-only key unless you need to trade, and restrict every key with `allowed_ips`.
- The SDK sends keys only as `X-API-Key`/`X-API-Secret` headers over HTTPS, only to
  endpoints that need them, and never in a URL. Do not put keys in URLs, source code or logs.
- The SDK redacts keys from its reprs, log records and exception messages, including
  any key or secret a server response echoes back (message, details, fields). Your own logging
  of request objects or environment variables is your responsibility.
