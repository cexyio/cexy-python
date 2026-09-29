"""Retry policy: which failures are retried and how long to wait."""

from __future__ import annotations

import random
from typing import Optional

from cexy.errors import CexyApiError, RateLimitError, retry_after_seconds

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
#: The longest wait a server hint (Retry-After, retry_after_seconds, X-RateLimit-Reset) may cause.
#: A longer requested wait is not honoured: the call fails at once with the server's error.
MAX_SERVER_WAIT_S = 120.0


class RetryPolicy:
    def __init__(self, max_retries: int = 3, base_delay: float = 0.5, max_delay: float = 8.0) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter. ``attempt`` is 0 for the first retry."""
        cap = min(self.max_delay, self.base_delay * (2**attempt))
        return random.uniform(0, cap)  # noqa: S311 - jitter, not security

    @staticmethod
    def is_retryable(err: CexyApiError) -> bool:
        """A 4xx is never retryable except 429 and 409 CONCURRENT_MODIFICATION (and those only
        when the server marks them retryable), whatever the body says."""
        concurrent = err.status == 409 and err.code == "CONCURRENT_MODIFICATION"
        if 400 <= err.status < 500 and err.status != 429 and not concurrent:
            return False
        return err.retryable or err.status in RETRYABLE_STATUS

    @staticmethod
    def server_wait(err: CexyApiError) -> Optional[float]:
        """The server's requested wait in seconds, if it sent a usable one (may exceed
        ``MAX_SERVER_WAIT_S``; the transport does not retry then)."""
        return retry_after_seconds(err.headers, err.details)

    def delay_for(self, err: CexyApiError, attempt: int) -> float:
        """Server-requested wait (429 Retry-After / details.retry_after_seconds, or a 409
        CONCURRENT_MODIFICATION hint) plus a little jitter; otherwise backoff."""
        server: Optional[float] = self.server_wait(err)
        if server is not None:
            return min(server, MAX_SERVER_WAIT_S) + random.uniform(0, 0.25)  # noqa: S311
        if isinstance(err, RateLimitError):
            return max(1.0, self.backoff(attempt))
        return self.backoff(attempt)
