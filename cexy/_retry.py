"""Retry policy: which failures are retried and how long to wait."""

from __future__ import annotations

import random
from typing import Optional

from cexy.errors import CexyApiError, RateLimitError, retry_after_seconds

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})


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
        return err.retryable or err.status in RETRYABLE_STATUS

    def delay_for(self, err: CexyApiError, attempt: int) -> float:
        """Server-requested wait (429 Retry-After / details.retry_after_seconds, or a 409
        CONCURRENT_MODIFICATION hint) plus a little jitter; otherwise backoff."""
        server: Optional[float] = retry_after_seconds(err.headers, err.details)
        if server is not None:
            return min(server, 60.0) + random.uniform(0, 0.25)  # noqa: S311
        if isinstance(err, RateLimitError):
            return max(1.0, self.backoff(attempt))
        return self.backoff(attempt)
