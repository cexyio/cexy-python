"""Client-side token bucket.

The client picks 100 requests/minute without a key (the server allows about 120/min
per IP for anonymous requests) and 300/min with a key (about 600/min per key). When the
server sends ``X-RateLimit-Remaining`` / ``X-RateLimit-Reset``, the bucket adapts: it
never holds more tokens than the server says remain, and when the server says zero
remain it waits until the reset. Server headers are untrusted: unusable values are ignored and
no wait exceeds 120 s.
"""

from __future__ import annotations

import math
import random
import threading
import time
from typing import Callable, Mapping, Optional

#: The limiter never blocks longer than this because of a server hint (same as the transport's
#: MAX_SERVER_WAIT_S).
MAX_WAIT_S = 120.0

#: Jitter (0 to this many seconds) added to a wait caused by a server hold, so requests held by
#: the same 429 do not all fire at the same instant.
HOLD_JITTER_S = 0.25


class TokenBucket:
    def __init__(
        self,
        per_minute: float = 300,
        burst: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if per_minute <= 0:
            raise ValueError("per_minute must be positive")
        self.rate = per_minute / 60.0
        self.capacity = float(burst if burst is not None else max(1.0, per_minute / 6))
        self.tokens = self.capacity
        self._clock = clock
        self._last = clock()
        self._blocked_until = 0.0
        self._lock = threading.Lock()

    def _refill(self, now: float) -> None:
        # A clock that moved backwards (or was swapped) refills nothing.
        self.tokens = min(self.capacity, self.tokens + max(0.0, now - self._last) * self.rate)
        self._last = now

    def acquire(self) -> float:
        """Take one token. Returns how long the caller must sleep first (0 if none)."""
        with self._lock:
            now = self._clock()
            self._refill(now)
            held = max(0.0, self._blocked_until - now)
            wait = held
            self.tokens -= 1.0
            if self.tokens < 0:
                wait = max(wait, -self.tokens / self.rate)
            if held > 0:
                wait += random.uniform(0, HOLD_JITTER_S)  # noqa: S311
            return min(wait, MAX_WAIT_S)

    def block_for(self, seconds: float) -> None:
        """Hold every request until ``seconds`` from now (a 429 hint: ``Retry-After`` or
        ``details.retry_after_seconds``). Capped at ``MAX_WAIT_S``, so a hint above 120 s still
        holds the client for 120 s (the failing request itself is not retried). An existing,
        later hold is never shortened. Unusable values (not finite, <= 0) are ignored."""
        if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0:
            return
        with self._lock:
            until = self._clock() + min(float(seconds), MAX_WAIT_S)
            self._blocked_until = max(self._blocked_until, until)

    def pending_wait(self) -> float:
        """How long the next ``acquire()`` would make the caller sleep, without taking a token."""
        with self._lock:
            now = self._clock()
            self._refill(now)
            wait = max(0.0, self._blocked_until - now)
            if self.tokens < 1.0:
                wait = max(wait, (1.0 - self.tokens) / self.rate)
            return min(wait, MAX_WAIT_S)

    def update_from_headers(self, headers: Mapping[str, str]) -> None:
        """Adapt to ``X-RateLimit-Remaining`` and ``X-RateLimit-Reset`` (case-insensitive keys)."""
        low = {k.lower(): v for k, v in headers.items()}
        try:
            remaining = float(low["x-ratelimit-remaining"])
        except (KeyError, ValueError, TypeError):
            return
        if not math.isfinite(remaining):
            return
        remaining = max(0.0, remaining)  # untrusted: a negative count would block for ever
        reset = self._reset_delay(low.get("x-ratelimit-reset"))
        with self._lock:
            now = self._clock()
            self._refill(now)
            self.tokens = min(self.tokens, remaining)
            if remaining <= 0:
                self._blocked_until = max(self._blocked_until, now + (reset if reset is not None else 1.0))

    def _reset_delay(self, raw: Optional[str]) -> Optional[float]:
        # X-RateLimit-Reset is the number of seconds until the window resets.
        if raw is None:
            return None
        try:
            value = float(raw)
        except (ValueError, TypeError):
            return None
        if not math.isfinite(value) or value < 0:
            return None
        return min(value, MAX_WAIT_S)
