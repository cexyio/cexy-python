"""Shared logic of the opt-in cancel-all loop (``cancel_all(..., until_done=True)``).

The rules follow ``tests/fixtures/conformance/trading/cancel_all_until_done.json`` (from
cexy-api-spec), which every SDK implements:

- every round is exactly ONE HTTP request: the transport does not retry inside the loop, so
  the loop never sends more than ``max_rounds`` requests;
- repeat while ``has_more`` is true or any failure code is ``INVALID_STATE`` or
  ``SERVICE_UNAVAILABLE`` (an order still being placed, or its state unreadable);
- after a round that made no progress (nothing cancelled or already closed), sleep 1, 2, 4, 8,
  then 15 s before the next call; reset after any progress; no sleep after progress;
- a retryable error (429, a retryable 5xx, a network failure) is a round without progress:
  after a 429 the loop waits the server's Retry-After exactly, without advancing the backoff;
  after any other retryable error it waits the next backoff step;
- stop when done, after ``max_rounds`` calls, or when the next wait would bring the elapsed
  time to or past ``time_budget`` seconds (that wait is not taken; ``last_error_code`` names
  the error that caused it, if any);
- a non-retryable error is raised, with the merged result so far attached as ``.partial``;
- merge the rounds by order id: an order's latest state wins.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Tuple

from cexy._generated import models as m

RETRY_CODES = frozenset({"INVALID_STATE", "SERVICE_UNAVAILABLE"})
BACKOFF_S: Tuple[float, ...] = (1.0, 2.0, 4.0, 8.0, 15.0)
Stopped = Literal["done", "max_rounds", "time_budget", "error"]


@dataclass
class CancelAllResult:
    """The merged outcome of ``cancel_all(..., until_done=True)``.

    Every order the calls handled is in exactly one of ``cancelled``, ``already_closed`` and
    ``failed`` (its latest state). ``failures`` explains each order still in ``failed``.
    ``stopped`` is ``"done"``, ``"max_rounds"`` or ``"time_budget"`` (``"error"`` only on the
    ``.partial`` result attached to a raised error); ``has_more`` is the last successful call's
    value (true means open orders may remain: call again later). ``last_error_code`` is the
    code of the error in the last round, if that round failed (e.g. ``RATE_LIMITED``).
    """

    cancelled: List[str] = field(default_factory=list)
    already_closed: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    failures: List[m.CancelFailureResponse] = field(default_factory=list)
    has_more: bool = False
    rounds: int = 0
    stopped: Stopped = "done"
    last_error_code: Optional[str] = None

    @property
    def complete(self) -> bool:
        """True when the loop finished because nothing was left to retry."""
        return self.stopped == "done"


class _Merge:
    def __init__(self) -> None:
        self.state: Dict[str, str] = {}
        self.failure: Dict[str, m.CancelFailureResponse] = {}
        self.has_more = True  # unknown until a call succeeds

    def add(self, r: m.CancelAllResponse) -> None:
        self.has_more = r.has_more
        for oid in r.cancelled:
            self._set(oid, "cancelled")
        for oid in r.already_closed:
            self._set(oid, "already_closed")
        by_id = {f.order_id: f for f in r.failures}
        for oid in r.failed:
            self._set(oid, "failed")
            if oid in by_id:
                self.failure[oid] = by_id[oid]

    def _set(self, oid: str, state: str) -> None:
        self.state.pop(oid, None)  # latest state wins
        self.state[oid] = state
        if state != "failed":
            self.failure.pop(oid, None)

    def _pick(self, state: str) -> List[str]:
        return [o for o, st in self.state.items() if st == state]

    def result(self, rounds: int, stopped: Stopped, last_error_code: Optional[str] = None) -> CancelAllResult:
        failed = self._pick("failed")
        return CancelAllResult(
            cancelled=self._pick("cancelled"),
            already_closed=self._pick("already_closed"),
            failed=failed,
            failures=[self.failure[o] for o in failed if o in self.failure],
            has_more=self.has_more,
            rounds=rounds,
            stopped=stopped,
            last_error_code=last_error_code,
        )


def should_continue(r: m.CancelAllResponse) -> bool:
    return r.has_more or any(f.code in RETRY_CODES for f in r.failures)


def made_progress(r: m.CancelAllResponse) -> bool:
    return bool(r.cancelled or r.already_closed)


def next_backoff(streak: int) -> float:
    return BACKOFF_S[min(streak, len(BACKOFF_S) - 1)]
