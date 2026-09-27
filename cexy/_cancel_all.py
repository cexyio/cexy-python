"""Shared logic of the opt-in cancel-all loop (``cancel_all(..., until_done=True)``).

The rules follow ``tests/fixtures/conformance/trading/cancel_all_until_done.json`` (from
cexy-api-spec), which every SDK implements:

- repeat while ``has_more`` is true or any failure code is ``INVALID_STATE`` or
  ``SERVICE_UNAVAILABLE`` (an order still being placed, or its state unreadable);
- after a round that made no progress (nothing cancelled or already closed), sleep 1, 2, 4, 8,
  then 15 s before the next call; reset after any progress; no sleep after progress;
- stop when done, after ``max_rounds`` calls, or when the next sleep would bring the elapsed
  time to or past ``time_budget`` seconds; transport retry waits (e.g. a 429's Retry-After)
  count against the budget, and a retry that would pass it is not attempted;
- merge the rounds by order id: an order's latest state wins.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Tuple

from cexy._generated import models as m

RETRY_CODES = frozenset({"INVALID_STATE", "SERVICE_UNAVAILABLE"})
BACKOFF_S: Tuple[float, ...] = (1.0, 2.0, 4.0, 8.0, 15.0)
Stopped = Literal["done", "max_rounds", "time_budget"]


@dataclass
class CancelAllResult:
    """The merged outcome of ``cancel_all(..., until_done=True)``.

    Every order the calls handled is in exactly one of ``cancelled``, ``already_closed`` and
    ``failed`` (its latest state). ``failures`` explains each order still in ``failed``.
    ``stopped`` is ``"done"``, ``"max_rounds"`` or ``"time_budget"``; ``has_more`` is the last
    call's value (true means open orders may remain: call again later).
    """

    cancelled: List[str] = field(default_factory=list)
    already_closed: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    failures: List[m.CancelFailureResponse] = field(default_factory=list)
    has_more: bool = False
    rounds: int = 0
    stopped: Stopped = "done"

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

    def result(self, rounds: int, stopped: Stopped) -> CancelAllResult:
        failed = self._pick("failed")
        return CancelAllResult(
            cancelled=self._pick("cancelled"),
            already_closed=self._pick("already_closed"),
            failed=failed,
            failures=[self.failure[o] for o in failed if o in self.failure],
            has_more=self.has_more,
            rounds=rounds,
            stopped=stopped,
        )


def should_continue(r: m.CancelAllResponse) -> bool:
    return r.has_more or any(f.code in RETRY_CODES for f in r.failures)


def made_progress(r: m.CancelAllResponse) -> bool:
    return bool(r.cancelled or r.already_closed)


def next_backoff(streak: int) -> float:
    return BACKOFF_S[min(streak, len(BACKOFF_S) - 1)]
