"""Batch place and batch cancel: local checks and the reading of a result after retries.

Shared by the async and sync clients; the HTTP calls stay in ``resources.py``. The rules are the
shared conformance cases in ``cexy-api-spec/conformance/trading/batch_orders.json``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence

from cexy._generated import models as m

MAX_BATCH_ORDERS = 20
MAX_CANCEL_ENTRIES = 50
#: The server's rule for a client order id (after it trims surrounding whitespace, which the SDK
#: refuses instead, so the id in a result is always the one sent).
CLIENT_ORDER_ID = re.compile(r"[A-Za-z0-9_\-.:]{1,64}")

PlaceOutcome = Literal["placed", "failed", "recovered", "unknown"]
CancelOutcome = Literal["cancelled", "failed", "recovered", "unknown"]


@dataclass(frozen=True)
class BatchOrderOutcome:
    """One order of ``place_orders``, in request order.

    ``outcome``:

    - ``placed``: the server placed it now; ``order`` and ``fills`` are set.
    - ``failed``: refused; ``error`` says why. ``ALREADY_EXISTS`` here means the id was used
      before this call.
    - ``recovered``: an earlier attempt of this call placed it (its answer was lost), so the retry
      was answered ``ALREADY_EXISTS``. ``order_id`` and ``status`` say what became of it; ``order``
      is set only when the SDK had to look it up. A ``rejected`` status means it was refused
      (funds): its id is spent and it must not be placed again under that id.
    - ``unknown``: as ``recovered``, but its state could not be read. It was not placed again;
      look it up with ``order_by_client_id``.
    """

    index: int
    client_order_id: str
    outcome: PlaceOutcome
    order_id: Optional[str] = None
    status: Optional[str] = None
    order: Optional[m.OrderResponse] = None
    fills: List[m.FillResponse] = field(default_factory=list)
    error: Optional[m.ErrorBody] = None


@dataclass(frozen=True)
class BatchPlaceResult:
    """What ``place_orders`` did. The counts are computed from ``results``: ``placed`` counts
    placed and recovered orders that were not rejected, ``failed`` the refused ones (a recovered
    ``rejected`` included), ``unknown`` those whose state could not be read."""

    results: List[BatchOrderOutcome]
    placed: int
    failed: int
    unknown: int


@dataclass(frozen=True)
class CancelEntryOutcome:
    """One entry of ``cancel_orders``, in request order.

    ``outcome``: ``cancelled`` (``order`` is the cancelled order), ``failed`` (``error`` says
    why), ``recovered`` (an earlier attempt of this call may have cancelled it, so the retry was
    answered ``INVALID_STATE``; ``order`` is its current state and ``status`` says cancelled or,
    for example, filled), or ``unknown`` (as recovered, but its state could not be read).
    """

    index: int
    outcome: CancelOutcome
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    status: Optional[str] = None
    order: Optional[m.OrderResponse] = None
    error: Optional[m.ErrorBody] = None


@dataclass(frozen=True)
class BatchCancelResult:
    """What ``cancel_orders`` did. ``cancelled`` counts entries now cancelled (recovered ones
    included), ``failed`` the rest that have a known state, ``unknown`` those that do not."""

    results: List[CancelEntryOutcome]
    cancelled: int
    failed: int
    unknown: int


def check_client_order_id(cid: Any, where: str) -> str:
    if not isinstance(cid, str) or not CLIENT_ORDER_ID.fullmatch(cid):
        raise ValueError(f"{where}: client_order_id must be 1 to 64 characters of A-Z a-z 0-9 _ - . :")
    return cid


def check_place(orders: Sequence[Any]) -> None:
    if not 1 <= len(orders) <= MAX_BATCH_ORDERS:
        raise ValueError(f"place_orders takes 1 to {MAX_BATCH_ORDERS} orders, got {len(orders)}")
    seen = set()
    for i, o in enumerate(orders):
        cid = o.get("client_order_id")
        if cid is None:
            continue
        check_client_order_id(cid, f"orders[{i}]")
        if cid in seen:
            raise ValueError(f"orders[{i}]: client_order_id {cid!r} is used twice in this batch")
        seen.add(cid)


def check_cancel(entries: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    if not 1 <= len(entries) <= MAX_CANCEL_ENTRIES:
        raise ValueError(f"cancel_orders takes 1 to {MAX_CANCEL_ENTRIES} entries, got {len(entries)}")
    out: List[Dict[str, Any]] = []
    for i, e in enumerate(entries):
        oid, cid = e.get("order_id"), e.get("client_order_id")
        if (oid is None or oid == "") == (cid is None or cid == ""):
            raise ValueError(f"orders[{i}]: give exactly one of order_id and client_order_id")
        out.append({"order_id": oid} if oid else {"client_order_id": cid})
    return out


def _status(value: Any) -> Optional[str]:
    raw = getattr(value, "value", value)
    return raw if isinstance(raw, str) and raw else None


def _detail(err: m.ErrorBody, name: str) -> Optional[str]:
    v = (err.details or {}).get(name)
    raw = getattr(v, "root", v)
    return raw if isinstance(raw, str) and raw else None


def place_from_result(r: m.BatchOrderResult, after_ambiguous: bool) -> BatchOrderOutcome:
    """Read one result. A recovered result still needing a lookup has ``status`` None."""
    if r.error is None and r.order is not None:
        return BatchOrderOutcome(
            r.index, r.client_order_id, "placed", r.order.id, _status(r.order.status), r.order, list(r.fills)
        )
    err = r.error
    if err is not None and after_ambiguous and _status(err.code) == "ALREADY_EXISTS":
        return BatchOrderOutcome(
            r.index, r.client_order_id, "recovered", _detail(err, "order_id"), _detail(err, "status"), error=err
        )
    return BatchOrderOutcome(r.index, r.client_order_id, "failed", error=err, fills=list(r.fills))


def needs_lookup(o: BatchOrderOutcome) -> bool:
    return o.outcome == "recovered" and (o.order_id is None or o.status is None)


def cancel_from_result(r: m.CancelBatchResult, after_ambiguous: bool) -> CancelEntryOutcome:
    if r.error is None and r.order is not None:
        return CancelEntryOutcome(r.index, "cancelled", r.order.id, r.client_order_id, _status(r.order.status), r.order)
    err = r.error
    if err is not None and after_ambiguous and _status(err.code) == "INVALID_STATE":
        return CancelEntryOutcome(r.index, "recovered", r.order_id, r.client_order_id, error=err)
    return CancelEntryOutcome(r.index, "failed", r.order_id, r.client_order_id, error=err)


def place_result(results: List[BatchOrderOutcome]) -> BatchPlaceResult:
    placed = sum(1 for o in results if o.outcome == "placed" or (o.outcome == "recovered" and o.status != "rejected"))
    unknown = sum(1 for o in results if o.outcome == "unknown")
    return BatchPlaceResult(results, placed, len(results) - placed - unknown, unknown)


def cancel_result(results: List[CancelEntryOutcome]) -> BatchCancelResult:
    cancelled = sum(
        1 for o in results if o.outcome == "cancelled" or (o.outcome == "recovered" and o.status == "cancelled")
    )
    unknown = sum(1 for o in results if o.outcome == "unknown")
    return BatchCancelResult(results, cancelled, len(results) - cancelled - unknown, unknown)


__all__ = [
    "BatchCancelResult",
    "BatchOrderOutcome",
    "BatchPlaceResult",
    "CancelEntryOutcome",
]
