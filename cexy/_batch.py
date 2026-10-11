"""Batch place and batch cancel: local checks and the reading of a result after retries.

Shared by the async and sync clients; the HTTP calls stay in ``resources.py``. The rules are the
shared conformance cases in ``cexy-api-spec/conformance/trading/batch_orders.json``.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence

from cexy._generated import models as m
from cexy.errors import CexyApiError, CexyConnectionError, CexyError, ConfigurationError, MissingCredentialsError

MAX_BATCH_ORDERS = 20
MAX_CANCEL_ENTRIES = 50
#: The server's rule for a client order id (after it trims surrounding whitespace, which the SDK
#: refuses instead, so the id in a result is always the one sent).
CLIENT_ORDER_ID = re.compile(r"[A-Za-z0-9_\-.:]{1,64}")


def new_client_order_id() -> str:
    """A fresh ``client_order_id`` (a UUID), as ``place_order`` and ``place_orders`` generate. Create
    and save your ids with it before the call if you need to reconcile after a failure."""
    return str(uuid.uuid4())


class BatchResponseError(CexyError):
    """A 200 to a batch call that the SDK cannot use: no ``data``, an undecodable body, or results
    that do not match the request (count, ``index`` or ``client_order_id``). The server may have
    acted on any of the orders."""


PlaceOutcome = Literal["placed", "failed", "recovered", "unknown"]
CancelOutcome = Literal["cancelled", "failed", "recovered", "unknown"]


@dataclass(frozen=True)
class BatchOrderOutcome:
    """One order of ``place_orders``, in request order.

    ``outcome``:

    - ``placed``: the server placed it now; ``order`` and ``fills`` are set.
    - ``failed``: refused; ``error`` says why. ``ALREADY_EXISTS`` here means the id was used
      before this call.
    - ``recovered``: the order exists although the item carries an error: an earlier attempt of
      this call placed it (its answer was lost, so the retry was answered ``ALREADY_EXISTS``), or
      the error names the order (``MARKET_UNAVAILABLE`` when the market stopped while matching, an
      ``INTERNAL`` error after the order was stored). ``order_id`` and ``status`` say what became of
      it; ``order`` is set only when the SDK had to look it up. ``error`` keeps the server's error.
      A ``rejected`` status means it was refused (funds): its id is spent and it must not be
      placed again under that id.
    - ``unknown``: the order may exist but its state could not be read. It was not placed again;
      reconcile it with ``order_by_client_id`` before placing anything in its place.
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
    _check_not_dot_segment(cid, where)
    return cid


def _check_not_dot_segment(cid: str, where: str) -> None:
    # The server accepts them, but "." and ".." cannot be a path segment, so the order could
    # never be looked up or cancelled by its id.
    if cid in (".", ".."):
        raise ValueError(f'{where}: client_order_id must not be "." or ".."')


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
        if not oid:
            _check_not_dot_segment(str(cid), f"orders[{i}]")
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
    """Read one result. A recovered result still needing a lookup has ``order_id`` or ``status``
    None (see ``needs_lookup``)."""
    if r.error is None and r.order is not None:
        return BatchOrderOutcome(
            r.index, r.client_order_id, "placed", r.order.id, _status(r.order.status), r.order, list(r.fills)
        )
    err = r.error
    code = _status(err.code) if err is not None else None
    recovered = err is not None and (
        # ALREADY_EXISTS is this call's own order only after a lost answer; otherwise the caller
        # reused the id.
        (code == "ALREADY_EXISTS" and after_ambiguous)
        # Any other error that names an order means the order exists (MARKET_UNAVAILABLE when the
        # market stopped while matching); INTERNAL may follow a stored order: look it up.
        or (code != "ALREADY_EXISTS" and _detail(err, "order_id") is not None)
        or code == "INTERNAL"
    )
    if recovered and err is not None:
        return BatchOrderOutcome(
            r.index, r.client_order_id, "recovered", _detail(err, "order_id"), _detail(err, "status"), error=err
        )
    return BatchOrderOutcome(r.index, r.client_order_id, "failed", error=err, fills=list(r.fills))


def check_results(sent_cids: Sequence[str], data: Any) -> Any:
    """The parsed ``data`` of a place-batch 200, or ``BatchResponseError`` if it is missing or
    does not match the request."""
    if not isinstance(data, Mapping) or "data" not in data:
        raise BatchResponseError("place_orders: a 200 without data")
    try:
        parsed = m.BatchOrderResponse.model_validate(data["data"])
    except Exception as exc:  # pydantic's ValidationError, or anything a bad body raises
        raise BatchResponseError(f"place_orders: an undecodable 200 ({type(exc).__name__})") from exc
    if len(parsed.results) != len(sent_cids):
        raise BatchResponseError(f"place_orders: {len(parsed.results)} results for {len(sent_cids)} orders")
    for i, r in enumerate(parsed.results):
        if r.index != i or r.client_order_id != sent_cids[i]:
            raise BatchResponseError(f"place_orders: result {i} does not match the order sent at that position")
    return parsed


def check_cancel_results(sent: int, data: Any) -> Any:
    if not isinstance(data, Mapping) or "data" not in data:
        raise BatchResponseError("cancel_orders: a 200 without data")
    try:
        parsed = m.CancelBatchResponse.model_validate(data["data"])
    except Exception as exc:
        raise BatchResponseError(f"cancel_orders: an undecodable 200 ({type(exc).__name__})") from exc
    if len(parsed.results) != sent or any(r.index != i for i, r in enumerate(parsed.results)):
        raise BatchResponseError("cancel_orders: the results do not match the entries sent")
    return parsed


def needs_lookup(o: BatchOrderOutcome) -> bool:
    return o.outcome == "recovered" and (o.order_id is None or o.status is None)


def cancel_from_result(r: m.CancelBatchResult, after_ambiguous: bool) -> CancelEntryOutcome:
    if r.error is None and r.order is not None:
        return CancelEntryOutcome(r.index, "cancelled", r.order.id, r.client_order_id, _status(r.order.status), r.order)
    err = r.error
    if err is not None and after_ambiguous and _status(err.code) == "INVALID_STATE":
        return CancelEntryOutcome(r.index, "recovered", r.order_id, r.client_order_id, error=err)
    return CancelEntryOutcome(r.index, "failed", r.order_id, r.client_order_id, error=err)


def attach_client_order_ids(exc: BaseException, cids: Sequence[str], attempts: Sequence[str]) -> None:
    """Record on an error raised by a placement every ``client_order_id`` it sent, in request
    order, and ``maybe_placed``: False only when every attempt was a definite refusal (or nothing
    was sent); True after any ambiguous attempt and for any failure that is not an HTTP answer
    (a network error after sending, a cancelled task, an unusable 200)."""
    if isinstance(exc, (MissingCredentialsError, ConfigurationError)):
        maybe = False
    elif isinstance(exc, (CexyApiError, CexyConnectionError)):
        maybe = "ambiguous" in attempts
    else:
        maybe = True
    try:
        exc.client_order_ids = list(cids)  # type: ignore[attr-defined]
        exc.maybe_placed = maybe  # type: ignore[attr-defined]
    except Exception:  # noqa: S110 - an exception type that refuses attributes keeps its own message
        pass


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
    "BatchResponseError",
    "BatchOrderOutcome",
    "BatchPlaceResult",
    "CancelEntryOutcome",
    "attach_client_order_ids",
    "new_client_order_id",
]
