"""place_orders, cancel_orders and cancel_order_by_client_id (shared conformance cases)."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Callable, Dict, List, Optional

import httpx
import pytest
import respx

import cexy
from cexy._batch import CLIENT_ORDER_ID
from tests.conftest import BASE, KEY, SECRET, load

FIX = load("trading/batch_orders.json")
TEMPLATE: Dict[str, Any] = FIX["order_template"]
PLACE_URL = BASE + "/api/v1/trading/orders/batch"
CANCEL_URL = BASE + "/api/v1/trading/orders/cancel-batch"
BY_CID = BASE + "/api/v1/trading/orders/by-client-id/"
BY_ID = BASE + "/api/v1/trading/orders/"


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: List[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.now += s

    async def asleep(self, s: float) -> None:
        self.sleep(s)


def sync_client(fc: FakeClock, **kw: Any) -> cexy.Client:
    c = cexy.Client(api_key=KEY, api_secret=SECRET, **kw)
    c._transport._sleep = fc.sleep
    c._transport._clock = fc.clock
    return c


def async_client(fc: FakeClock, **kw: Any) -> cexy.AsyncClient:
    c = cexy.AsyncClient(api_key=KEY, api_secret=SECRET, **kw)
    c._transport._sleep = fc.asleep
    c._transport._clock = fc.clock
    return c


def order(stub: Dict[str, Any]) -> Dict[str, Any]:
    return {**TEMPLATE, **stub}


def with_orders(data: Dict[str, Any]) -> Dict[str, Any]:
    out = json.loads(json.dumps(data))
    for r in out.get("results", []):
        if r.get("order"):
            r["order"] = order(r["order"])
    return out


def reply(item: Dict[str, Any], data_fn: Callable[[Dict[str, Any]], Dict[str, Any]] = with_orders) -> Any:
    if "network_error" in item:
        raise httpx.ReadTimeout("lost response")
    if "text" in item:
        return httpx.Response(item["http_status"], text=item["text"])
    if "body" in item:
        return httpx.Response(item["http_status"], json=item["body"])
    if item["http_status"] == 200:
        return httpx.Response(200, json={"data": data_fn(item["data"])})
    return httpx.Response(item["http_status"], json={"error": item["error"]}, headers=item.get("headers") or {})


def echo_placed(request: httpx.Request) -> httpx.Response:
    """A place-batch 200 that places every order sent, in order."""
    sent = json.loads(request.content)["orders"]
    results = [
        {
            "index": i,
            "client_order_id": o["client_order_id"],
            "order": order({"id": f"o{i}"}),
            "fills": [],
            "error": None,
        }
        for i, o in enumerate(sent)
    ]
    return httpx.Response(200, json={"data": {"placed": len(sent), "failed": 0, "results": results}})


def echo_cancelled(request: httpx.Request) -> httpx.Response:
    sent = json.loads(request.content)["orders"]
    results = [
        {
            "index": i,
            "order_id": e.get("order_id"),
            "client_order_id": e.get("client_order_id"),
            "order": order({"id": e.get("order_id") or f"o{i}", "status": "cancelled"}),
            "error": None,
        }
        for i, e in enumerate(sent)
    ]
    return httpx.Response(200, json={"data": {"cancelled": len(sent), "failed": 0, "results": results}})


class Server:
    """Answers the batch call from `attempts` and the lookups from `lookups`, recording both."""

    def __init__(self, case: Dict[str, Any], url: str, method: str = "POST") -> None:
        self.case = case
        self.bodies: List[bytes] = []
        self.nonces: List[str] = []
        self.lookups: List[str] = []
        self.batch = respx.route(method=method, url=url).mock(side_effect=self._batch)
        respx.route(method="GET", url__startswith=BY_CID).mock(side_effect=self._lookup("cid", BY_CID))
        respx.route(method="GET", url__regex=re.escape(BY_ID) + r"[^/]+$").mock(
            side_effect=self._lookup("order", BY_ID)
        )

    def _batch(self, request: httpx.Request) -> Any:
        n = len(self.bodies)
        self.bodies.append(request.content)
        self.nonces.append(request.headers["X-API-Nonce"])
        item = self.case["attempts"][n]
        if item.get("echo_cids"):
            sent = [o["client_order_id"] for o in json.loads(request.content)["orders"]]

            def echo(data: Dict[str, Any]) -> Dict[str, Any]:
                out = with_orders(data)
                for r in out["results"]:
                    if r["client_order_id"] == "<generated>":
                        r["client_order_id"] = sent[r["index"]]
                return out

            return reply(item, echo)
        return reply(item)

    def _lookup(self, kind: str, prefix: str) -> Callable[[httpx.Request], Any]:
        def answer(request: httpx.Request) -> Any:
            key = f"{kind}:{str(request.url)[len(prefix) :]}"
            self.lookups.append(key)
            item = (self.case.get("lookups") or {})[key]
            if "network_error" in item:
                raise httpx.ReadTimeout("lost lookup")
            if "text" in item:
                return httpx.Response(item["http_status"], text=item["text"])
            if item["http_status"] == 200:
                return httpx.Response(200, json={"data": order(item["data"])})
            return reply(item)

        return answer


def check_common(case: Dict[str, Any], srv: Server, res: Any, transport: Any) -> None:
    exp = case["expect"]
    assert srv.batch.call_count == exp["calls"]
    if exp.get("same_body"):
        assert len(set(srv.bodies)) == 1
        assert len(set(srv.nonces)) == len(srv.nonces)  # every attempt signed afresh
    if "lookups" in exp:
        assert srv.lookups == exp["lookups"]
    if exp.get("hold") == "placements":
        assert transport._placements_held_until > 0
        assert transport.limiter._blocked_until == 0
    if exp.get("error") == "batch":
        assert isinstance(res, BaseException), res
        cause = exp["cause"]
        if cause == "connection":
            assert isinstance(res, cexy.CexyConnectionError)
        elif cause == "bad_response":
            assert isinstance(res, cexy.BatchResponseError)
        else:
            assert isinstance(res, cexy.CexyApiError) and res.code == cause
        if "error_details_limit" in exp:
            assert res.details["limit"] == exp["error_details_limit"]
        assert res.maybe_placed is exp["maybe_placed"]  # type: ignore[attr-defined]
        sent = [o["client_order_id"] for o in json.loads(srv.bodies[0])["orders"]]
        assert res.client_order_ids == sent  # type: ignore[attr-defined]
    elif "error_code" in exp and "results" not in exp:
        assert isinstance(res, cexy.CexyApiError) and res.code == exp["error_code"]
        if "error_details_limit" in exp:
            assert res.details["limit"] == exp["error_details_limit"]


def check_results(exp: Dict[str, Any], res: Any, kind: str) -> None:
    got = []
    for o in res.results:
        row: Dict[str, Any] = {"index": o.index, "outcome": o.outcome}
        if o.outcome in ("placed", "recovered", "cancelled"):
            row["order_id"], row["status"] = o.order_id, o.status
        if o.outcome == "failed":
            row["error_code"] = getattr(o.error.code, "value", o.error.code)
        got.append(row)
    assert got == exp["results"]
    first = "placed" if kind == "place" else "cancelled"
    assert (getattr(res, first), res.failed, res.unknown) == (exp[first], exp["failed"], exp["unknown"])


PLACE = FIX["place"]
CANCEL = FIX["cancel"]
BY_CLIENT_ID = FIX["cancel_by_client_id"]


@pytest.mark.parametrize("case", PLACE, ids=[c["id"] for c in PLACE])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_place_conformance(case: Dict[str, Any], mode: str) -> None:
    async def go() -> None:
        with respx.mock:
            srv = Server(case, PLACE_URL)
            fc = FakeClock()
            req = case["request"]
            res: Any
            if mode == "sync":
                c = sync_client(fc)
                try:
                    res = c.trading.place_orders(req["symbol"], req["orders"])
                except cexy.CexyError as exc:
                    res = exc
            else:
                c = async_client(fc)
                try:
                    res = await c.trading.place_orders(req["symbol"], req["orders"])
                except cexy.CexyError as exc:
                    res = exc
            check_common(case, srv, res, c._transport)
            exp = case["expect"]
            if "results" in exp:
                check_results(exp, res, "place")
            for i in exp.get("generated_cid_indexes", []):
                sent = json.loads(srv.bodies[0])["orders"][i]["client_order_id"]
                assert CLIENT_ORDER_ID.fullmatch(sent)
                got = res.client_order_ids[i] if isinstance(res, BaseException) else res.results[i].client_order_id
                assert got == sent

    asyncio.run(go())


@pytest.mark.parametrize("case", CANCEL, ids=[c["id"] for c in CANCEL])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_cancel_conformance(case: Dict[str, Any], mode: str) -> None:
    async def go() -> None:
        with respx.mock:
            srv = Server(case, CANCEL_URL)
            fc = FakeClock()
            res: Any
            try:
                if mode == "sync":
                    c = sync_client(fc)
                    res = c.trading.cancel_orders(case["request"]["orders"])
                else:
                    c = async_client(fc)
                    res = await c.trading.cancel_orders(case["request"]["orders"])
            except cexy.CexyError as exc:
                res = exc
            if case["expect"].get("error") == "bad_response":
                assert isinstance(res, cexy.BatchResponseError) and srv.batch.call_count == case["expect"]["calls"]
                return
            check_common(case, srv, res, c._transport)
            check_results(case["expect"], res, "cancel")

    asyncio.run(go())


@pytest.mark.parametrize("case", BY_CLIENT_ID, ids=[c["id"] for c in BY_CLIENT_ID])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_cancel_by_client_id_conformance(case: Dict[str, Any], mode: str) -> None:
    async def go() -> None:
        with respx.mock:
            srv = Server(case, BY_CID + case["client_order_id"], "DELETE")
            fc = FakeClock()
            res: Any
            c: Any = sync_client(fc) if mode == "sync" else async_client(fc)
            try:
                out = c.trading.cancel_order_by_client_id(case["client_order_id"])
                res = out if mode == "sync" else await out
            except cexy.CexyApiError as exc:
                res = exc
            exp = case["expect"]
            assert srv.batch.call_count == exp["calls"] and srv.lookups == exp["lookups"]
            if "error_code" in exp:
                assert isinstance(res, cexy.CexyApiError) and res.code == exp["error_code"]
            else:
                assert (res.id, res.status.value) == (exp["order_id"], exp["status"])

    asyncio.run(go())


def expand(req: Dict[str, Any], call: str) -> Any:
    if "orders_count" in req:
        n = req["orders_count"]
        if call == "place":
            return [
                {"side": "buy", "type": "limit", "price": "1", "quantity": "1", "client_order_id": f"c{i}"}
                for i in range(n)
            ]
        return [{"order_id": f"o{i}"} for i in range(n)]
    return req["orders"]


@pytest.mark.parametrize("case", FIX["validation"], ids=[c["id"] for c in FIX["validation"]])
def test_validation_refused_locally(case: Dict[str, Any]) -> None:
    with respx.mock(assert_all_called=False) as router:
        route = router.route(url__startswith=BASE).respond(500)
        c = sync_client(FakeClock())
        orders = expand(case["request"], case["call"])
        with pytest.raises(ValueError):
            if case["call"] == "place":
                c.trading.place_orders(case["request"]["symbol"], orders)
            else:
                c.trading.cancel_orders(orders)
        assert route.call_count == 0


@pytest.mark.parametrize("case", FIX["valid"], ids=[c["id"] for c in FIX["valid"]])
@respx.mock
def test_valid_requests_are_sent(case: Dict[str, Any]) -> None:
    c = sync_client(FakeClock())
    if case["call"] == "place":
        orders = (
            [{"side": "buy", "type": "market", "quantity": "1", "client_order_id": case["client_order_id"]}]
            if "client_order_id" in case
            else expand(case, "place")
        )
        route = respx.post(PLACE_URL).mock(side_effect=echo_placed)
        c.trading.place_orders("BTC/USDT", orders)
    else:
        route = respx.post(CANCEL_URL).mock(side_effect=echo_cancelled)
        c.trading.cancel_orders(expand(case, "cancel"))
    assert route.call_count == 1


@pytest.mark.parametrize("case", FIX["cid_path"], ids=[c["client_order_id"] for c in FIX["cid_path"]])
@respx.mock
def test_cancel_by_client_id_path_is_encoded(case: Dict[str, Any]) -> None:
    route = respx.delete(url__startswith=BY_CID).respond(json={"data": order({"id": "o1", "status": "cancelled"})})
    sync_client(FakeClock()).trading.cancel_order_by_client_id(case["client_order_id"])
    assert route.calls.last.request.url.raw_path.decode() == case["path"]


COST = FIX["cost"]["calls"]


def tokens_taken(c: cexy.Client, fn: Callable[[], Any]) -> float:
    lim = c._transport.limiter
    before = lim.tokens
    fn()
    return before - lim.tokens


@respx.mock
def test_limiter_cost_per_call() -> None:
    n = 7
    c = sync_client(FakeClock())
    respx.post(PLACE_URL).mock(side_effect=echo_placed)
    respx.post(CANCEL_URL).mock(side_effect=echo_cancelled)
    respx.delete(url__startswith=BY_ID).respond(json={"data": order({"id": "o1", "status": "cancelled"})})
    place = [{"side": "buy", "type": "market", "quantity": "1"} for _ in range(n)]
    got = {
        "place_orders": tokens_taken(c, lambda: c.trading.place_orders("BTC/USDT", place)),
        "cancel_orders": tokens_taken(c, lambda: c.trading.cancel_orders([{"order_id": f"o{i}"} for i in range(n)])),
        "cancel_order": tokens_taken(c, lambda: c.trading.cancel_order("o1")),
        "cancel_order_by_client_id": tokens_taken(c, lambda: c.trading.cancel_order_by_client_id("c1")),
    }
    for call, taken in got.items():
        assert taken == pytest.approx(float(COST[call]["request_tokens"].replace("N", str(n))))


def actions_taken(c: cexy.Client, fn: Callable[[], Any]) -> float:
    lim = c._transport.order_limiter
    before = lim.tokens
    fn()
    return before - lim.tokens


@respx.mock
def test_order_actions_budget_counts_only_placements() -> None:
    n = 5
    c = sync_client(FakeClock(), order_actions_per_second=50)
    respx.post(PLACE_URL).mock(side_effect=echo_placed)
    respx.post(BASE + "/api/v1/trading/orders").respond(json={"data": {"order": order({"id": "o1"}), "fills": []}})
    respx.post(CANCEL_URL).mock(side_effect=echo_cancelled)
    respx.delete(url__startswith=BY_ID).respond(json={"data": order({"id": "o1", "status": "cancelled"})})
    place = [{"side": "buy", "type": "market", "quantity": "1"} for _ in range(n)]
    got = {
        "place_orders": actions_taken(c, lambda: c.trading.place_orders("BTC/USDT", place)),
        "place_order": actions_taken(c, lambda: c.trading.place_order("BTC/USDT", "buy", "market", quantity="1")),
        "cancel_orders": actions_taken(c, lambda: c.trading.cancel_orders([{"order_id": f"o{i}"} for i in range(n)])),
        "cancel_order": actions_taken(c, lambda: c.trading.cancel_order("o1")),
        "cancel_order_by_client_id": actions_taken(c, lambda: c.trading.cancel_order_by_client_id("c1")),
    }
    for call, taken in got.items():
        assert taken == pytest.approx(float(COST[call]["order_actions"].replace("N", str(n))))


@respx.mock
def test_order_actions_hold_delays_placements_not_reads_or_cancels() -> None:
    fc = FakeClock()
    c = sync_client(fc)
    c._transport._hold_placements(5)
    respx.get(BY_ID + "o1").respond(json={"data": order({"id": "o1"})})
    respx.delete(BY_ID + "o1").respond(json={"data": order({"id": "o1", "status": "cancelled"})})
    respx.post(PLACE_URL).mock(side_effect=echo_placed)
    c.trading.order("o1")
    c.trading.cancel_order("o1")
    assert fc.sleeps == []
    c.trading.place_orders("BTC/USDT", [{"side": "buy", "type": "market", "quantity": "1"}])
    assert len(fc.sleeps) == 1 and 5 <= fc.sleeps[0] <= 5.25


@respx.mock
def test_request_limit_429_still_holds_everything() -> None:
    fc = FakeClock()
    c = sync_client(fc)
    err = {
        "error": {"code": "RATE_LIMITED", "message": "slow", "retryable": True, "details": {"retry_after_seconds": 3}}
    }
    respx.get(BY_ID + "o1").mock(
        side_effect=[httpx.Response(429, json=err), httpx.Response(200, json={"data": order({"id": "o1"})})]
    )
    c.trading.order("o1")
    assert c._transport.limiter._blocked_until > 0 and c._transport._placements_held_until == 0


@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan"), True, "50"])
def test_order_actions_per_second_must_be_positive(bad: Optional[Any]) -> None:
    with pytest.raises(cexy.ConfigurationError):
        cexy.Client(api_key=KEY, api_secret=SECRET, order_actions_per_second=bad)


@respx.mock
def test_place_orders_wire_body() -> None:
    route = respx.post(PLACE_URL).mock(side_effect=echo_placed)
    from decimal import Decimal

    sync_client(FakeClock()).trading.place_orders(
        "BTC/USDT",
        [
            {
                "side": "sell",
                "type": "limit",
                "price": Decimal("100.50"),
                "quantity": "2",
                "time_in_force": "gtc",
                "client_order_id": "x1",
            }
        ],
    )
    assert json.loads(route.calls.last.request.content) == {
        "symbol": "BTC/USDT",
        "orders": [
            {
                "side": "sell",
                "type": "limit",
                "price": "100.50",
                "quantity": "2",
                "time_in_force": "gtc",
                "client_order_id": "x1",
            }
        ],
    }
    assert "Idempotency-Key" not in route.calls.last.request.headers


def test_place_orders_refuses_float_amounts() -> None:
    with pytest.raises((TypeError, ValueError)):
        sync_client(FakeClock()).trading.place_orders("BTC/USDT", [{"side": "buy", "type": "market", "quantity": 0.1}])


@respx.mock
def test_single_place_order_error_carries_its_id() -> None:
    respx.post(BASE + "/api/v1/trading/orders").mock(side_effect=httpx.ReadTimeout("lost"))
    respx.get(url__startswith=BY_CID).respond(
        404, json={"error": {"code": "NOT_FOUND", "message": "x", "retryable": False}}
    )
    with pytest.raises(cexy.CexyConnectionError) as info:
        sync_client(FakeClock()).trading.place_order("BTC/USDT", "buy", "market", quantity="1", client_order_id="s1")
    assert info.value.client_order_ids == ["s1"]  # type: ignore[attr-defined]
    assert info.value.maybe_placed is True  # type: ignore[attr-defined]


def test_local_refusal_carries_nothing() -> None:
    with pytest.raises(ValueError) as info:
        sync_client(FakeClock()).trading.place_orders(
            "BTC/USDT", [{"side": "buy", "type": "market", "quantity": "1", "client_order_id": ".."}]
        )
    assert not hasattr(info.value, "client_order_ids")


def test_new_client_order_id_is_exported_and_valid() -> None:
    a, b = cexy.new_client_order_id(), cexy.new_client_order_id()
    assert a != b and CLIENT_ORDER_ID.fullmatch(a)


@respx.mock
def test_lookup_with_an_unusable_body_is_unknown() -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "placed": 0,
                    "failed": 1,
                    "results": [
                        {
                            "index": 0,
                            "client_order_id": "c1",
                            "order": None,
                            "fills": [],
                            "error": {"code": "INTERNAL", "message": "x", "retryable": False},
                        }
                    ],
                }
            },
        )

    respx.post(PLACE_URL).mock(side_effect=answer)
    respx.get(BY_CID + "c1").respond(200, json={"data": {"id": 1}})
    res = sync_client(FakeClock()).trading.place_orders(
        "BTC/USDT", [{"side": "buy", "type": "market", "quantity": "1", "client_order_id": "c1"}]
    )
    assert [o.outcome for o in res.results] == ["unknown"] and res.unknown == 1


def _internal_item(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "data": {
                "placed": 0,
                "failed": 1,
                "results": [
                    {
                        "index": 0,
                        "client_order_id": "c1",
                        "order": None,
                        "fills": [],
                        "error": {"code": "INTERNAL", "message": "x", "retryable": False},
                    }
                ],
            }
        },
    )


@respx.mock
def test_interrupt_during_lookups_carries_the_ids() -> None:
    respx.post(PLACE_URL).mock(side_effect=_internal_item)
    c = sync_client(FakeClock())

    def interrupted(outcome: Any) -> Any:
        raise KeyboardInterrupt

    c.trading._recover_placed = interrupted  # type: ignore[method-assign]
    with pytest.raises(KeyboardInterrupt) as info:
        c.trading.place_orders(
            "BTC/USDT", [{"side": "buy", "type": "market", "quantity": "1", "client_order_id": "c1"}]
        )
    assert info.value.client_order_ids == ["c1"]  # type: ignore[attr-defined]
    assert info.value.maybe_placed is True  # type: ignore[attr-defined]


@respx.mock
def test_a_200_that_is_not_utf8_is_a_batch_response_error() -> None:
    respx.post(PLACE_URL).respond(200, content=b'{"data": "\xff\xfe"}', headers={"Content-Type": "application/json"})
    with pytest.raises(cexy.BatchResponseError) as info:
        sync_client(FakeClock()).trading.place_orders(
            "BTC/USDT", [{"side": "buy", "type": "market", "quantity": "1", "client_order_id": "c1"}]
        )
    assert info.value.maybe_placed is True  # type: ignore[attr-defined]


def test_configuration_error_after_an_ambiguous_attempt_is_maybe_placed() -> None:
    from cexy._batch import attach_client_order_ids

    early, late = cexy.ConfigurationError("x"), cexy.ConfigurationError("y")
    attach_client_order_ids(early, ["c1"], [])
    attach_client_order_ids(late, ["c1"], ["ambiguous"])
    assert early.maybe_placed is False and late.maybe_placed is True  # type: ignore[attr-defined]
