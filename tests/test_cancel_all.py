"""cancel_all: the single call, and the opt-in until_done loop (shared conformance cases)."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

import httpx
import pytest
import respx

import cexy
from tests.conftest import BASE, KEY, SECRET, load

URL = BASE + "/api/v1/trading/orders/cancel-all"
CASES = load("trading/cancel_all_until_done.json")["cases"]


def done(**kw: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {"cancelled": [], "already_closed": [], "failed": [], "failures": [], "has_more": False}
    return {**base, **kw}


class FakeClock:
    """Calls take no time; only sleeps (the loop's and the transport's) advance the clock."""

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


def sync_client(fc: FakeClock) -> cexy.Client:
    c = cexy.Client(api_key=KEY, api_secret=SECRET)
    c._transport._sleep = fc.sleep
    c._transport._clock = fc.clock
    return c


def async_client(fc: FakeClock) -> cexy.AsyncClient:
    c = cexy.AsyncClient(api_key=KEY, api_secret=SECRET)
    c._transport._sleep = fc.asleep
    c._transport._clock = fc.clock
    return c


def replies(case: Dict[str, Any]) -> List[httpx.Response]:
    bodies = list(case["responses"])
    if case.get("responses_repeat_last"):
        bodies += [bodies[-1]] * 50
    return [httpx.Response(200, json={"data": b}) for b in bodies]


def check(case: Dict[str, Any], res: cexy.CancelAllResult, route: Any, fc: FakeClock) -> None:
    exp = case["expect"]
    assert route.call_count == exp["calls"]
    assert fc.sleeps == exp["sleeps_s"]
    assert res.stopped == exp["stopped"] and res.rounds == exp["calls"]
    assert set(res.cancelled) == set(exp["cancelled"])
    assert set(res.already_closed) == set(exp["already_closed"])
    assert set(res.failed) == set(exp["failed"])
    assert {f.order_id: f.code for f in res.failures} == exp["failure_codes"]


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
@respx.mock
def test_until_done_conformance_sync(case: Dict[str, Any]) -> None:
    route = respx.post(URL).mock(side_effect=replies(case))
    fc = FakeClock()
    opts = {("time_budget" if k == "time_budget_s" else k): v for k, v in (case.get("options") or {}).items()}
    res = sync_client(fc).trading.cancel_all(symbol=None, until_done=True, **opts)
    check(case, res, route, fc)


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_until_done_conformance_async(case: Dict[str, Any]) -> None:
    async def run() -> None:
        with respx.mock:
            route = respx.post(URL).mock(side_effect=replies(case))
            fc = FakeClock()
            opts = {("time_budget" if k == "time_budget_s" else k): v for k, v in (case.get("options") or {}).items()}
            async with async_client(fc) as client:
                res = await client.trading.cancel_all(symbol=None, until_done=True, **opts)
            check(case, res, route, fc)

    asyncio.run(run())


@respx.mock
def test_default_is_one_call_with_the_typed_response() -> None:
    body = done(
        cancelled=["o1"],
        already_closed=["o2"],
        failed=["o3"],
        has_more=True,
        failures=[{"order_id": "o3", "code": "INVALID_STATE", "message": "still being placed"}],
    )
    route = respx.post(URL).respond(json={"data": body})
    res = sync_client(FakeClock()).trading.cancel_all(symbol="BTC/USDT")
    assert route.call_count == 1 and isinstance(res, cexy.models.CancelAllResponse)
    assert res.already_closed == ["o2"] and res.has_more is True and res.failures[0].code == "INVALID_STATE"
    assert json.loads(route.calls.last.request.content) == {"symbol": "BTC/USDT"}
    assert "Idempotency-Key" not in route.calls.last.request.headers


@respx.mock
def test_twenty_rounds_with_progress_never_exceed_twenty_requests() -> None:
    route = respx.post(URL).mock(
        side_effect=[httpx.Response(200, json={"data": done(cancelled=[f"o{i}"], has_more=True)}) for i in range(40)]
    )
    res = sync_client(FakeClock()).trading.cancel_all(symbol=None, until_done=True)
    assert route.call_count == 20 and res.rounds == 20 and res.stopped == "max_rounds" and len(res.cancelled) == 20


@respx.mock
def test_an_order_that_ends_closed_is_never_also_failed() -> None:
    pending = {"order_id": "p1", "code": "INVALID_STATE", "message": "still being placed"}
    route = respx.post(URL).mock(
        side_effect=[
            httpx.Response(
                200, json={"data": done(failed=["p1", "p2"], failures=[pending, {**pending, "order_id": "p2"}])}
            ),
            httpx.Response(200, json={"data": done(cancelled=["p1"], already_closed=["p2"])}),
        ]
    )
    res = sync_client(FakeClock()).trading.cancel_all(symbol=None, until_done=True)
    assert route.call_count == 2 and res.failed == [] and res.failures == []
    assert res.cancelled == ["p1"] and res.already_closed == ["p2"]


RATE = {
    "error": {
        "code": "RATE_LIMITED",
        "message": "30 a minute",
        "retryable": True,
        "details": {"retry_after_seconds": 119},
    }
}


def rate_limited_sequence() -> List[httpx.Response]:
    pending = {"order_id": "p1", "code": "INVALID_STATE", "message": "still being placed"}
    return [
        httpx.Response(200, json={"data": done(failed=["p1"], failures=[pending])}),
        httpx.Response(429, json=RATE, headers={"Retry-After": "119"}),
        httpx.Response(200, json={"data": done(cancelled=["p1"])}),
    ]


@respx.mock
def test_429_retry_after_counts_against_the_budget_sync() -> None:
    route = respx.post(URL).mock(side_effect=rate_limited_sequence())
    fc = FakeClock()
    res = sync_client(fc).trading.cancel_all(symbol=None, until_done=True, time_budget=120)
    # Round 1 is stuck (sleep 1); round 2 gets a 429 asking for 119 s: 1 + 119 reaches the budget.
    assert res.stopped == "time_budget" and route.call_count == 2 and fc.sleeps == [1.0]
    assert res.failed == ["p1"] and res.rounds == 1


def test_429_retry_after_counts_against_the_budget_async() -> None:
    async def run() -> None:
        with respx.mock:
            route = respx.post(URL).mock(side_effect=rate_limited_sequence())
            fc = FakeClock()
            async with async_client(fc) as client:
                res = await client.trading.cancel_all(symbol=None, until_done=True, time_budget=120)
            assert res.stopped == "time_budget" and route.call_count == 2 and fc.sleeps == [1.0]

    asyncio.run(run())


@respx.mock
def test_429_within_the_budget_waits_and_the_wait_advances_the_clock() -> None:
    short = {**RATE, "error": {**RATE["error"], "details": {"retry_after_seconds": 5}}}
    route = respx.post(URL).mock(
        side_effect=[
            httpx.Response(429, json=short, headers={"Retry-After": "5"}),
            httpx.Response(200, json={"data": done(cancelled=["o1"])}),
        ]
    )
    fc = FakeClock()
    res = sync_client(fc).trading.cancel_all(symbol=None, until_done=True, time_budget=120)
    assert res.stopped == "done" and route.call_count == 2 and len(fc.sleeps) == 1
    assert 5.0 <= fc.sleeps[0] <= 5.25 and fc.now == fc.sleeps[0]  # Retry-After plus transport jitter


@respx.mock
def test_unknown_symbol_is_not_found() -> None:
    respx.post(URL).respond(404, json={"error": {"code": "NOT_FOUND", "message": "no such market", "retryable": False}})
    with pytest.raises(cexy.NotFoundError):
        sync_client(FakeClock()).trading.cancel_all(symbol="NOPE/USDT", until_done=True)
