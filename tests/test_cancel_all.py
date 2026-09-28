"""cancel_all: the single call, and the opt-in until_done loop (shared conformance cases)."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

import httpx
import pytest
import respx

import cexy
import cexy.models
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


def to_response(item: Dict[str, Any]) -> httpx.Response:
    """A conformance response item: a `data` object (200), or `http_status` + `error`/`body`."""
    if "http_status" not in item:
        return httpx.Response(200, json={"data": item})
    body = item["body"] if "body" in item else {"error": item["error"]}
    return httpx.Response(item["http_status"], json=body, headers=item.get("headers") or {})


def replies(case: Dict[str, Any]) -> List[httpx.Response]:
    items = list(case["responses"])
    if case.get("responses_repeat_last"):
        items += [items[-1]] * 50
    return [to_response(i) for i in items]


def options(case: Dict[str, Any]) -> Dict[str, Any]:
    return {("time_budget" if k == "time_budget_s" else k): v for k, v in (case.get("options") or {}).items()}


def check(case: Dict[str, Any], res: Any, route: Any, fc: FakeClock) -> None:
    exp = case["expect"]
    assert route.call_count == exp["calls"]
    assert fc.sleeps == exp["sleeps_s"]
    if "error_code" in exp:  # the loop raised: `res` is the exception
        assert isinstance(res, cexy.CexyApiError) and res.code == exp["error_code"]
        assert set(res.partial.cancelled) == set(exp["partial_cancelled"])  # type: ignore[attr-defined]
        assert res.partial.rounds == exp["calls"]  # type: ignore[attr-defined]
        return
    assert isinstance(res, cexy.CancelAllResult)
    assert res.stopped == exp["stopped"] and res.rounds == exp["calls"]
    assert res.last_error_code == exp.get("last_error_code")
    assert set(res.cancelled) == set(exp["cancelled"])
    assert set(res.already_closed) == set(exp["already_closed"])
    assert set(res.failed) == set(exp["failed"])
    assert {f.order_id: f.code for f in res.failures} == exp["failure_codes"]


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
@respx.mock
def test_until_done_conformance_sync(case: Dict[str, Any]) -> None:
    route = respx.post(URL).mock(side_effect=replies(case))
    fc = FakeClock()
    res: Any
    try:
        res = sync_client(fc).trading.cancel_all(symbol=None, until_done=True, **options(case))
    except cexy.CexyApiError as exc:
        res = exc
    check(case, res, route, fc)


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_until_done_conformance_async(case: Dict[str, Any]) -> None:
    async def run() -> None:
        with respx.mock:
            route = respx.post(URL).mock(side_effect=replies(case))
            fc = FakeClock()
            res: Any
            async with async_client(fc) as client:
                try:
                    res = await client.trading.cancel_all(symbol=None, until_done=True, **options(case))
                except cexy.CexyApiError as exc:
                    res = exc
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
    # Round 1 is stuck (sleep 1); round 2 is a 429 asking for 119 s: 1 + 119 reaches the budget,
    # so that wait is not taken. The 429 counts as a round.
    assert res.stopped == "time_budget" and route.call_count == 2 and fc.sleeps == [1.0]
    assert res.failed == ["p1"] and res.rounds == 2 and res.last_error_code == "RATE_LIMITED"


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
    assert res.stopped == "done" and route.call_count == 2
    assert fc.sleeps == [5.0] and fc.now == 5.0  # the loop waits the server's Retry-After exactly


@respx.mock
def test_unknown_symbol_is_not_found() -> None:
    respx.post(URL).respond(404, json={"error": {"code": "NOT_FOUND", "message": "no such market", "retryable": False}})
    with pytest.raises(cexy.NotFoundError):
        sync_client(FakeClock()).trading.cancel_all(symbol="NOPE/USDT", until_done=True)


@respx.mock
def test_twenty_rounds_of_errors_never_exceed_twenty_requests() -> None:
    busy = {"error": {"code": "SERVICE_UNAVAILABLE", "message": "busy", "retryable": True}}
    route = respx.post(URL).mock(side_effect=[httpx.Response(503, json=busy)] * 60)
    res = sync_client(FakeClock()).trading.cancel_all(symbol=None, until_done=True, time_budget=10_000)
    # Transport retries are off inside the loop: one HTTP request per round.
    assert route.call_count == 20 and res.rounds == 20 and res.stopped == "max_rounds"
    assert res.last_error_code == "SERVICE_UNAVAILABLE"


@respx.mock
def test_network_failure_is_a_round_without_progress() -> None:
    route = respx.post(URL).mock(
        side_effect=[httpx.ReadTimeout("slow"), httpx.Response(200, json={"data": done(cancelled=["o1"])})]
    )
    fc = FakeClock()
    res = sync_client(fc).trading.cancel_all(symbol=None, until_done=True)
    assert route.call_count == 2 and fc.sleeps == [1.0] and res.cancelled == ["o1"] and res.rounds == 2


def _limited_round() -> httpx.Response:
    # A successful round that also says the account's request window is used up for 170 s.
    return httpx.Response(
        200,
        json={"data": done(cancelled=["o1"], has_more=True)},
        headers={"X-RateLimit-Limit": "600", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "170"},
    )


@respx.mock
def test_a_rate_limiter_block_counts_against_the_budget_sync() -> None:
    route = respx.post(URL).mock(side_effect=[_limited_round()])
    fc = FakeClock()
    with sync_client(fc) as client:
        res = client.trading.cancel_all(symbol=None, until_done=True, time_budget=120.0)
    assert route.call_count == 1
    assert fc.now < 120.0 and fc.sleeps == []
    assert res.stopped == "time_budget" and res.last_error_code == "RATE_LIMITED"
    assert res.cancelled == ["o1"] and res.rounds == 1


def test_a_rate_limiter_block_counts_against_the_budget_async() -> None:
    async def run() -> None:
        with respx.mock:
            route = respx.post(URL).mock(side_effect=[_limited_round()])
            fc = FakeClock()
            async with async_client(fc) as client:
                res = await client.trading.cancel_all(symbol=None, until_done=True, time_budget=120.0)
            assert route.call_count == 1
            assert fc.now < 120.0 and fc.sleeps == []
            assert res.stopped == "time_budget" and res.last_error_code == "RATE_LIMITED"
            assert res.cancelled == ["o1"] and res.rounds == 1

    asyncio.run(run())


@respx.mock
def test_a_short_rate_limiter_block_is_waited_within_the_budget() -> None:
    first = httpx.Response(
        200,
        json={"data": done(cancelled=["o1"], has_more=True)},
        headers={"X-RateLimit-Limit": "600", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "5"},
    )
    route = respx.post(URL).mock(side_effect=[first, httpx.Response(200, json={"data": done(cancelled=["o2"])})])
    fc = FakeClock()
    with sync_client(fc) as client:
        res = client.trading.cancel_all(symbol=None, until_done=True, time_budget=120.0)
    assert route.call_count == 2 and res.stopped == "done"
    assert 5.0 <= fc.now < 120.0
