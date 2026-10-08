"""Server-controlled waits are untrusted (shared conformance: transport/server_waits.json).

Retry-After, details.retry_after_seconds and X-RateLimit-Reset never make the SDK panic,
overflow or hang: unusable values are ignored, a wait above 120 s is not taken (the call fails
at once with the server's error), and the limiter never blocks longer than 120 s.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import httpx
import pytest
import respx

import cexy
from tests.conftest import BASE, load
from tests.test_cancel_all import FakeClock, to_response

URL = BASE + "/api/v1/time"
SPEC = load("transport/server_waits.json")
CASES = SPEC["cases"]


def replies(case: Dict[str, Any]) -> List[httpx.Response]:
    items = list(case["responses"])
    return [to_response(i) for i in items + [items[-1]] * 10]


def check(case: Dict[str, Any], outcome: Any, route: Any, fc: FakeClock) -> None:
    exp = case["expect"]
    assert route.call_count == exp["calls"]
    if "sleeps_s" in exp:
        assert len(fc.sleeps) == len(exp["sleeps_s"])
        for got, want in zip(fc.sleeps, exp["sleeps_s"]):
            assert want <= got <= want + 1
    if "no_sleep_longer_than_s" in exp:
        assert all(s <= exp["no_sleep_longer_than_s"] for s in fc.sleeps)
    assert all(s <= SPEC["max_server_wait_s"] for s in fc.sleeps)
    if exp.get("ok"):
        assert not isinstance(outcome, Exception)
    if "error_code" in exp:
        assert isinstance(outcome, cexy.RateLimitError) and outcome.code == exp["error_code"]
        if "retry_after_s" in exp:
            assert outcome.retry_after == exp["retry_after_s"]


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
@respx.mock
def test_server_waits_conformance_sync(case: Dict[str, Any]) -> None:
    route = respx.get(URL).mock(side_effect=replies(case))
    fc = FakeClock()
    client = cexy.Client()
    client._transport._sleep = fc.sleep
    client._transport._clock = fc.clock
    outcome: Any = None
    for _ in range(1 + case.get("then_calls", 0)):
        try:
            outcome = client.time()
        except cexy.CexyApiError as exc:
            outcome = exc
    check(case, outcome, route, fc)


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_server_waits_conformance_async(case: Dict[str, Any]) -> None:
    async def run() -> None:
        with respx.mock:
            route = respx.get(URL).mock(side_effect=replies(case))
            fc = FakeClock()
            outcome: Any = None
            async with cexy.AsyncClient() as client:
                client._transport._sleep = fc.asleep
                client._transport._clock = fc.clock
                for _ in range(1 + case.get("then_calls", 0)):
                    try:
                        outcome = await client.time()
                    except cexy.CexyApiError as exc:
                        outcome = exc
            check(case, outcome, route, fc)

    asyncio.run(run())


@pytest.mark.parametrize(
    "headers, details",
    [
        ({"Retry-After": "nan"}, {}),
        ({"Retry-After": "-5"}, {"retry_after_seconds": -1}),
        ({"Retry-After": "inf"}, {"retry_after_seconds": float("inf")}),
        ({"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}, {}),  # a date in the past: 0
        ({"Retry-After": "Fri, 31 Dec 9999 23:59:59 GMT"}, {"retry_after_seconds": 10**400}),
        ({"Retry-After": "\x00garbage"}, {"retry_after_seconds": True}),
    ],
)
def test_retry_after_parsing_never_raises(headers: Dict[str, str], details: Dict[str, Any]) -> None:
    from cexy.errors import retry_after_seconds

    value = retry_after_seconds(headers, details)
    assert value is None or value >= 0


def test_limiter_ignores_unusable_headers_and_caps_waits() -> None:
    from cexy._ratelimit import MAX_WAIT_S, TokenBucket

    now = [0.0]
    bucket = TokenBucket(per_minute=60, clock=lambda: now[0])
    for headers in (
        {"X-RateLimit-Remaining": "nan", "X-RateLimit-Reset": "5"},
        {"X-RateLimit-Remaining": "-1e308", "X-RateLimit-Reset": "inf"},
        {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1e308"},
    ):
        bucket.update_from_headers(headers)
        assert 0 <= bucket.acquire() <= MAX_WAIT_S


# --- shared hold: a 429 with a hint blocks the whole client's limiter ---------------------------


def _hold_client(fc: FakeClock) -> cexy.Client:
    c = cexy.Client(max_retries=0)
    c._transport._sleep = fc.sleep
    c._transport._clock = fc.clock
    return c


@respx.mock
def test_429_hint_holds_the_next_call_sync() -> None:
    respx.get(URL).mock(
        side_effect=[
            httpx.Response(429, json={"error": {"code": "RATE_LIMITED", "message": "x"}}, headers={"Retry-After": "3"}),
            httpx.Response(200, json={"data": {"epoch_ms": 1, "iso": "2026-01-01T00:00:00Z"}}),
        ]
    )
    fc = FakeClock()
    client = _hold_client(fc)
    with pytest.raises(cexy.RateLimitError):
        client.time()
    assert fc.sleeps == []  # max_retries=0: the failing call itself does not wait
    client.time()
    assert len(fc.sleeps) == 1 and 3 <= fc.sleeps[0] <= 3.25


def test_429_hint_holds_the_next_call_async() -> None:
    async def run() -> None:
        with respx.mock:
            respx.get(URL).mock(
                side_effect=[
                    httpx.Response(
                        429,
                        json={
                            "error": {"code": "RATE_LIMITED", "message": "x", "details": {"retry_after_seconds": "3"}}
                        },
                    ),
                    httpx.Response(200, json={"data": {"epoch_ms": 1, "iso": "2026-01-01T00:00:00Z"}}),
                ]
            )
            fc = FakeClock()
            async with cexy.AsyncClient(max_retries=0) as client:
                client._transport._sleep = fc.asleep
                client._transport._clock = fc.clock
                with pytest.raises(cexy.RateLimitError):
                    await client.time()
                await client.time()
            assert len(fc.sleeps) == 1 and 3 <= fc.sleeps[0] <= 3.25

    asyncio.run(run())


@respx.mock
def test_hint_above_120s_fails_at_once_but_holds_the_limiter_for_120s() -> None:
    respx.get(URL).mock(
        side_effect=[
            httpx.Response(
                429, json={"error": {"code": "RATE_LIMITED", "message": "x"}}, headers={"Retry-After": "500"}
            ),
            httpx.Response(200, json={"data": {"epoch_ms": 1, "iso": "2026-01-01T00:00:00Z"}}),
        ]
    )
    fc = FakeClock()
    client = cexy.Client()  # default retries: a hint above 120 s still raises without waiting
    client._transport._sleep = fc.sleep
    client._transport._clock = fc.clock
    with pytest.raises(cexy.RateLimitError) as ei:
        client.time()
    assert ei.value.retry_after == 500 and fc.sleeps == []
    client.time()
    assert fc.sleeps == [120.0]


def test_limiter_keeps_the_later_hold() -> None:
    from cexy._ratelimit import MAX_WAIT_S, TokenBucket

    now = [0.0]
    bucket = TokenBucket(per_minute=6000, burst=1000, clock=lambda: now[0])
    bucket.block_for(5)
    bucket.block_for(2)  # a later, shorter hint never shortens the hold (H-3)
    assert 5 <= bucket.acquire() <= 5.25
    now[0] = 1.0
    bucket.block_for(10)  # a longer one extends it
    assert 10 <= bucket.acquire() <= 10.25
    bucket.block_for(1e9)  # capped
    assert bucket.acquire() == MAX_WAIT_S
    for bad in (0, -1, float("nan"), float("inf"), "x"):
        bucket.block_for(bad)  # type: ignore[arg-type]
    now[0] = 1000.0
    assert bucket.acquire() == 0


def test_retry_after_numeric_string_body() -> None:
    from cexy.errors import retry_after_seconds

    assert retry_after_seconds({}, {"retry_after_seconds": "7"}) == 7
    assert retry_after_seconds({"Retry-After": "2"}, {"retry_after_seconds": " 5.5 "}) == 5.5
    assert retry_after_seconds({"Retry-After": "9"}, {"retry_after_seconds": "5"}) == 9
    for bad in ("nan", "inf", "-3", "abc", ""):
        assert retry_after_seconds({}, {"retry_after_seconds": bad}) is None
