from __future__ import annotations

import httpx
import pytest
import respx

import cexy
from cexy._ratelimit import TokenBucket
from tests.conftest import BASE, SleepRecorder


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def test_default_bucket_depends_on_credentials() -> None:
    assert cexy.Client()._transport.limiter.rate * 60 == pytest.approx(100)
    keyed = cexy.Client(api_key="ak_test_key", api_secret="test_secret")
    assert keyed._transport.limiter.rate * 60 == pytest.approx(300)
    assert cexy.AsyncClient()._transport.limiter.rate * 60 == pytest.approx(100)


def test_burst_then_wait() -> None:
    clock = Clock()
    b = TokenBucket(per_minute=60, burst=2, clock=clock)
    assert b.acquire() == 0 and b.acquire() == 0
    assert b.acquire() == pytest.approx(1.0)  # 1 token/s
    clock.t += 5
    assert b.acquire() == 0


def test_adapts_to_remaining_header() -> None:
    clock = Clock()
    b = TokenBucket(per_minute=600, burst=100, clock=clock)
    b.update_from_headers({"X-RateLimit-Remaining": "1", "X-RateLimit-Reset": "30"})
    assert b.acquire() == 0
    assert b.acquire() > 0  # server said only one request remained


def test_zero_remaining_blocks_until_reset() -> None:
    clock = Clock()
    b = TokenBucket(per_minute=600, burst=100, clock=clock)
    b.update_from_headers({"x-ratelimit-remaining": "0", "x-ratelimit-reset": "7"})
    assert b.acquire() >= 7
    clock.t += 8
    assert b.acquire() == 0 or b.acquire() < 1


def test_missing_or_bad_headers_ignored() -> None:
    b = TokenBucket()
    before = b.tokens
    b.update_from_headers({})
    b.update_from_headers({"X-RateLimit-Remaining": "lots"})
    assert b.tokens == before


@respx.mock
def test_client_honours_remaining_zero(public_client: cexy.Client, sleeps: SleepRecorder) -> None:
    respx.get(BASE + "/api/v1/markets").mock(
        side_effect=[
            httpx.Response(200, json={"data": []}, headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "3"}),
            httpx.Response(200, json={"data": []}),
        ]
    )
    public_client.markets.list()
    assert sleeps.calls == []
    public_client.markets.list()
    assert sleeps.calls and sleeps.calls[0] >= 2.9


def test_rate_limit_configurable() -> None:
    assert cexy.Client(rate_limit_per_minute=60)._transport.limiter.rate == pytest.approx(1.0)
