"""Path values stay one URL segment; which failures are retried (4xx, mutations)."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
import respx

import cexy
from cexy._common import build_path, operation
from tests.conftest import BASE, KEY, SECRET, SleepRecorder

SUB = operation("sub_account_balances")


@pytest.mark.parametrize("value", [".", ".."])
def test_builder_rejects_dot_segments(value: str) -> None:
    # The URL layer would resolve them (even as %2E), sending the request to another route.
    with pytest.raises(ValueError, match="must not be"):
        build_path(SUB, {"id": value})


@pytest.mark.parametrize(
    "value, segment",
    [
        ("a/b", "a%2Fb"),
        ("%2F", "%252F"),
        ("a?b", "a%3Fb"),
        ("a#b", "a%23b"),
        ("é✓", "%C3%A9%E2%9C%93"),
        ("%2e%2e", "%252e%252e"),
        ("a b", "a%20b"),
        ("...", "..."),
        (".a", ".a"),
    ],
)
def test_other_values_stay_one_segment(client: cexy.Client, value: str, segment: str) -> None:
    assert build_path(SUB, {"id": value}) == f"/api/v1/account/sub-accounts/{segment}/balances"
    with respx.mock:
        route = respx.route().respond(json={"data": []})
        client.account.sub_account_balances(value)
        url = route.calls.last.request.url
        assert url.raw_path == f"/api/v1/account/sub-accounts/{segment}/balances".encode()
        assert url.fragment == ""


@respx.mock
@pytest.mark.parametrize("value", [".", ".."])
def test_get_and_mutations_reject_dot_segments_before_any_request(client: cexy.Client, value: str) -> None:
    route = respx.route().respond(json={"data": {}})
    for call in (
        lambda: client.account.sub_account_balances(value),
        lambda: client.trading.order_by_client_id(value),
        lambda: client.trading.cancel_order(value),
        lambda: client.pools.join(value, base_amount="1", quote_amount="2"),
    ):
        with pytest.raises(ValueError):
            call()
    assert route.call_count == 0


def _err(status: int, code: str, retryable: Any = True) -> httpx.Response:
    return httpx.Response(status, json={"error": {"code": code, "message": "x", "retryable": retryable}})


@respx.mock
@pytest.mark.parametrize(
    "status, code",
    [
        (400, "VALIDATION_FAILED"),
        (404, "NOT_FOUND"),
        (408, "HTTP_408"),
        (409, "ALREADY_EXISTS"),
        (422, "INVALID_STATE"),
    ],
)
def test_4xx_is_never_retried_even_if_marked_retryable(client: cexy.Client, status: int, code: str) -> None:
    route = respx.get(BASE + "/api/v1/markets").mock(
        side_effect=[_err(status, code), httpx.Response(200, json={"data": []})]
    )
    with pytest.raises(cexy.CexyApiError):
        client.markets.list()
    assert route.call_count == 1


@respx.mock
@pytest.mark.parametrize("status, code", [(429, "RATE_LIMITED"), (409, "CONCURRENT_MODIFICATION")])
def test_429_and_concurrent_modification_still_retried(client: cexy.Client, status: int, code: str) -> None:
    route = respx.get(BASE + "/api/v1/markets").mock(
        side_effect=[_err(status, code), httpx.Response(200, json={"data": []})]
    )
    client.markets.list()
    assert route.call_count == 2


@respx.mock
def test_404_marked_retryable_is_sent_once(client: cexy.Client) -> None:
    route = respx.get(BASE + "/api/v1/account/sub-accounts/other/balances").mock(
        side_effect=[_err(404, "NOT_FOUND"), httpx.Response(200, json={"data": []})]
    )
    with pytest.raises(cexy.NotFoundError):
        client.account.sub_account_balances("other")
    assert route.call_count == 1


@respx.mock
def test_place_order_does_not_retry_other_409_even_if_retryable(client: cexy.Client) -> None:
    route = respx.post(BASE + "/api/v1/trading/orders").mock(
        side_effect=[_err(409, "ALREADY_EXISTS"), httpx.Response(200, json={"data": {}})]
    )
    with pytest.raises(cexy.ConflictError):
        client.trading.place_order("BTC/USDT", "buy", "market", quantity="0.01")
    assert route.call_count == 1


@respx.mock
def test_mutation_without_key_that_is_not_repeat_safe_is_sent_once(client: cexy.Client, sleeps: SleepRecorder) -> None:
    # Every public mutation is repeat-safe (see REPEAT_SAFE_MUTATIONS and the pool methods'
    # Idempotency-Key), so exercise the transport rule directly: a pool join WITHOUT a key.
    route = respx.post(BASE + "/api/v1/pools/BTC%2FUSDT/join").mock(
        side_effect=[_err(409, "CONCURRENT_MODIFICATION"), httpx.Response(200, json={"data": {}})]
    )
    with pytest.raises(cexy.ConflictError):
        client._transport.request("join_pool", path={"symbol": "BTC/USDT"}, body={"base_amount": "1"})
    assert route.call_count == 1
    assert "Idempotency-Key" not in route.calls.last.request.headers
    assert sleeps.calls == []


def test_async_mutation_without_key_is_sent_once_and_dots_rejected() -> None:
    async def run() -> None:
        with respx.mock:
            route = respx.post(BASE + "/api/v1/pools/BTC%2FUSDT/join").mock(
                side_effect=[_err(409, "CONCURRENT_MODIFICATION"), httpx.Response(200, json={"data": {}})]
            )
            async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET) as c:
                with pytest.raises(cexy.ConflictError):
                    await c._transport.request("join_pool", path={"symbol": "BTC/USDT"}, body={"base_amount": "1"})
                with pytest.raises(ValueError):
                    await c.account.sub_account_balances("..")
                with pytest.raises(ValueError):
                    await c.trading.cancel_order(".")
            assert route.call_count == 1

    asyncio.run(run())
