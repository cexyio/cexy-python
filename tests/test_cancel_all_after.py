"""cancel_all_after (dead-man switch): body rules, local validation, retries, DEAD_MAN_NOT_ARMED."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
import respx

import cexy
import cexy.models
from tests.conftest import BASE, KEY, ORDER, SECRET, load

URL = BASE + "/api/v1/trading/orders/cancel-all-after"
PLACE = BASE + "/api/v1/trading/orders"
BY_CID = BASE + "/api/v1/trading/orders/by-client-id/"
ARMED = {
    "data": {
        "armed": True,
        "deadline": "2026-10-07T12:00:10Z",
        "server_time": "2026-10-07T12:00:00Z",
        "symbol": "BTC/USDT",
        "timeout_ms": 10000,
    }
}
DISARMED = {
    "data": {"armed": False, "deadline": None, "server_time": "2026-10-07T12:00:00Z", "symbol": None, "timeout_ms": 0}
}


def _body(route: Any) -> Any:
    return json.loads(route.calls.last.request.content)


def _aclient() -> cexy.AsyncClient:
    c = cexy.AsyncClient(api_key=KEY, api_secret=SECRET)

    async def no_sleep(_: float) -> None:
        return None

    c._transport._sleep = no_sleep
    return c


@respx.mock
def test_symbol_body_and_response(client: cexy.Client) -> None:
    route = respx.post(URL).respond(json=ARMED)
    res = client.trading.cancel_all_after(symbol="BTC/USDT", timeout_ms=10000)
    assert _body(route) == {"symbol": "BTC/USDT", "timeout_ms": 10000}
    assert "Idempotency-Key" not in route.calls.last.request.headers
    assert isinstance(res, cexy.models.CancelAllAfterResponse)
    assert res.armed is True and res.timeout_ms == 10000 and res.symbol == "BTC/USDT"
    assert res.deadline is not None


@respx.mock
def test_none_sends_explicit_null(client: cexy.Client) -> None:
    route = respx.post(URL).respond(json=ARMED)
    client.trading.cancel_all_after(symbol=None, timeout_ms=10000)
    raw = route.calls.last.request.content
    assert json.loads(raw) == {"symbol": None, "timeout_ms": 10000}
    assert b'"symbol":null' in raw


@respx.mock
@pytest.mark.parametrize("symbol", ["", " ", "  ", "\t\n"])
def test_blank_symbol_raises(client: cexy.Client, symbol: str) -> None:
    route = respx.post(URL).respond(json=ARMED)
    with pytest.raises(ValueError):
        client.trading.cancel_all_after(symbol=symbol, timeout_ms=10000)
    assert route.call_count == 0


@respx.mock
def test_symbol_keyword_is_required(client: cexy.Client) -> None:
    route = respx.post(URL).respond(json=ARMED)
    with pytest.raises(TypeError):
        client.trading.cancel_all_after(timeout_ms=10000)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        client.trading.cancel_all_after(None, 10000)  # type: ignore[misc]
    assert route.call_count == 0


@respx.mock
def test_zero_disarms_and_is_sent_as_zero(client: cexy.Client) -> None:
    route = respx.post(URL).respond(json=DISARMED)
    res = client.trading.cancel_all_after(symbol=None, timeout_ms=0)
    assert _body(route) == {"symbol": None, "timeout_ms": 0}
    assert res.armed is False and res.deadline is None


@respx.mock
@pytest.mark.parametrize("timeout_ms", [-1, 1.5, True, False, None, "10000", 10000.0])
def test_bad_timeout_raises(client: cexy.Client, timeout_ms: Any) -> None:
    route = respx.post(URL).respond(json=ARMED)
    with pytest.raises(ValueError):
        client.trading.cancel_all_after(symbol="BTC/USDT", timeout_ms=timeout_ms)
    assert route.call_count == 0


@respx.mock
def test_server_owns_the_range(client: cexy.Client) -> None:
    route = respx.post(URL).respond(json=ARMED)
    client.trading.cancel_all_after(symbol="BTC/USDT", timeout_ms=1)  # no local range check
    assert _body(route)["timeout_ms"] == 1
    err = {"error": {"code": "VALIDATION_FAILED", "message": "", "fields": {"timeout_ms": "5000..600000"}}}
    respx.post(URL).respond(400, json=err)
    with pytest.raises(cexy.ValidationError):
        client.trading.cancel_all_after(symbol="BTC/USDT", timeout_ms=999_999_999)


@respx.mock
def test_retried_after_connection_error(client: cexy.Client) -> None:
    route = respx.post(URL).mock(side_effect=[httpx.ConnectError("refused"), httpx.Response(200, json=ARMED)])
    res = client.trading.cancel_all_after(symbol="BTC/USDT", timeout_ms=10000)
    assert route.call_count == 2 and res.armed is True
    assert json.loads(route.calls[0].request.content) == json.loads(route.calls[1].request.content)


@respx.mock
def test_retried_after_read_timeout(client: cexy.Client) -> None:
    route = respx.post(URL).mock(side_effect=[httpx.ReadTimeout("lost"), httpx.Response(200, json=ARMED)])
    client.trading.cancel_all_after(symbol=None, timeout_ms=10000)
    assert route.call_count == 2


async def test_async_symbol_none_and_response() -> None:
    async with _aclient() as client:
        with respx.mock:
            route = respx.post(URL).respond(json=ARMED)
            res = await client.trading.cancel_all_after(symbol=None, timeout_ms=10000)
            assert b'"symbol":null' in route.calls.last.request.content
    assert res.armed is True


async def test_async_validation_and_retry() -> None:
    async with _aclient() as client:
        with respx.mock:
            route = respx.post(URL).mock(side_effect=[httpx.ConnectError("refused"), httpx.Response(200, json=ARMED)])
            with pytest.raises(ValueError):
                await client.trading.cancel_all_after(symbol="  ", timeout_ms=10000)
            for bad in (-1, 1.5, True):
                with pytest.raises(ValueError):
                    await client.trading.cancel_all_after(symbol="BTC/USDT", timeout_ms=bad)  # type: ignore[arg-type]
            with pytest.raises(TypeError):
                await client.trading.cancel_all_after(timeout_ms=10000)  # type: ignore[call-arg]
            assert route.call_count == 0
            await client.trading.cancel_all_after(symbol="BTC/USDT", timeout_ms=0)
    assert route.call_count == 2


@respx.mock
def test_dead_man_not_armed_conformance_not_retried_nor_recovered(client: cexy.Client) -> None:
    fx = load("errors/dead_man_not_armed.json")
    post = respx.post(PLACE).respond(fx["status"], json=fx["body"])
    lookup = respx.get(url__startswith=BY_CID).respond(200, json={"data": ORDER})
    with pytest.raises(cexy.ConflictError) as exc:
        client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1")
    assert exc.value.code == fx["expect"]["error_code"] == "DEAD_MAN_NOT_ARMED"
    assert exc.value.details == fx["expect"]["details"]
    assert exc.value.retryable is False and fx["expect"]["retry"] is False
    assert post.call_count == 1 and lookup.call_count == 0


@respx.mock
def test_dead_man_not_armed_after_a_lost_first_attempt_is_not_recovered(client: cexy.Client) -> None:
    # The first attempt is lost, so a retry follows; the retry's DEAD_MAN_NOT_ARMED is raised, not
    # turned into an existing order (only ALREADY_EXISTS / IDEMPOTENCY_KEY_CONFLICT trigger that).
    fx = load("errors/dead_man_not_armed.json")
    post = respx.post(PLACE).mock(side_effect=[httpx.ConnectError("refused"), httpx.Response(409, json=fx["body"])])
    lookup = respx.get(url__startswith=BY_CID).respond(200, json={"data": ORDER})
    with pytest.raises(cexy.ConflictError) as exc:
        client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1")
    assert exc.value.code == "DEAD_MAN_NOT_ARMED"
    assert post.call_count == 2 and lookup.call_count == 0
