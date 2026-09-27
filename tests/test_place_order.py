"""Retry safety for POST /trading/orders (client_order_id + by-client-id check)."""

from __future__ import annotations

import json
import uuid

import httpx
import pytest
import respx

import cexy
import cexy.models
from tests.conftest import BASE, ORDER

PLACE = BASE + "/api/v1/trading/orders"
BY_CID = BASE + "/api/v1/trading/orders/by-client-id/"
OK = {"data": {"order": ORDER, "fills": []}}
NOT_FOUND = {"error": {"code": "NOT_FOUND", "message": "no such order", "retryable": False}}


def _cid(request: httpx.Request) -> str:
    return str(json.loads(request.content)["client_order_id"])


@respx.mock
def test_client_order_id_auto_generated_and_no_idempotency_key(client: cexy.Client) -> None:
    route = respx.post(PLACE).respond(json=OK)
    client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="60000")
    req = route.calls.last.request
    uuid.UUID(_cid(req))
    assert "Idempotency-Key" not in req.headers


@respx.mock
def test_caller_client_order_id_kept(client: cexy.Client) -> None:
    route = respx.post(PLACE).respond(json=OK)
    client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1", client_order_id="mine-1")
    assert _cid(route.calls.last.request) == "mine-1"


@respx.mock
def test_ambiguous_failure_finds_existing_order(client: cexy.Client) -> None:
    post = respx.post(PLACE).mock(side_effect=[httpx.ReadTimeout("lost response")])
    lookup = respx.get(url__startswith=BY_CID).respond(json={"data": {**ORDER, "client_order_id": "abc"}})
    out = client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="60000", client_order_id="abc")
    assert post.call_count == 1  # never resent
    assert lookup.call_count == 1
    assert str(lookup.calls.last.request.url).endswith("/by-client-id/abc")
    assert out.order.id == "o1" and out.fills == []


@respx.mock
def test_ambiguous_failure_not_found_resends_same_client_order_id(client: cexy.Client) -> None:
    post = respx.post(PLACE).mock(side_effect=[httpx.ReadTimeout("lost"), httpx.Response(200, json=OK)])
    lookup = respx.get(url__startswith=BY_CID).respond(404, json=NOT_FOUND)
    client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="60000")
    assert post.call_count == 2 and lookup.call_count == 1
    assert _cid(post.calls[0].request) == _cid(post.calls[1].request)


@respx.mock
def test_ambiguous_5xx_checks_before_retry(client: cexy.Client) -> None:
    err = {"error": {"code": "INTERNAL", "message": "", "retryable": True}}
    post = respx.post(PLACE).mock(side_effect=[httpx.Response(502, json=err), httpx.Response(200, json=OK)])
    lookup = respx.get(url__startswith=BY_CID).respond(404, json=NOT_FOUND)
    client.trading.place_order("BTC/USDT", "sell", "market", quantity="0.1")
    assert lookup.call_count == 1 and post.call_count == 2


@respx.mock
def test_already_exists_on_retry_returns_existing(client: cexy.Client) -> None:
    dup = {"error": {"code": "ALREADY_EXISTS", "message": "client order id in use", "retryable": False}}
    respx.post(PLACE).mock(side_effect=[httpx.ReadTimeout("lost"), httpx.Response(409, json=dup)])
    lookup = respx.get(url__startswith=BY_CID).mock(
        side_effect=[httpx.Response(404, json=NOT_FOUND), httpx.Response(200, json={"data": ORDER})]
    )
    out = client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1")
    assert out.order.id == "o1" and lookup.call_count == 2


@respx.mock
def test_connect_error_retries_without_lookup(client: cexy.Client) -> None:
    # A connection that never opened cannot have placed an order.
    post = respx.post(PLACE).mock(side_effect=[httpx.ConnectError("refused"), httpx.Response(200, json=OK)])
    lookup = respx.get(url__startswith=BY_CID).respond(404, json=NOT_FOUND)
    client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1")
    assert post.call_count == 2 and lookup.call_count == 0


@respx.mock
def test_rate_limited_place_order_retried_without_lookup(client: cexy.Client) -> None:
    rl = {"error": {"code": "RATE_LIMITED", "message": "", "details": {"retry_after_seconds": 1}, "retryable": True}}
    post = respx.post(PLACE).mock(side_effect=[httpx.Response(429, json=rl), httpx.Response(200, json=OK)])
    lookup = respx.get(url__startswith=BY_CID).respond(404, json=NOT_FOUND)
    client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1")
    assert post.call_count == 2 and lookup.call_count == 0


@respx.mock
def test_validation_error_not_retried(client: cexy.Client) -> None:
    body = {"error": {"code": "PRECISION_EXCEEDED", "message": "", "fields": {"price": "8 dp max"}, "retryable": False}}
    post = respx.post(PLACE).respond(400, json=body)
    with pytest.raises(cexy.ValidationError) as exc:
        client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1.000000001")
    assert post.call_count == 1 and exc.value.fields == {"price": "8 dp max"}


@respx.mock
def test_order_endpoints_do_not_rely_on_idempotency_key(client: cexy.Client) -> None:
    # The server does not honour Idempotency-Key on order endpoints; safety rests on
    # client_order_id (place) and on cancels being repeatable.
    cancel = respx.delete(BASE + "/api/v1/trading/orders/o1").respond(json={"data": ORDER})
    cancel_all = respx.post(BASE + "/api/v1/trading/orders/cancel-all").respond(
        json={"data": {"cancelled": ["o1"], "already_closed": [], "failed": [], "failures": [], "has_more": False}}
    )
    client.trading.cancel_order("o1")
    res = client.trading.cancel_all(symbol="BTC/USDT")
    assert "Idempotency-Key" not in cancel.calls.last.request.headers
    assert "Idempotency-Key" not in cancel_all.calls.last.request.headers
    assert json.loads(cancel_all.calls.last.request.content) == {"symbol": "BTC/USDT"}
    assert res.cancelled == ["o1"]


@respx.mock
def test_cancel_all_requires_explicit_symbol(client: cexy.Client) -> None:
    route = respx.post(BASE + "/api/v1/trading/orders/cancel-all").respond(
        json={"data": {"cancelled": [], "already_closed": [], "failed": [], "failures": [], "has_more": False}}
    )
    with pytest.raises(TypeError):
        client.trading.cancel_all()  # type: ignore[call-arg]
    assert route.call_count == 0
    client.trading.cancel_all(symbol=None)  # explicit: every market
    assert json.loads(route.calls.last.request.content) == {}


INVALID_STATE = {"error": {"code": "INVALID_STATE", "message": "order is not open", "retryable": False}}


@respx.mock
def test_cancel_retry_invalid_state_returns_current_order(client: cexy.Client) -> None:
    # First attempt's response is lost; the retry finds the order already cancelled.
    cancel = respx.delete(BASE + "/api/v1/trading/orders/o1").mock(
        side_effect=[httpx.ReadTimeout("lost"), httpx.Response(409, json=INVALID_STATE)]
    )
    get = respx.get(BASE + "/api/v1/trading/orders/o1").respond(json={"data": {**ORDER, "status": "cancelled"}})
    out = client.trading.cancel_order("o1")
    assert cancel.call_count == 2 and get.call_count == 1
    assert out.status == cexy.models.OrderStatus.CANCELLED


@respx.mock
def test_cancel_first_attempt_invalid_state_raises(client: cexy.Client) -> None:
    # On the first attempt INVALID_STATE is a real answer (e.g. the order already filled).
    respx.delete(BASE + "/api/v1/trading/orders/o1").respond(409, json=INVALID_STATE)
    get = respx.get(BASE + "/api/v1/trading/orders/o1").respond(json={"data": ORDER})
    with pytest.raises(cexy.ConflictError) as exc:
        client.trading.cancel_order("o1")
    assert exc.value.code == "INVALID_STATE" and get.call_count == 0


async def test_async_place_order_recovery() -> None:
    async with cexy.AsyncClient(api_key="ak_test_key", api_secret="test_secret") as client:

        async def no_sleep(_: float) -> None:
            return None

        client._transport._sleep = no_sleep
        with respx.mock:
            post = respx.post(PLACE).mock(side_effect=[httpx.ReadTimeout("lost")])
            respx.get(url__startswith=BY_CID).respond(json={"data": ORDER})
            out = await client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1")
    assert out.order.id == "o1" and post.call_count == 1
