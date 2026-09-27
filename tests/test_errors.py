"""conformance/errors/*.json: error mapping and retry behaviour."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
import respx

import cexy
from tests.conftest import BASE, SleepRecorder, load

JOIN_OK = {"data": {"shares_minted": "1", "base_deposited": "1", "quote_deposited": "100", "shares_held": "1"}}


def _resp(fx: dict) -> httpx.Response:
    return httpx.Response(fx["status"], json=fx["body"], headers=fx.get("headers", {}))


@respx.mock
def test_rate_limited_waits_and_retries(public_client: cexy.Client, sleeps: SleepRecorder) -> None:
    fx = load("errors/rate_limited.json")
    route = respx.get(BASE + "/api/v1/markets").mock(side_effect=[_resp(fx), httpx.Response(200, json={"data": []})])
    assert public_client.markets.list() == []
    assert route.call_count == 2
    assert fx["expect"]["retry"] is True
    assert max(sleeps.calls) >= fx["expect"]["wait_seconds_at_least"]


@respx.mock
def test_rate_limited_exhausted_raises(public_client: cexy.Client, sleeps: SleepRecorder) -> None:
    fx = load("errors/rate_limited.json")
    route = respx.get(BASE + "/api/v1/markets").mock(return_value=_resp(fx))
    with pytest.raises(cexy.RateLimitError) as exc:
        public_client.markets.list()
    assert route.call_count == 4  # 1 + max_retries
    assert exc.value.retry_after == 2
    assert exc.value.request_id == "req_test_1"
    assert all(s >= 2 for s in sleeps.calls)


@respx.mock
def test_retry_after_uses_larger_of_header_and_details(public_client: cexy.Client, sleeps: SleepRecorder) -> None:
    body = {"error": {"code": "RATE_LIMITED", "message": "x", "details": {"retry_after_seconds": 5}, "retryable": True}}
    respx.get(BASE + "/api/v1/markets").mock(
        side_effect=[
            httpx.Response(429, json=body, headers={"Retry-After": "1"}),
            httpx.Response(200, json={"data": []}),
        ]
    )
    public_client.markets.list()
    assert sleeps.calls[-1] >= 5


@respx.mock
def test_concurrent_modification_retries_with_same_idempotency_key(client: cexy.Client, sleeps: SleepRecorder) -> None:
    fx = load("errors/idempotency_in_flight.json")
    route = respx.post(BASE + "/api/v1/pools/BTC%2FUSDT/join").mock(
        side_effect=[_resp(fx), httpx.Response(200, json=JOIN_OK)]
    )
    out = client.pools.join("BTC/USDT", base_amount="1", quote_amount=Decimal("100"))
    assert out.shares_minted == Decimal("1")
    assert route.call_count == 2
    keys = [c.request.headers["Idempotency-Key"] for c in route.calls]
    assert fx["expect"]["same_idempotency_key"] is True
    assert keys[0] == keys[1] and len(keys[0]) >= 32
    assert sleeps.calls[0] >= 2  # details.retry_after_seconds


@respx.mock
def test_each_call_gets_a_new_idempotency_key(client: cexy.Client) -> None:
    route = respx.post(BASE + "/api/v1/pools/BTC%2FUSDT/join").respond(json=JOIN_OK)
    client.pools.join("BTC/USDT", base_amount="1", quote_amount="1")
    client.pools.join("BTC/USDT", base_amount="1", quote_amount="1", idempotency_key="my-key")
    keys = [c.request.headers["Idempotency-Key"] for c in route.calls]
    assert keys[0] != "my-key" and keys[1] == "my-key"


@respx.mock
def test_unknown_code_maps_to_base_class(client: cexy.Client) -> None:
    fx = load("errors/unknown_code.json")
    route = respx.get(BASE + "/api/v1/account/balances").mock(return_value=_resp(fx))
    with pytest.raises(cexy.CexyApiError) as exc:
        client.account.balances()
    assert type(exc.value).__name__ == fx["expect"]["error_class"]
    assert exc.value.code == fx["expect"]["error_code"]
    assert not exc.value.is_known_code
    assert route.call_count == 1


@respx.mock
def test_insufficient_funds_not_retried(client: cexy.Client) -> None:
    fx = load("errors/insufficient_funds.json")
    route = respx.post(BASE + "/api/v1/trading/orders").mock(return_value=_resp(fx))
    with pytest.raises(cexy.UnprocessableError) as exc:
        client.trading.place_order("BTC/USDT", "buy", "limit", quantity="1.5", price="10")
    assert route.call_count == 1
    assert exc.value.code == fx["expect"]["error_code"]
    assert exc.value.details == fx["expect"]["details"]
    assert exc.value.request_id == "req_test_2"
    assert exc.value.retryable is False


@pytest.mark.parametrize(
    ("status", "code", "cls"),
    [
        (400, "VALIDATION_FAILED", cexy.ValidationError),
        (401, "TOKEN_EXPIRED", cexy.AuthenticationError),
        (403, "API_KEY_NOT_ALLOWED", cexy.ForbiddenError),
        (404, "NOT_FOUND", cexy.NotFoundError),
        (409, "ALREADY_EXISTS", cexy.ConflictError),
        (422, "MARKET_UNAVAILABLE", cexy.UnprocessableError),
        (503, "UNDER_MAINTENANCE", cexy.ServerError),
    ],
)
@respx.mock
def test_error_classes(client: cexy.Client, status: int, code: str, cls: type) -> None:
    body = {"error": {"code": code, "message": "m", "fields": {"price": "too many decimals"}, "retryable": False}}
    respx.get(BASE + "/api/v1/account/balances").respond(status, json=body)
    with pytest.raises(cls) as exc:
        client.account.balances()
    assert exc.value.fields == {"price": "too many decimals"}


@respx.mock
def test_non_json_error_body_does_not_crash(public_client: cexy.Client) -> None:
    respx.get(BASE + "/api/v1/markets").respond(502, text="<html>bad gateway</html>")
    with pytest.raises(cexy.ServerError) as exc:
        public_client.markets.list()
    assert exc.value.status == 502


@respx.mock
def test_get_retries_network_errors(public_client: cexy.Client) -> None:
    route = respx.get(BASE + "/api/v1/markets").mock(
        side_effect=[httpx.ReadTimeout("slow"), httpx.Response(200, json={"data": []})]
    )
    assert public_client.markets.list() == []
    assert route.call_count == 2


def test_max_retries_zero() -> None:
    c = cexy.Client(max_retries=0)
    with respx.mock:
        route = respx.get(BASE + "/api/v1/markets").respond(
            503, json={"error": {"code": "INTERNAL", "message": "", "retryable": True}}
        )
        with pytest.raises(cexy.ServerError):
            c.markets.list()
    assert route.call_count == 1


def test_every_spec_error_code_maps_to_a_class() -> None:
    """Every ErrorCode in the spec is known, and its class matches its HTTP status family."""
    import json
    from pathlib import Path

    from cexy import errors

    spec = json.loads((Path(__file__).parents[1] / "spec" / "openapi.sdk.json").read_text())
    codes = spec["components"]["schemas"]["ErrorCode"]["enum"]
    assert set(codes) <= errors.KNOWN_ERROR_CODES
    err = errors.from_response(451, {"error": {"code": "JURISDICTION_BLOCKED", "message": "x", "retryable": False}}, {})
    assert type(err) is errors.JurisdictionBlockedError
    assert isinstance(err, errors.ForbiddenError)  # existing ForbiddenError handlers still catch it
    assert cexy.JurisdictionBlockedError is errors.JurisdictionBlockedError
