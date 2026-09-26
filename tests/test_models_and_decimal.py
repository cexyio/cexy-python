from __future__ import annotations

import json
from decimal import Decimal

import pytest
import respx

import cexy
from cexy import models as m
from cexy._decimal import to_wire
from tests.conftest import BALANCE, BASE, ORDER


@respx.mock
def test_float_amount_rejected_locally(client: cexy.Client) -> None:
    route = respx.route().respond(200)
    with pytest.raises(TypeError, match="float"):
        client.trading.place_order("BTC/USDT", "buy", "limit", quantity=0.1, price="60000")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        client.pools.join("BTC/USDT", base_amount=1.0, quote_amount="1")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        client.pools.exit("BTC/USDT", shares=0.5)  # type: ignore[arg-type]
    assert route.call_count == 0


@respx.mock
def test_amounts_serialized_as_strings(client: cexy.Client) -> None:
    route = respx.post(BASE + "/api/v1/trading/orders").respond(json={"data": {"order": ORDER, "fills": []}})
    out = client.trading.place_order(
        "BTC/USDT", m.OrderSide.BUY, "limit", quantity=Decimal("0.50000000"), price=60000, time_in_force="gtc"
    )
    body = json.loads(route.calls.last.request.content)
    assert body["quantity"] == "0.50000000"
    assert body["price"] == "60000"
    assert body["side"] == "buy" and body["type"] == "limit" and body["time_in_force"] == "gtc"
    assert "stop_price" not in body  # None is omitted
    assert isinstance(out.order.quantity, Decimal) and out.order.quantity == Decimal("0.5")


def test_to_wire() -> None:
    assert to_wire(Decimal("1E-8")) == "0.00000001"
    assert to_wire("1.50") == "1.50"
    with pytest.raises(ValueError):
        to_wire("abc")
    with pytest.raises(ValueError):
        to_wire(Decimal("NaN"))
    with pytest.raises(TypeError):
        to_wire(True)  # type: ignore[arg-type]


@respx.mock
def test_response_amounts_are_decimal(client: cexy.Client) -> None:
    respx.get(BASE + "/api/v1/account/balances/BTC").respond(json={"data": BALANCE})
    bal = client.account.balance("BTC")
    assert bal.available == Decimal("1.50000000")
    assert str(bal.available) == "1.50000000"  # precision preserved
    assert bal.model_dump(mode="json")["available"] == "1.50000000"


def test_unknown_enum_values_parse() -> None:
    order = m.OrderResponse.model_validate({**ORDER, "status": "some_future_status", "extra_field": 1})
    assert order.status.is_unknown and order.status.value == "some_future_status"
    assert m.OrderStatus("open") is m.OrderStatus.OPEN
    assert not m.OrderStatus.OPEN.is_unknown


def test_error_codes_match_errors_yaml() -> None:
    import re
    from pathlib import Path

    text = (Path(__file__).parent.parent / "spec" / "errors.yaml").read_text()
    codes = set(re.findall(r"^\s+-\s+([A-Z_]+)\s*$", text, re.M))
    assert codes == {c.value for c in m.ErrorCode}
    assert codes == set(cexy.errors.KNOWN_ERROR_CODES)
    # every known code has a specific exception class
    for code in codes:
        assert cexy.errors.error_class_for(code, 400) is not cexy.CexyApiError
