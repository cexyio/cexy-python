"""BalanceResponse.held_incoming: typed, already part of `locked`, [] when a server omits it."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import respx

import cexy
from tests.conftest import BASE, KEY, SECRET

BALANCES = BASE + "/api/v1/account/balances"
ROW = {"asset": "USDT", "available": "90.00", "locked": "10.00", "pending": "0", "total": "100.00"}
HELD = [
    {"transfer_id": "a" * 24, "amount": "4.00", "available_at": "2026-09-30T10:00:00.123Z"},
    {"transfer_id": "b" * 24, "amount": "6.00", "available_at": "2026-10-01T10:00:00.456Z"},
]


@respx.mock
def test_two_held_entries_decode_typed(client: cexy.Client) -> None:
    respx.get(BALANCES).respond(json={"data": [dict(ROW, held_incoming=HELD)]})
    (b,) = client.account.balances()
    assert [h.transfer_id for h in b.held_incoming] == ["a" * 24, "b" * 24]
    assert b.held_incoming[0].amount == Decimal("4.00")
    assert b.held_incoming[0].available_at == datetime(2026, 9, 30, 10, 0, 0, 123000, tzinfo=timezone.utc)
    assert b.locked == Decimal("10.00")  # held amounts are already inside locked


@respx.mock
def test_empty_list(client: cexy.Client) -> None:
    respx.get(BALANCES + "/USDT").respond(json={"data": dict(ROW, held_incoming=[])})
    assert client.account.balance("USDT").held_incoming == []


@respx.mock
def test_missing_field_defaults_to_empty_list(client: cexy.Client) -> None:
    respx.get(BALANCES).respond(json={"data": [ROW]})
    respx.get(BALANCES + "/USDT").respond(json={"data": ROW})
    assert client.account.balances()[0].held_incoming == []
    assert client.account.balance("USDT").held_incoming == []


@respx.mock
def test_async_missing_field_defaults_to_empty_list() -> None:
    respx.get(BALANCES).respond(json={"data": [ROW]})

    async def run() -> None:
        async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET) as c:
            assert (await c.account.balances())[0].held_incoming == []

    asyncio.run(run())


SUB = BASE + "/api/v1/account/sub-accounts/sub%2F1%20%3Fx/balances"


@respx.mock
def test_sub_account_balances_path_auth_and_held(client: cexy.Client) -> None:
    route = respx.get(SUB).respond(json={"data": [dict(ROW, held_incoming=HELD)]})
    (b,) = client.account.sub_account_balances("sub/1 ?x")
    assert route.call_count == 1
    req = route.calls.last.request
    assert req.url.raw_path.startswith(b"/api/v1/account/sub-accounts/sub%2F1%20%3Fx/balances")
    assert req.headers["X-API-Key"] == KEY
    assert "Idempotency-Key" not in req.headers
    assert [h.transfer_id for h in b.held_incoming] == ["a" * 24, "b" * 24]


@respx.mock
def test_sub_account_balances_missing_held_defaults(client: cexy.Client) -> None:
    respx.get(BASE + "/api/v1/account/sub-accounts/sub_1/balances").respond(json={"data": [ROW]})
    assert client.account.sub_account_balances("sub_1")[0].held_incoming == []


@respx.mock
def test_sub_account_balances_404_is_not_found_without_retry(client: cexy.Client) -> None:
    route = respx.get(BASE + "/api/v1/account/sub-accounts/other/balances").respond(
        404, json={"error": {"code": "NOT_FOUND", "message": "no such sub-account", "retryable": False}}
    )
    try:
        client.account.sub_account_balances("other")
        raise AssertionError("expected NotFoundError")
    except cexy.NotFoundError:
        pass
    assert route.call_count == 1


@respx.mock
def test_sub_account_balances_empty_id_rejected_before_request(client: cexy.Client) -> None:
    route = respx.route().respond(200, json={"data": []})
    for bad in ("",):
        try:
            client.account.sub_account_balances(bad)
            raise AssertionError("expected ValueError")
        except ValueError:
            pass
    assert route.call_count == 0


def test_async_sub_account_balances() -> None:
    async def run() -> None:
        with respx.mock:
            respx.get(BASE + "/api/v1/account/sub-accounts/sub_1/balances").respond(json={"data": [ROW]})
            async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET) as c:
                rows = await c.account.sub_account_balances("sub_1")
                assert rows[0].held_incoming == []

    asyncio.run(run())


@respx.mock
def test_sub_account_balances_404_without_retryable_or_json_is_not_found_once(client: cexy.Client) -> None:
    for sub, resp in (
        ("no-retryable", httpx.Response(404, json={"error": {"code": "NOT_FOUND", "message": "no such sub-account"}})),
        ("html", httpx.Response(404, text="<html>not found</html>", headers={"content-type": "text/html"})),
    ):
        url = BASE + f"/api/v1/account/sub-accounts/{sub}/balances"
        route = respx.get(url).mock(side_effect=[resp, httpx.Response(200, json={"data": []})])
        try:
            client.account.sub_account_balances(sub)
            raise AssertionError("expected NotFoundError")
        except cexy.NotFoundError as e:
            assert e.retryable is False
        assert route.call_count == 1


@respx.mock
def test_account_id(client: cexy.Client) -> None:
    route = respx.get(f"{BASE}/api/v1/account/id").respond(json={"data": {"user_id": "aaaa0001"}})
    assert client.account.id() == "aaaa0001"
    assert route.calls.last.request.headers["X-API-Key"] == KEY


@respx.mock
async def test_async_account_id() -> None:
    respx.get(f"{BASE}/api/v1/account/id").respond(json={"data": {"user_id": "aaaa0001"}})
    async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET, base_url=BASE) as c:
        assert await c.account.id() == "aaaa0001"
