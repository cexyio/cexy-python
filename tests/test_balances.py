"""BalanceResponse.held_incoming: typed, already part of `locked`, [] when a server omits it."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

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
