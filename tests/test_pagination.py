from __future__ import annotations

import httpx
import respx

import cexy
from tests.conftest import BASE, LEDGER


def _pages(request: httpx.Request) -> httpx.Response:
    cursor = request.url.params.get("cursor")
    if cursor is None:
        return httpx.Response(
            200, json={"items": [LEDGER, {**LEDGER, "id": "l2"}], "has_more": True, "next_cursor": "c2"}
        )
    if cursor == "c2":
        return httpx.Response(200, json={"items": [{**LEDGER, "id": "l3"}], "has_more": True, "next_cursor": "c3"})
    return httpx.Response(200, json={"items": [{**LEDGER, "id": "l4"}], "has_more": False})


@respx.mock
def test_auto_paging_iter(client: cexy.Client) -> None:
    route = respx.get(BASE + "/api/v1/account/ledger").mock(side_effect=_pages)
    page = client.account.ledger(asset="BTC", limit=2)
    assert [e.id for e in page.items] == ["l1", "l2"] and page.has_more and page.next_cursor == "c2"
    assert [e.id for e in page.auto_paging_iter()] == ["l1", "l2", "l3", "l4"]
    # filters are carried to every page
    assert all(c.request.url.params["asset"] == "BTC" and c.request.url.params["limit"] == "2" for c in route.calls)
    assert [c.request.url.params.get("cursor") for c in route.calls] == [None, "c2", "c3"]


@respx.mock
def test_next_page_and_last_page(client: cexy.Client) -> None:
    respx.get(BASE + "/api/v1/account/ledger").mock(side_effect=_pages)
    p1 = client.account.ledger()
    p2 = p1.next_page()
    assert p2 is not None and [e.id for e in p2.items] == ["l3"]
    p3 = p2.next_page()
    assert p3 is not None and not p3.has_more and p3.next_cursor is None
    assert p3.next_page() is None


async def test_async_auto_paging_iter() -> None:
    async with cexy.AsyncClient(api_key="ak_test_key", api_secret="test_secret") as client:
        with respx.mock:
            respx.get(BASE + "/api/v1/account/ledger").mock(side_effect=_pages)
            page = await client.account.ledger()
            ids = [e.id async for e in page.auto_paging_iter()]
    assert ids == ["l1", "l2", "l3", "l4"]
