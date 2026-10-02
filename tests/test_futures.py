"""Futures data (read only): the facade, signing, and the history paging rules
(shared conformance: futures/history_paging.json)."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx
import pytest
import respx

import cexy
from tests.conftest import BASE, KEY, SECRET, AsyncSleepRecorder, SleepRecorder, load

PAGING = load("futures/history_paging.json")
CASES = PAGING["cases"]
FILLS_URL = BASE + "/api/v1/futures/fills"
FUNDING_URL = BASE + "/api/v1/futures/funding"
AS_OF = "2026-10-02T08:00:00Z"
MARKET = {
    "coin": "BTC",
    "max_leverage": 40,
    "size_decimals": 5,
    "mark_price": "60000.0",
    "oracle_price": "60001.0",
    "mid_price": "60000.5",
    "funding_rate": "0.0000125",
    "open_interest": "1000.5",
    "volume_24h": "123456.7",
    "price_24h_ago": "59000.0",
}
SIGNED = ("X-API-Key", "X-API-Timestamp", "X-API-Nonce", "X-API-Signature")


class Pages:
    """Serves a case's pages in order and records the requests."""

    def __init__(self, case: Dict[str, Any], rows_key: str = "fills") -> None:
        self.pages = case["pages"]
        self.rows_key = rows_key
        self.requests: List[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        n = len(self.requests)
        self.requests.append(request)
        assert n < len(self.pages), "more requests than the case allows"
        page = self.pages[n]
        assert request.url.params.get("cursor") == page["request_cursor"]
        if page["request_cursor"] is None:
            assert b"cursor" not in request.url.query
        body = dict(page["response"])
        if self.rows_key != "fills":
            body[self.rows_key] = [funding_row(r) for r in body.pop("fills")]
        return httpx.Response(200, json={"data": body})


def funding_row(fill: Dict[str, Any]) -> Dict[str, Any]:
    # The fixture's rows are fills; for funding, carry the id in an extra field.
    return {"coin": fill["coin"], "amount": "-0.5", "position_size": "0.01", "rate": "0.0000125",
            "time": fill["time"], "id": fill["id"]}  # fmt: skip


def check(
    case: Dict[str, Any], pages: Pages, ids: List[str], err: Optional[BaseException], sleeps: List[float]
) -> None:
    exp = case["expect"]
    assert len(pages.requests) == exp["requests"]
    assert len(sleeps) == exp["sleeps"]
    if "error_code" in exp:
        assert isinstance(err, cexy.PagingStalledError)
        assert isinstance(err, cexy.CexyApiError)
        assert err.code == exp["error_code"] and err.retryable is exp["error_retryable"]
        assert ids == exp["ids_before_error"]
    else:
        assert err is None
        assert ids == exp["ids"]
    if "encoded_query_of_request_2" in exp:
        assert pages.requests[1].url.query.decode() == exp["encoded_query_of_request_2"]
    for req in pages.requests:
        assert all(h in req.headers for h in SIGNED)
        assert "X-API-Secret" not in req.headers


def sync_client(sleeps: SleepRecorder) -> cexy.Client:
    c = cexy.Client(api_key=KEY, api_secret=SECRET)
    c._transport._sleep = sleeps
    return c


def test_fixture_max_retries_is_the_default() -> None:
    assert PAGING["operation"] == "GET /api/v1/futures/fills"
    assert PAGING["max_retries"] == 3


@pytest.mark.parametrize("rows_key", ["fills", "funding"])
@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_history_paging_conformance_sync(case: Dict[str, Any], rows_key: str) -> None:
    sleeps = SleepRecorder()
    client = sync_client(sleeps)
    pages = Pages(case, rows_key)
    url = FILLS_URL if rows_key == "fills" else FUNDING_URL
    ids: List[str] = []
    err: Optional[BaseException] = None
    with respx.mock:
        respx.get(url__startswith=url).mock(side_effect=pages)
        it = client.futures.iter_fills() if rows_key == "fills" else client.futures.iter_funding()
        try:
            for row in it:
                ids.append(row.id if rows_key == "fills" else row.model_extra["id"])  # type: ignore[union-attr,index]
        except cexy.CexyApiError as exc:
            err = exc
    check(case, pages, ids, err, sleeps.calls)


@pytest.mark.parametrize("rows_key", ["fills", "funding"])
@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
async def test_history_paging_conformance_async(case: Dict[str, Any], rows_key: str) -> None:
    sleeps = AsyncSleepRecorder()
    pages = Pages(case, rows_key)
    url = FILLS_URL if rows_key == "fills" else FUNDING_URL
    ids: List[str] = []
    err: Optional[BaseException] = None
    async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET) as client:
        client._transport._sleep = sleeps
        with respx.mock:
            respx.get(url__startswith=url).mock(side_effect=pages)
            it = client.futures.iter_fills() if rows_key == "fills" else client.futures.iter_funding()
            try:
                async for row in it:
                    ids.append(row.id if rows_key == "fills" else row.model_extra["id"])  # type: ignore[union-attr,index]
            except cexy.CexyApiError as exc:
                err = exc
    check(case, pages, ids, err, sleeps.calls)


@pytest.mark.parametrize("max_retries", [0, 1, 5])
def test_max_retries_is_configurable(max_retries: int) -> None:
    stalled = next(c for c in CASES if c["id"] == "busy_provider_gives_up_after_max_retries")
    busy = stalled["pages"][1]
    case = {"pages": [stalled["pages"][0]] + [busy] * (max_retries + 1)}
    sleeps = SleepRecorder()
    client = sync_client(sleeps)
    pages = Pages(case)
    with respx.mock:
        respx.get(url__startswith=FILLS_URL).mock(side_effect=pages)
        with pytest.raises(cexy.PagingStalledError) as exc:
            list(client.futures.iter_fills(max_retries=max_retries))
    assert len(pages.requests) == max_retries + 2 and len(sleeps.calls) == max_retries
    assert exc.value.details == {"retries": max_retries}
    assert client._transport.policy.is_retryable(exc.value)


def test_busy_count_resets_after_progress() -> None:
    # Three busy answers, a page with rows, three more busy answers: never stalls.
    row = CASES[0]["pages"][0]["response"]["fills"][0]
    seq = [(None, [row], "c1")] + [("c1", [], "c1")] * 3 + [("c1", [row], "c2")] + [("c2", [], "c2")] * 3
    seq.append(("c2", [], None))
    case = {
        "pages": [
            {"request_cursor": c, "response": {"has_account": True, "fills": r, "next_cursor": n}} for c, r, n in seq
        ]
    }
    sleeps = SleepRecorder()
    pages = Pages(case)
    with respx.mock:
        respx.get(url__startswith=FILLS_URL).mock(side_effect=pages)
        assert len(list(sync_client(sleeps).futures.iter_fills())) == 2
    assert len(pages.requests) == len(seq) and len(sleeps.calls) == 6


@respx.mock
def test_public_market_data_sends_no_credentials() -> None:
    respx.get(BASE + "/api/v1/futures/markets").respond(
        json={"data": {"markets": [MARKET], "as_of": AS_OF, "stale": False}}
    )
    respx.get(BASE + "/api/v1/futures/markets/BTC").respond(
        json={"data": {"market": MARKET, "as_of": AS_OF, "stale": True}}
    )
    book = respx.get(BASE + "/api/v1/futures/markets/BTC/orderbook").respond(
        json={
            "data": {
                "coin": "BTC",
                "bids": [{"price": "59999", "size": "1.5", "orders": 3}],
                "asks": [],
                "time": 1790000000000,
                "as_of": AS_OF,
                "stale": False,
            }
        }
    )
    candles = respx.get(BASE + "/api/v1/futures/markets/BTC/candles").respond(
        json={"data": {"coin": "BTC", "interval": "1h", "candles": [], "as_of": AS_OF, "stale": False}}
    )
    trades = respx.get(BASE + "/api/v1/futures/markets/BTC/trades").respond(
        json={"data": {"coin": "BTC", "trades": [], "as_of": AS_OF, "stale": False}}
    )
    c = cexy.Client(api_key=KEY, api_secret=SECRET)
    ms = c.futures.markets()
    assert ms.markets[0].mark_price == Decimal("60000.0") and not ms.stale
    assert c.futures.market("BTC").stale is True
    assert str(c.futures.order_book("BTC", depth=5).bids[0].size) == "1.5"
    c.futures.candles("BTC", "1h", before=1790000000000)
    c.futures.trades("BTC", limit=50)
    assert book.calls.last.request.url.params["depth"] == "5"
    assert candles.calls.last.request.url.query == b"interval=1h&before=1790000000000"
    assert trades.calls.last.request.url.params["limit"] == "50"
    for route in respx.mock.routes:
        for call in route.calls:
            assert not any(h in call.request.headers for h in SIGNED)


@respx.mock
def test_account_reads_are_signed_and_report_no_account() -> None:
    pos = respx.get(BASE + "/api/v1/futures/positions").respond(
        json={"data": {"has_account": False, "positions": None, "as_of": None, "stale": False}}
    )
    orders = respx.get(BASE + "/api/v1/futures/orders").respond(
        json={"data": {"has_account": False, "orders": [], "as_of": None, "stale": False}}
    )
    c = cexy.Client(api_key=KEY, api_secret=SECRET)
    p = c.futures.positions()
    assert p.has_account is False and p.positions is None
    assert c.futures.open_orders().has_account is False
    for route in (pos, orders):
        headers = route.calls.last.request.headers
        assert all(h in headers for h in SIGNED) and "X-API-Secret" not in headers


@pytest.mark.parametrize(
    "call",
    [
        lambda f: f.positions(),
        lambda f: f.open_orders(),
        lambda f: f.fills(),
        lambda f: f.funding(),
        lambda f: list(f.iter_fills()),
        lambda f: list(f.iter_funding()),
    ],
)
@respx.mock(assert_all_called=False)
def test_account_reads_need_a_key(respx_mock: respx.MockRouter, call: Any) -> None:
    route = respx_mock.route().respond(200)
    with pytest.raises(cexy.MissingCredentialsError):
        call(cexy.Client().futures)
    assert route.call_count == 0


@respx.mock
def test_futures_data_unavailable_is_retried_then_raised() -> None:
    body = {
        "error": {
            "code": "SERVICE_UNAVAILABLE",
            "message": "futures data unavailable",
            "details": {"reason": "futures_data_unavailable"},
            "retryable": True,
        }
    }
    route = respx.get(BASE + "/api/v1/futures/markets").mock(
        return_value=httpx.Response(503, json=body, headers={"Retry-After": "2"})
    )
    sleeps = SleepRecorder()
    c = cexy.Client(max_retries=2)
    c._transport._sleep = sleeps
    with pytest.raises(cexy.ServerError) as exc:
        c.futures.markets()
    assert exc.value.code == "SERVICE_UNAVAILABLE" and exc.value.retryable
    assert exc.value.details["reason"] == "futures_data_unavailable"
    assert route.call_count == 3 and len(sleeps.calls) == 2 and all(2 <= s <= 2.25 for s in sleeps.calls)


def test_paging_stalled_is_local_and_retryable() -> None:
    err = cexy.PagingStalledError("stalled", retries=3)
    assert err.code == "PAGING_STALLED" and err.retryable and not err.is_known_code
    assert json.dumps(err.details) == '{"retries": 3}'
