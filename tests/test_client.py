from __future__ import annotations

import pytest
import respx

import cexy
from tests.conftest import BASE, KEY, SECRET


def test_version() -> None:
    assert cexy.__version__ == "0.1.0.dev13"
    assert cexy.USER_AGENT == "cexy-python/0.1.0.dev13"


@pytest.mark.parametrize(
    "url", ["http://api.cexy.io", "ftp://x", "https://user:pw@api.cexy.io", "https://api.cexy.io?k=v"]
)
def test_base_url_validated(url: str) -> None:
    with pytest.raises(ValueError):
        cexy.Client(base_url=url)


def test_base_url_default_and_custom() -> None:
    assert cexy.Client().base_url == "https://api.cexy.io"
    assert cexy.Client(base_url="https://example.test/").base_url == "https://example.test"


def test_authenticator_and_keys_are_exclusive() -> None:
    with pytest.raises(ValueError):
        cexy.Client(api_key=KEY, api_secret=SECRET, authenticator=cexy.HeaderKeyAuth(KEY, SECRET))


@respx.mock
def test_custom_authenticator_extension_point() -> None:
    class Signer:
        def apply(self, method, url, headers, body):  # type: ignore[no-untyped-def]
            headers["X-API-Key"] = "ak_test_key"
            headers["X-Signature"] = f"sig({method} {url})"

        def secrets(self):  # type: ignore[no-untyped-def]
            return ("ak_test_key",)

    route = respx.get(BASE + "/api/v1/account/balances").respond(json={"data": []})
    cexy.Client(authenticator=Signer()).account.balances()
    assert route.calls.last.request.headers["X-Signature"] == "sig(GET https://api.cexy.io/api/v1/account/balances)"


def test_repr_without_credentials() -> None:
    assert "credentials=None" in repr(cexy.Client())


@respx.mock
def test_context_manager_and_symbol_encoding() -> None:
    route = respx.get(BASE + "/api/v1/markets/BTC%2FUSDT/orderbook").respond(
        json={
            "data": {
                "symbol": "BTC/USDT",
                "bids": [["1", "2"]],
                "asks": [],
                "sequence": 5,
                "timestamp": "2026-09-10T11:42:31Z",
            }
        }
    )
    with cexy.Client() as c:
        book = c.markets.orderbook("BTC/USDT", depth=10)
    assert book.sequence == 5 and str(book.bids[0][0]) == "1"
    assert route.calls.last.request.url.params["depth"] == "10"


async def test_async_client_public_call() -> None:
    with respx.mock:
        route = respx.get(BASE + "/api/v1/time").respond(
            json={"data": {"iso": "2026-09-26T00:00:00+00:00", "epoch_ms": 1}}
        )
        async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET) as c:
            t = await c.time()
    assert t.epoch_ms == 1
    assert "X-API-Key" not in route.calls.last.request.headers


def test_unknown_query_param_rejected() -> None:
    from cexy._common import build_query, operation

    with pytest.raises(ValueError):
        build_query(operation("list_markets"), {"api_key": KEY})


def test_naive_datetime_rejected() -> None:
    from datetime import datetime

    with pytest.raises(ValueError):
        cexy.Client(api_key=KEY, api_secret=SECRET).exports.orders(from_=datetime(2026, 1, 1))


@pytest.mark.parametrize(
    "url", ["http://localhost", "http://127.0.0.1", "http://[::1]", "http://localhost/api", "https://api.cexy.io"]
)
def test_insecure_base_url_only_for_loopback_with_opt_in(url: str) -> None:
    c = cexy.Client(base_url=url, allow_insecure=True)
    assert c.base_url == url.rstrip("/")


@pytest.mark.parametrize("url", ["http://localhost", "http://127.0.0.1", "http://[::1]"])
def test_insecure_base_url_needs_opt_in(url: str) -> None:
    with pytest.raises(cexy.ConfigurationError):
        cexy.Client(base_url=url)
    with pytest.raises(cexy.ConfigurationError):
        cexy.AsyncClient(base_url=url)


@pytest.mark.parametrize(
    "url", ["http://api.cexy.io", "http://example.test", "http://203.0.113.5", "http://localhost.example.test"]
)
def test_insecure_base_url_refused_for_other_hosts(url: str) -> None:
    with pytest.raises(cexy.ConfigurationError, match="only allowed for localhost"):
        cexy.Client(base_url=url, allow_insecure=True)
