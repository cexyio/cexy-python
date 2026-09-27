"""The SDK never follows HTTP redirects: credentials must not reach another host, and orders are never re-posted."""

from __future__ import annotations

import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator, List, Tuple

import httpx
import pytest

import cexy
from tests.conftest import KEY, SECRET

API = "https://api.cexy.io"
EVIL = "http://evil.invalid/collect"


def _redirecting(status: int, hits: List[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "evil.invalid":
            hits.append(request)
            return httpx.Response(200, json={"data": []})
        return httpx.Response(status, headers={"location": EVIL})

    return httpx.MockTransport(handler)


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_redirect_is_an_error_even_with_a_following_http_client(status: int) -> None:
    hits: List[httpx.Request] = []
    http = httpx.Client(transport=_redirecting(status, hits), follow_redirects=True)
    client = cexy.Client(api_key=KEY, api_secret=SECRET, http_client=http, max_retries=3)
    with pytest.raises(cexy.CexyApiError) as info:
        client.account.balances()
    assert info.value.code == "UNEXPECTED_REDIRECT"
    assert info.value.status == status
    assert info.value.retryable is False
    assert hits == []


def test_place_order_307_is_posted_once_and_not_looked_up() -> None:
    hits: List[httpx.Request] = []
    seen: List[Tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.url.host == "evil.invalid":
            hits.append(request)
            return httpx.Response(200, json={})
        return httpx.Response(307, headers={"location": EVIL})

    http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)
    client = cexy.Client(api_key=KEY, api_secret=SECRET, http_client=http)
    with pytest.raises(cexy.CexyApiError) as info:
        client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1", client_order_id="cid-r")
    assert info.value.code == "UNEXPECTED_REDIRECT"
    assert seen == [("POST", "/api/v1/trading/orders")]
    assert hits == []


def test_async_redirect_is_an_error_even_with_a_following_http_client() -> None:
    hits: List[httpx.Request] = []

    async def run() -> None:
        http = httpx.AsyncClient(transport=_redirecting(302, hits), follow_redirects=True)
        async with cexy.AsyncClient(api_key=KEY, api_secret=SECRET, http_client=http) as client:
            with pytest.raises(cexy.CexyApiError) as info:
                await client.account.balances()
            assert info.value.code == "UNEXPECTED_REDIRECT"

    asyncio.run(run())
    assert hits == []


@pytest.fixture
def two_servers() -> Iterator[Tuple[str, str, List[dict]]]:
    """A local "API" that always redirects to a second local server, which records every request."""
    hits: List[dict] = []
    servers: List[ThreadingHTTPServer] = []

    class Target(BaseHTTPRequestHandler):
        def _any(self) -> None:
            hits.append(dict(self.headers))
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"data": []}')

        do_GET = do_POST = _any

        def log_message(self, *args: object) -> None:
            pass

    target = ThreadingHTTPServer(("127.0.0.1", 0), Target)
    servers.append(target)
    target_url = f"http://127.0.0.1:{target.server_address[1]}"

    class Api(BaseHTTPRequestHandler):
        def _any(self) -> None:
            code = 307 if self.command == "POST" else 302
            self.send_response(code)
            self.send_header("location", f"{target_url}/collect")
            self.send_header("content-length", "0")
            self.end_headers()

        do_GET = do_POST = _any

        def log_message(self, *args: object) -> None:
            pass

    api = ThreadingHTTPServer(("127.0.0.1", 0), Api)
    servers.append(api)
    for s in servers:
        threading.Thread(target=s.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{api.server_address[1]}", target_url, hits
    finally:
        for s in servers:
            s.shutdown()
            s.server_close()


def test_real_servers_second_origin_receives_nothing(two_servers: Tuple[str, str, List[dict]]) -> None:
    api, _target, hits = two_servers
    http = httpx.Client(follow_redirects=True)
    client = cexy.Client(api_key=KEY, api_secret=SECRET, base_url=api, allow_insecure=True, http_client=http)
    with pytest.raises(cexy.CexyApiError, match="UNEXPECTED_REDIRECT"):
        client.account.balances()
    with pytest.raises(cexy.CexyApiError, match="UNEXPECTED_REDIRECT"):
        client.trading.place_order("BTC/USDT", "buy", "limit", quantity="0.5", price="1", client_order_id="cid-s")
    assert hits == []
