"""Request signing (planned scheme): conformance/signing/vectors.json (vendored), a RECORDING
server that recomputes every signature from the raw request it received, transport rules, and the
WebSocket auth_key."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterator, List

import httpx
import pytest
import respx

import cexy
from cexy import ws as cws
from cexy.auth import HmacAuth, canonical_path, canonical_query, canonical_request, new_nonce
from cexy.errors import CexyApiError
from tests.conftest import BASE, load
from tests.conftest import ORDER as CONF_ORDER
from tests.test_ws import (  # noqa: F401  (the server fixture comes from test_ws)
    FakeServer,
    make,
    server,
)

V = load("signing/vectors.json")


def _split(target: str) -> tuple[str, str]:
    path, _, query = target.partition("?")
    return path, query


@pytest.mark.parametrize("case", V["rest"], ids=[c["name"] for c in V["rest"]])
def test_vector(case: Dict[str, Any]) -> None:
    path, query = _split(case["request_target"])
    assert canonical_path(path) == case["canonical_path"]
    assert canonical_query(query) == case["canonical_query"]
    assert hashlib.sha256(case["body"].encode()).hexdigest() == case["body_sha256"]
    creq = canonical_request(case["method"], path, query, V["timestamp"], V["nonce"], case["body"].encode())
    assert creq == case["canonical_request"]
    auth = HmacAuth(V["key_id"], V["secret"], now_ms=lambda: int(V["timestamp"]), nonce=lambda: V["nonce"])
    headers: Dict[str, str] = {"X-API-Secret": "must be removed"}
    auth.apply(case["method"], BASE + case["request_target"], headers, case["body"].encode() or None)
    for k, v in case["headers"].items():
        assert headers[k] == v, k
    assert "X-API-Secret" not in headers


def test_vector_ws_and_negative() -> None:
    auth = HmacAuth(V["key_id"], V["secret"])
    w = V["ws"]
    assert auth.sign_websocket_challenge(w["welcome"]["connection_id"], w["welcome"]["challenge"]) == (
        w["auth_key"]["key_id"],
        w["auth_key"]["signature"],
    )
    n = V["negative"]
    right = hmac.new(V["secret"].encode(), n["canonical_request"].encode(), hashlib.sha256).hexdigest()
    wrong = hmac.new(n["wrong_secret"].encode(), n["canonical_request"].encode(), hashlib.sha256).hexdigest()
    assert (right, wrong) == (n["signature_with_right_secret"], n["signature_with_wrong_secret"])


def test_nonce_shape() -> None:
    a = new_nonce()
    assert len(a) == 22 and all(c.isalnum() or c in "-_" for c in a)
    assert new_nonce() != a


# ---- recording server --------------------------------------------------------------------------

ORDER = {
    **CONF_ORDER,
    "id": "6aa9003697a77bccb95b2282",
    "client_order_id": "11111111-1111-1111-1111-111111111111",
}


_UNRESERVED = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
_PCT = re.compile(rb"%([0-9A-Fa-f]{2})")


def _indep_enc(s: str) -> str:
    """Independent of cexy.auth: regex decoding, then a byte-by-byte RFC 3986 encoder."""
    raw = _PCT.sub(lambda m: bytes([int(m.group(1), 16)]), s.encode("utf-8"))
    return "".join(chr(b) if b in _UNRESERVED else f"%{b:02X}" for b in raw)


def _indep_path(path: str) -> str:
    return "/".join(_indep_enc(seg) for seg in path.split("/"))


def _indep_query(query: str) -> str:
    pairs = sorted(
        (_indep_enc(n).encode(), _indep_enc(v).encode())
        for n, _, v in (part.partition("=") for part in query.split("&") if part)
    )
    return "&".join(f"{n.decode()}={v.decode()}" for n, v in pairs)


@pytest.mark.parametrize("case", V["rest"], ids=[c["name"] for c in V["rest"]])
def test_independent_canonicaliser_agrees_with_vectors(case: Dict[str, Any]) -> None:
    path, query = _split(case["request_target"])
    assert (_indep_path(path), _indep_query(query)) == (case["canonical_path"], case["canonical_query"])


def test_query_rules() -> None:
    assert canonical_query("a=1&&b=2&") == "a=1&b=2"  # empty parts are dropped
    assert canonical_query("?a=1") == "%3Fa=1"  # the query is already split off: "?" is data


def _verify(method: str, raw_target: str, body: bytes, headers: Dict[str, str]) -> bool:
    """Independent check (hashlib/hmac and the canonicaliser above, written from the spec text)."""
    canonical = "\n".join(
        [
            "CEXY-HMAC-SHA256-v1",
            method,
            _indep_path(raw_target.partition("?")[0]),
            _indep_query(raw_target.partition("?")[2]),
            headers.get("x-api-timestamp", ""),
            headers.get("x-api-nonce", ""),
            hashlib.sha256(body).hexdigest(),
        ]
    )
    expected = hmac.new(V["secret"].encode(), canonical.encode(), hashlib.sha256).hexdigest()
    ts = int(headers.get("x-api-timestamp", "0"))
    return expected == headers.get("x-api-signature") and abs(ts - time.time() * 1000) < 30_000


@pytest.fixture
def recorder() -> Iterator[tuple[str, List[Dict[str, Any]]]]:
    seen: List[Dict[str, Any]] = []

    class H(BaseHTTPRequestHandler):
        def _handle(self) -> None:
            n = int(self.headers.get("content-length") or 0)
            body = self.rfile.read(n) if n else b""
            hdrs = {k.lower(): v for k, v in self.headers.items()}
            seen.append(
                {
                    "method": self.command,
                    "target": self.path,
                    "headers": hdrs,
                    "body": body,
                    "valid": _verify(self.command, self.path, body, hdrs),
                }
            )
            if self.path.startswith("/api/v1/account/ledger"):
                data: Any = {"items": [], "next_cursor": None, "has_more": False}
            elif self.command == "POST":
                data = {"order": ORDER, "fills": []}
            else:
                data = ORDER
            out = json.dumps({"data": data}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

        do_GET = do_POST = do_DELETE = _handle

        def log_message(self, *a: Any) -> None:
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}", seen
    httpd.shutdown()


def test_recording_server_sync(recorder: tuple[str, List[Dict[str, Any]]]) -> None:
    base, seen = recorder
    c = cexy.Client(api_key=V["key_id"], api_secret=V["secret"], auth="hmac", base_url=base, allow_insecure=True)
    c.account.ledger(asset="Über €", cursor="a b+c/d?e&f=g~h")
    c.trading.place_order("BTC/USDT", "buy", "limit", quantity="1", price="1", client_order_id=ORDER["client_order_id"])
    c.trading.cancel_order(ORDER["id"])
    c.trading.order_by_client_id("sub/1 ?x+y")
    assert [f"{s['method']} {s['target'].split('?')[0]}" for s in seen] == [
        "GET /api/v1/account/ledger",
        "POST /api/v1/trading/orders",
        "DELETE /api/v1/trading/orders/6aa9003697a77bccb95b2282",
        "GET /api/v1/trading/orders/by-client-id/sub%2F1%20%3Fx%2By",
    ]
    assert "%20" in seen[0]["target"] and "+" not in seen[0]["target"]
    for s in seen:
        assert s["valid"], s["target"]
        assert "x-api-secret" not in s["headers"]
        assert s["headers"]["x-api-key"] == V["key_id"]


async def test_recording_server_async(recorder: tuple[str, List[Dict[str, Any]]]) -> None:
    base, seen = recorder
    async with cexy.AsyncClient(
        api_key=V["key_id"], api_secret=V["secret"], auth="hmac", base_url=base, allow_insecure=True
    ) as c:
        await c.account.ledger(asset="a b", cursor="x+y")
        await c.trading.cancel_order(ORDER["id"])
    assert all(s["valid"] for s in seen) and len(seen) == 2


@respx.mock
async def test_async_transport_signs_the_exact_body_and_every_retry() -> None:
    route = respx.post(f"{BASE}/api/v1/trading/orders").mock(
        side_effect=[
            httpx.Response(503, json={"error": {"code": "SERVICE_UNAVAILABLE", "message": "x", "retryable": True}}),
            httpx.Response(201, json={"data": {"order": ORDER, "fills": []}}),
        ]
    )
    async with cexy.AsyncClient(api_key=V["key_id"], api_secret=V["secret"], auth="hmac") as c:

        async def no_sleep(_: float) -> None:
            pass

        c._transport._sleep = no_sleep  # type: ignore[assignment]
        await c.trading.place_order(
            "BTC/USDT", "buy", "limit", quantity="1", price="1", client_order_id=ORDER["client_order_id"]
        )
    assert route.call_count == 2
    for call in route.calls:
        req = call.request
        hdrs = {k.lower(): v for k, v in req.headers.items()}
        target = req.url.raw_path.decode()
        assert req.content.startswith(b"{")
        assert _verify("POST", target, req.content, hdrs), "the signature must cover the bytes sent"
    nonces = [c.request.headers["x-api-nonce"] for c in route.calls]
    assert nonces[0] != nonces[1]


@respx.mock
async def test_async_signature_expired_resends_once() -> None:
    server_ms = int(time.time() * 1000) + 120_000
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        side_effect=[_err(401, "SIGNATURE_EXPIRED", server_time_ms=server_ms), httpx.Response(200, json={"data": []})]
    )
    async with cexy.AsyncClient(api_key=V["key_id"], api_secret=V["secret"], auth="hmac") as c:
        await c.account.balances()
    assert route.call_count == 2
    assert route.calls[0].request.headers["x-api-nonce"] != route.calls[1].request.headers["x-api-nonce"]


@respx.mock
@pytest.mark.parametrize("bad", [None, "soon", float("nan"), float("inf"), True])
def test_signature_expired_without_a_usable_server_clock(bad: Any) -> None:
    details = {} if bad is None else {"server_time_ms": bad}
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        return_value=httpx.Response(
            401,
            content=json.dumps(
                {"error": {"code": "SIGNATURE_EXPIRED", "message": "expired", "retryable": True, "details": details}}
            ),
        )
    )
    c = _hmac_client()
    c._transport.policy.max_retries = 0
    with pytest.raises(CexyApiError) as exc:
        c.account.balances()
    assert exc.value.code == "SIGNATURE_EXPIRED" and "clock" not in exc.value.message
    assert route.call_count == 1 and c._transport.auth.clock_offset_ms == 0  # type: ignore[union-attr]


def test_adjust_clock_refuses_non_finite() -> None:
    a = HmacAuth(V["key_id"], V["secret"])
    assert not a.adjust_clock(float("nan")) and not a.adjust_clock(float("inf")) and a.clock_offset_ms == 0


def test_signing_is_the_default(recorder: tuple[str, List[Dict[str, Any]]]) -> None:
    base, seen = recorder
    c = cexy.Client(api_key=V["key_id"], api_secret=V["secret"], base_url=base, allow_insecure=True)
    c.trading.cancel_order(ORDER["id"])
    assert seen[0]["valid"] and "x-api-secret" not in seen[0]["headers"]


def test_headers_mode_still_selectable(recorder: tuple[str, List[Dict[str, Any]]]) -> None:
    base, seen = recorder
    c = cexy.Client(api_key=V["key_id"], api_secret=V["secret"], auth="headers", base_url=base, allow_insecure=True)
    c.trading.cancel_order(ORDER["id"])
    assert seen[0]["headers"]["x-api-secret"] == V["secret"] and "x-api-signature" not in seen[0]["headers"]


@respx.mock
def test_signature_required_names_the_fix_and_is_not_retried() -> None:
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        return_value=httpx.Response(
            400,
            json={
                "error": {
                    "code": "SIGNATURE_REQUIRED",
                    "message": "This API key must sign its requests; sending the secret is no longer accepted.",
                    "retryable": False,
                }
            },
        )
    )
    c = cexy.Client(api_key=V["key_id"], api_secret=V["secret"], auth="headers")
    with pytest.raises(CexyApiError) as exc:
        c.account.balances()
    assert exc.value.code == "SIGNATURE_REQUIRED" and 'auth="hmac"' in exc.value.message
    assert not exc.value.retryable and route.call_count == 1


# ---- transport rules ---------------------------------------------------------------------------


def _hmac_client() -> cexy.Client:
    c = cexy.Client(api_key=V["key_id"], api_secret=V["secret"], auth="hmac")

    def no_sleep(_: float) -> None:
        pass

    c._transport._sleep = no_sleep  # type: ignore[assignment]
    return c


def _err(status: int, code: str, **details: Any) -> httpx.Response:
    return httpx.Response(
        status, json={"error": {"code": code, "message": code.lower(), "retryable": False, "details": details}}
    )


@respx.mock
def test_every_retry_resigns() -> None:
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        side_effect=[
            httpx.Response(503, json={"error": {"code": "SERVICE_UNAVAILABLE", "message": "x", "retryable": True}}),
            httpx.Response(200, json={"data": []}),
        ]
    )
    _hmac_client().account.balances()
    nonces = [c.request.headers["x-api-nonce"] for c in route.calls]
    assert len(nonces) == 2 and nonces[0] != nonces[1]
    assert all("x-api-secret" not in c.request.headers for c in route.calls)


def _warming() -> httpx.Response:
    return httpx.Response(
        503,
        headers={"Retry-After": "2"},
        json={
            "error": {
                "code": "SERVICE_UNAVAILABLE",
                "message": "x",
                "retryable": True,
                "details": {"reason": "nonce_store_warming"},
            }
        },
    )


@respx.mock
def test_nonce_store_warming_waits_retry_after_and_keeps_the_offset() -> None:
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        side_effect=[_warming(), httpx.Response(200, json={"data": []})]
    )
    c = _hmac_client()
    sleeps: List[float] = []
    c._transport._sleep = sleeps.append  # type: ignore[assignment]
    c.account.balances()
    assert route.call_count == 2 and len(sleeps) == 1 and 2 <= sleeps[0] <= 3
    nonces = [call.request.headers["x-api-nonce"] for call in route.calls]
    assert nonces[0] != nonces[1]
    assert c._transport.auth.clock_offset_ms == 0  # type: ignore[union-attr]


@respx.mock
async def test_nonce_store_warming_async() -> None:
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        side_effect=[_warming(), httpx.Response(200, json={"data": []})]
    )
    sleeps: List[float] = []

    async def record(d: float) -> None:
        sleeps.append(d)

    async with cexy.AsyncClient(api_key=V["key_id"], api_secret=V["secret"], auth="hmac") as c:
        c._transport._sleep = record  # type: ignore[assignment]
        await c.account.balances()
        assert c._transport.auth.clock_offset_ms == 0  # type: ignore[union-attr]
    assert route.call_count == 2 and len(sleeps) == 1 and 2 <= sleeps[0] <= 3


@respx.mock
def test_signature_expired_resends_once() -> None:
    server_ms = int(time.time() * 1000) + 120_000
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        side_effect=[_err(401, "SIGNATURE_EXPIRED", server_time_ms=server_ms), httpx.Response(200, json={"data": []})]
    )
    c = _hmac_client()
    c._transport.policy.max_retries = 0
    c.account.balances()
    assert route.call_count == 2
    assert abs(int(route.calls[1].request.headers["x-api-timestamp"]) - server_ms) < 5_000
    route2 = respx.get(f"{BASE}/api/v1/account/ledger").mock(
        side_effect=[_err(401, "SIGNATURE_EXPIRED", server_time_ms=server_ms)] * 3
    )
    with pytest.raises(CexyApiError) as exc:
        c.account.ledger()
    assert exc.value.code == "SIGNATURE_EXPIRED" and route2.call_count == 2


@respx.mock
def test_signature_expired_beyond_an_hour_is_a_clock_error() -> None:
    far = int(time.time() * 1000) + 2 * 3_600_000
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(
        return_value=_err(401, "SIGNATURE_EXPIRED", server_time_ms=far)
    )
    with pytest.raises(CexyApiError, match="more than 1 hour"):
        _hmac_client().account.balances()
    assert route.call_count == 1


@respx.mock
def test_key_not_signable_names_the_fix_and_never_falls_back() -> None:
    route = respx.get(f"{BASE}/api/v1/account/balances").mock(return_value=_err(401, "KEY_NOT_SIGNABLE"))
    with pytest.raises(CexyApiError) as exc:
        _hmac_client().account.balances()
    assert exc.value.message == "create a new API key; keys issued before request signing can't sign"
    assert route.call_count == 1 and "x-api-secret" not in route.calls[0].request.headers


def test_secret_never_in_repr() -> None:
    a = HmacAuth(V["key_id"], V["secret"])
    assert V["secret"] not in repr(a) + str(a)
    with pytest.raises(TypeError):
        import pickle

        pickle.dumps(a)


# ---- WebSocket auth_key ------------------------------------------------------------------------


async def test_auth_key_vector_and_next_challenge(server: FakeServer) -> None:  # noqa: F811
    w = V["ws"]
    server.welcome = {
        **server.welcome,
        "connection_id": w["welcome"]["connection_id"],
        "challenge": w["welcome"]["challenge"],
    }
    server.ack_auth = False
    signer = HmacAuth(V["key_id"], V["secret"])
    async with make(server, key_signer=signer) as ws:
        task = __import__("asyncio").ensure_future(ws.auth_key())
        await _until(lambda: len(server.ops(-1, "auth_key")) == 1)
        req = server.ops(-1, "auth_key")[0]
        assert (req["key_id"], req["signature"]) == (w["auth_key"]["key_id"], w["auth_key"]["signature"])
        assert "secret" not in req
        await server.push(
            {"type": "authenticated", "user_id": "u_1", "auth": "api_key", "challenge": "next-1", "id": req["id"]}
        )
        await task
        assert ws.auth_kind == "api_key" and ws.authenticated
        task2 = __import__("asyncio").ensure_future(ws.auth_key())
        await _until(lambda: len(server.ops(-1, "auth_key")) == 2)
        req2 = server.ops(-1, "auth_key")[1]
        assert req2["signature"] == signer.sign_websocket_challenge(w["welcome"]["connection_id"], "next-1")[1]
        await server.push(
            {"type": "authenticated", "user_id": "u_1", "auth": "api_key", "challenge": "n2", "id": req2["id"]}
        )
        await task2


async def test_auth_key_refused_is_not_retried_and_reconnect_signs_new_challenge(server: FakeServer) -> None:  # noqa: F811
    server.welcome = lambda n: {**WELCOME_BASE, "challenge": f"c-{n}"}
    server.ack_auth = False
    signer = HmacAuth(V["key_id"], V["secret"])
    async with make(server, key_signer=signer) as ws:
        task = __import__("asyncio").ensure_future(ws.auth_key())
        await _until(lambda: len(server.ops(0, "auth_key")) == 1)
        cid = ws.welcome["connection_id"]
        assert server.ops(0, "auth_key")[0]["signature"] == signer.sign_websocket_challenge(cid, "c-0")[1]
        await server.push(
            {
                "type": "error",
                "code": "UNAUTHENTICATED",
                "message": "bad key",
                "challenge": "c-0b",
                "id": server.ops(0, "auth_key")[0]["id"],
            }
        )
        with pytest.raises(cws.WebSocketError):
            await task
        await server.drop()
        await _until(lambda: len(server.conns) == 2 and ws.connections == 2)
        await ws.ping()
        assert server.ops(1, "auth_key") == []  # a refused key is not tried again automatically
        # A new auth_key on the new connection signs the NEW welcome's challenge.
        t2 = __import__("asyncio").ensure_future(ws.auth_key())
        await _until(lambda: len(server.ops(1, "auth_key")) == 1)
        assert server.ops(1, "auth_key")[0]["signature"] == signer.sign_websocket_challenge(cid, "c-1")[1]
        await server.push(
            {
                "type": "authenticated",
                "user_id": "u_1",
                "auth": "api_key",
                "challenge": "x",
                "id": server.ops(1, "auth_key")[0]["id"],
            }
        )
        await t2


async def test_a_challenge_is_consumed_when_signed(server: FakeServer) -> None:  # noqa: F811
    server.welcome = {**server.welcome, "challenge": "only"}
    async with make(server, key_signer=HmacAuth(V["key_id"], V["secret"]), request_timeout=0.05) as ws:
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.auth_key()  # no reply, so no new challenge
        assert exc.value.code == "TIMEOUT"
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.auth_key()
        assert exc.value.code == "NO_CHALLENGE"
        assert len(server.ops(-1, "auth_key")) == 1


async def test_a_late_reply_still_brings_the_next_challenge(server: FakeServer) -> None:  # noqa: F811
    server.welcome = {**server.welcome, "challenge": "c-0"}
    signer = HmacAuth(V["key_id"], V["secret"])
    async with make(server, key_signer=signer, request_timeout=0.05) as ws:
        with pytest.raises(cws.WebSocketError):
            await ws.auth_key()
        first = server.ops(-1, "auth_key")[0]
        await server.push(
            {"type": "authenticated", "user_id": "u_1", "auth": "api_key", "challenge": "late", "id": first["id"]}
        )
        await ws.ping()
        task = __import__("asyncio").ensure_future(ws.auth_key())
        await _until(lambda: len(server.ops(-1, "auth_key")) == 2)
        cid = ws.welcome["connection_id"]
        assert server.ops(-1, "auth_key")[1]["signature"] == signer.sign_websocket_challenge(cid, "late")[1]
        await server.push(
            {
                "type": "authenticated",
                "user_id": "u_1",
                "auth": "api_key",
                "challenge": "n",
                "id": server.ops(-1, "auth_key")[1]["id"],
            }
        )
        await task


async def test_a_late_refusal_stops_automatic_key_reauth(server: FakeServer) -> None:  # noqa: F811
    server.welcome = lambda n: {**WELCOME_BASE, "challenge": f"c-{n}"}
    async with make(server, key_signer=HmacAuth(V["key_id"], V["secret"]), request_timeout=0.05) as ws:
        with pytest.raises(cws.WebSocketError):
            await ws.auth_key()
        rid = server.ops(0, "auth_key")[0]["id"]
        await server.push(
            {"type": "error", "code": "UNAUTHENTICATED", "message": "bad key", "challenge": "z", "id": rid}
        )
        await ws.ping()
        await server.drop()
        await _until(lambda: len(server.conns) == 2 and ws.connections == 2)
        await ws.ping()
        assert server.ops(1, "auth_key") == []


async def test_auth_key_needs_a_signer(server: FakeServer) -> None:  # noqa: F811
    async with make(server) as ws:
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.auth_key()
        assert exc.value.code == "CONFIG"


WELCOME_BASE = load("ws/welcome.json")


async def _until(cond: Any, timeout: float = 2.0) -> None:
    import asyncio

    end = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > end:
            raise AssertionError("timed out")
        await asyncio.sleep(0.005)
