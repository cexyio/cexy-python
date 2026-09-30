"""WebSocket client against a local fake server replaying conformance/ws frames.

The fake server is a test fixture only: it listens on the loopback interface with an
OS-assigned port.
"""

from __future__ import annotations

import asyncio
import json
import warnings
from typing import Any, Dict, List, Optional

import httpx
import pytest
from websockets.asyncio.server import ServerConnection, serve

import cexy
from cexy import ws as cws
from cexy._generated.models import BalanceResponse
from cexy.ws import OrderBook, WebSocketClient
from tests.conftest import load

WELCOME = load("ws/welcome.json")
UPDATE = load("ws/orderbook_update.json")  # sequence 1042
PONG = load("ws/pong_unsolicited.json")
REVOKED = load("ws/session_revoked.json")
CONCURRENT = load("ws/concurrent_modification.json")


class FakeServer:
    def __init__(self) -> None:
        self.received: List[List[Dict[str, Any]]] = []  # per connection
        self.conns: List[ServerConnection] = []
        self.welcome = dict(WELCOME)
        self.reply_to_pings = True
        self.ack_auth = True  # False: never acknowledge auth (to test the timeout)
        self.refuse_subscribe = False  # True: answer subscribe with UNAUTHENTICATED
        self.port = 0
        self._server: Any = None

    async def handler(self, conn: ServerConnection) -> None:
        self.conns.append(conn)
        msgs: List[Dict[str, Any]] = []
        self.received.append(msgs)
        await conn.send(json.dumps(self.welcome))
        async for raw in conn:
            assert isinstance(raw, str), "client must send text frames"
            msg = json.loads(raw)
            msgs.append(msg)
            # Every request with an id is acknowledged with that id (asyncapi.yaml).
            rid = msg.get("id")
            if msg["op"] == "subscribe" and self.refuse_subscribe:
                err = {"type": "error", "code": "UNAUTHENTICATED", "message": "authentication required", "id": rid}
                await conn.send(json.dumps(err))
            elif msg["op"] == "subscribe" and any("NOPE" in c for c in msg["channels"]):
                await conn.send(
                    json.dumps({"type": "error", "code": "VALIDATION_FAILED", "message": "unknown market", "id": rid})
                )
            elif msg["op"] == "subscribe":
                await conn.send(json.dumps({"type": "subscribed", "channels": msg["channels"], "id": rid}))
            elif msg["op"] == "unsubscribe":
                await conn.send(json.dumps({"type": "unsubscribed", "channels": msg["channels"], "id": rid}))
            elif msg["op"] == "ping" and self.reply_to_pings:
                await conn.send(json.dumps({"type": "pong", "id": rid}))
            elif msg["op"] == "auth" and msg["token"].startswith("bad"):
                # echoes the token back, to prove the client redacts it
                err = {
                    "type": "error",
                    "code": "UNAUTHENTICATED",
                    "message": f"invalid token {msg['token']}",
                    "id": rid,
                }
                await conn.send(json.dumps(err))
            elif msg["op"] == "auth" and self.ack_auth:
                await conn.send(json.dumps({"type": "authenticated", "id": rid, "user_id": "u_1"}))

    async def push(self, frame: Dict[str, Any]) -> None:
        await self.conns[-1].send(json.dumps(frame))

    async def drop(self) -> None:
        await self.conns[-1].close()

    def ops(self, conn: int = -1, op: Optional[str] = None) -> List[Dict[str, Any]]:
        return [m for m in self.received[conn] if op is None or m["op"] == op]

    @property
    def url(self) -> str:
        return "ws://127.0.0.1:" + str(self.port) + "/api/v1/ws"


@pytest.fixture
async def server():  # type: ignore[no-untyped-def]
    fs = FakeServer()
    async with serve(fs.handler, "127.0.0.1", 0) as srv:
        fs.port = next(iter(srv.sockets)).getsockname()[1]
        yield fs


class Snapshots:
    """REST order-book snapshots served through an httpx mock transport."""

    def __init__(self, sequence: int = 1041) -> None:
        self.sequence = sequence
        self.calls = 0
        self.levels = 100

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        bids = [[str(60000 - i), "1"] for i in range(self.levels)]
        asks = [[str(60001 + i), "1"] for i in range(self.levels)]
        data = {
            "symbol": "BTC/USDT",
            "bids": bids,
            "asks": asks,
            "sequence": self.sequence,
            "timestamp": "2026-09-10T11:42:31Z",
        }
        return httpx.Response(200, json={"data": data})

    def client(self) -> cexy.AsyncClient:
        return cexy.AsyncClient(http_client=httpx.AsyncClient(transport=httpx.MockTransport(self.handler)))


def make(server: FakeServer, snaps: Optional[Snapshots] = None, **kw: Any) -> WebSocketClient:
    opts: Dict[str, Any] = dict(allow_insecure=True, request_timeout=0.5, backoff_base=0.01, backoff_max=0.05)
    opts.update(kw)
    return WebSocketClient(server.url, rest=(snaps or Snapshots()).client(), **opts)


async def next_event(ws: WebSocketClient, type_: str, timeout: float = 2.0) -> cws.Event:
    async def find() -> cws.Event:
        async for ev in ws:
            if ev.type == type_:
                return ev
        raise AssertionError

    return await asyncio.wait_for(find(), timeout)


async def until(pred: Any, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not pred():
        if loop.time() > end:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.01)


def test_default_url_and_wss_required() -> None:
    assert cws.DEFAULT_WS_URL == "wss://api.cexy.io/api/v1/ws"
    with pytest.raises(ValueError):
        WebSocketClient("ws://example.test/api/v1/ws")


async def test_welcome(server: FakeServer) -> None:
    async with make(server) as ws:
        assert ws.welcome == WELCOME
        assert ws.welcome["connection_id"] == "a1b2c3d4"
        assert ws.max_subscriptions == 100


async def test_ping_cadence(server: FakeServer) -> None:
    async with make(server, ping_interval=0.05):
        await asyncio.sleep(0.32)
    pings = server.ops(0, "ping")
    assert 4 <= len(pings) <= 7


async def test_default_ping_interval_is_30s() -> None:
    assert WebSocketClient().ping_interval == 30.0
    assert WebSocketClient().liveness_timeout == 75.0


async def test_unsolicited_pong_accepted_and_keeps_alive(server: FakeServer) -> None:
    server.reply_to_pings = False
    async with make(server, ping_interval=0.03, liveness_timeout=0.15) as ws:
        for _ in range(10):
            await server.push(PONG)
            await asyncio.sleep(0.03)
        assert ws.connections == 1  # pongs kept the connection alive
        assert ws._queue is not None and ws._queue.empty()  # pong is not surfaced as an event


async def test_silence_triggers_reconnect(server: FakeServer) -> None:
    server.reply_to_pings = False
    async with make(server, ping_interval=0.03, liveness_timeout=0.1) as ws:
        await next_event(ws, cws.RECONNECTED)
        assert ws.connections >= 2


async def test_orderbook_snapshot_and_sequencing(server: FakeServer) -> None:
    snaps = Snapshots(sequence=1041)
    async with make(server, snaps) as ws:
        book = await ws.order_book("BTC/USDT")
        # subscribe came before the snapshot
        assert server.ops(0, "subscribe")[0]["channels"] == ["orderbook:BTC/USDT"]
        assert book.sequence == 1041 and len(book.bids) == 50 and len(book.asks) == 50  # never deeper than 50
        await server.push({**UPDATE, "sequence": 1041, "data": {**UPDATE["data"], "bids": [["1", "1"]]}})
        await server.push(UPDATE)
        await until(lambda: book.sequence == 1042)
        assert book.bids == [(cws.Decimal("61000.10"), cws.Decimal("0.25"))]  # full replace
        assert book.asks == [(cws.Decimal("61001.00"), cws.Decimal("0.10"))]
        assert not book.stale


async def test_orderbook_gap_marks_stale_then_heals(server: FakeServer) -> None:
    snaps = Snapshots(sequence=1041)
    async with make(server, snaps) as ws:
        book = await ws.order_book("BTC/USDT")
        await server.push(UPDATE)
        await server.push({**UPDATE, "sequence": 1044})
        ev = await next_event(ws, cws.BOOK_STALE)
        assert ev.channel == "orderbook:BTC/USDT" and book.stale
        await server.push({**UPDATE, "sequence": 1045})
        await until(lambda: book.sequence == 1045)
        assert not book.stale
        assert snaps.calls == 1  # a gap does not force a resync


def test_orderbook_buffers_until_snapshot() -> None:
    async def run() -> None:
        book = OrderBook("BTC/USDT")
        for seq in (1040, 1041, 1042, 1043):
            book.apply_update(
                cws.Event(type="orderbook.update", sequence=seq, data={"bids": [[str(seq), "1"]], "asks": []})
            )
        book.apply_snapshot(1041, [["1", "1"]], [])
        assert book.sequence == 1043 and book.bids[0][0] == cws.Decimal("1043") and not book.stale

    asyncio.run(run())


async def test_concurrent_modification_resyncs_all_books(server: FakeServer) -> None:
    snaps = Snapshots(sequence=1041)
    async with make(server, snaps) as ws:
        book = await ws.order_book("BTC/USDT")
        snaps.sequence = 2000
        await server.push(CONCURRENT)
        ev = await next_event(ws, cws.RESYNC)
        assert ev.data == {"reason": "concurrent_modification"}
        await until(lambda: snaps.calls == 2 and book.sequence == 2000)
        assert book.synced


async def test_reconnect_resubscribes_and_resnapshots(server: FakeServer) -> None:
    snaps = Snapshots(sequence=1041)
    async with make(server, snaps) as ws:
        await ws.subscribe("ticker:BTC/USDT", "trades:BTC/USDT")
        book = await ws.order_book("BTC/USDT")
        await server.push(UPDATE)
        await until(lambda: book.sequence == 1042)
        snaps.sequence = 5  # the server restarted: sequences reset
        await server.drop()
        await next_event(ws, cws.RECONNECTED)
        await until(lambda: book.synced and book.sequence == 5)
        resub = server.ops(1, "subscribe")
        assert set(resub[0]["channels"]) == {"ticker:BTC/USDT", "trades:BTC/USDT", "orderbook:BTC/USDT"}
        await server.push({**UPDATE, "sequence": 6})
        await until(lambda: book.sequence == 6)  # not compared against the old connection's 1042
        assert ws.channels == {"ticker:BTC/USDT", "trades:BTC/USDT", "orderbook:BTC/USDT"}


async def test_reconnect_reauths_for_private_channels(server: FakeServer) -> None:
    async with make(server, token="session-token") as ws:
        assert ws.authenticated
        await ws.subscribe("orders")
        await server.drop()
        await next_event(ws, cws.RECONNECTED)
        ops = [m["op"] for m in server.received[1]]
        assert ops.index("auth") < ops.index("subscribe")


async def test_session_revoked_emits_auth_lost(server: FakeServer) -> None:
    lost: List[cws.Event] = []
    async with make(server) as ws:
        ws.on(cws.AUTH_LOST, lost.append)
        await ws.auth("session-token")
        await ws.subscribe("account", "orders")
        await server.push(REVOKED)
        ev = await next_event(ws, cws.AUTH_LOST)
        assert ev.data["reason"] == "logout_all"
        assert not ws.authenticated and lost
        assert server.conns[-1].state.name == "OPEN"  # server does not close; neither do we
        # after a reconnect the revoked token is not reused
        await server.drop()
        await next_event(ws, cws.RECONNECTED)
        assert server.ops(1, "auth") == []


async def test_auth_error_raises(server: FakeServer) -> None:
    async with make(server) as ws:
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.auth("bad")
        assert exc.value.code == "UNAUTHENTICATED"
        assert "bad" not in repr(ws)


async def test_token_redacted_and_not_in_url(server: FakeServer) -> None:
    ws = make(server, token="session-token")
    assert "session-token" not in repr(ws)
    assert "session-token" not in ws.url


async def test_unknown_event_types_ignored(server: FakeServer) -> None:
    async with make(server) as ws:
        await server.push({"type": "something.new", "channel": "ticker", "data": {}})
        await server.push(
            {"type": "ticker.update", "channel": "ticker:BTC/USDT", "data": {"last": "1"}, "new_field": 1}
        )
        ev = await next_event(ws, "ticker.update")
        assert ev.data == {"last": "1"}


async def test_unknown_protocol_version_warns_once(server: FakeServer) -> None:
    server.welcome = {**WELCOME, "protocol_version": 99}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        async with make(server):
            pass
        async with make(server):
            pass
    assert len([w for w in caught if "protocol_version 99" in str(w.message)]) == 1


async def test_subscription_limit_guard(server: FakeServer) -> None:
    async with make(server) as ws:
        channels = [f"ticker:C{i}/USDT" for i in range(105)]
        res = await ws.subscribe(*channels)
        assert len(res.added) == 100 and res.refused == channels[100:]
        assert sum(len(m["channels"]) for m in server.ops(0, "subscribe")) == 100
        again = await ws.subscribe("ticker:X/USDT")
        assert again.added == [] and again.refused == ["ticker:X/USDT"]
        with pytest.raises(ValueError):
            await ws.subscribe("x" * 65)


async def test_request_ids_correlate(server: FakeServer) -> None:
    async with make(server) as ws:
        await ws.subscribe("ticker")
        await ws.subscribe("markets")
        ids = [m["id"] for m in server.ops(0, "subscribe")]
        assert len(set(ids)) == 2


async def test_send_limiter() -> None:
    lim = cws._SendLimiter(3, window=0.2)
    loop = asyncio.get_running_loop()
    start = loop.time()
    for _ in range(4):
        await lim.acquire()
    assert loop.time() - start >= 0.19


def test_client_default_message_limit() -> None:
    assert WebSocketClient()._limiter.limit == 200


async def test_auth_waits_for_authenticated_ack(server: FakeServer) -> None:
    async with make(server) as ws:
        await ws.auth("session-token")
        assert ws.authenticated and ws.user_id == "u_1"
        auth = server.ops(0, "auth")[0]
        assert auth["id"] and auth["token"] == "session-token"


async def test_auth_without_ack_times_out(server: FakeServer) -> None:
    server.ack_auth = False
    async with make(server) as ws:
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.auth("session-token")
        assert exc.value.code == "TIMEOUT" and not ws.authenticated


async def test_unsubscribe_is_acknowledged(server: FakeServer) -> None:
    async with make(server) as ws:
        await ws.subscribe("ticker", "markets")
        await ws.unsubscribe("ticker")
        assert ws.channels == {"markets"}
        unsub = server.ops(0, "unsubscribe")[0]
        assert unsub["channels"] == ["ticker"] and unsub["id"]


async def test_ping_correlated_by_id(server: FakeServer) -> None:
    async with make(server) as ws:
        rtt = await ws.ping()
        assert rtt >= 0
        assert server.ops(0, "ping")[0]["id"]
        server.reply_to_pings = False
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.ping()
        assert exc.value.code == "TIMEOUT"


async def test_subscribe_error_ack_raises(server: FakeServer) -> None:
    async with make(server) as ws:
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.subscribe("orderbook:NOPE/USDT")
        assert exc.value.code == "VALIDATION_FAILED"
        assert ws.channels == set()


def test_orderbook_update_is_full_replacement() -> None:
    async def run() -> None:
        book = OrderBook("BTC/USDT")
        book.apply_snapshot(1041, [["1", "1"], ["0.9", "2"]], [["2", "1"]])
        assert UPDATE["data"]["full"] is True
        book.apply_update(cws.Event(type="orderbook.update", sequence=1042, data=UPDATE["data"]))
        # both sides replaced outright: no level from the snapshot survives
        assert book.bids == [(cws.Decimal("61000.10"), cws.Decimal("0.25"))]
        assert book.asks == [(cws.Decimal("61001.00"), cws.Decimal("0.10"))]

    asyncio.run(run())


@pytest.mark.parametrize("url", ["ws://localhost/api/v1/ws", "ws://127.0.0.1/api/v1/ws", "ws://[::1]/api/v1/ws"])
def test_ws_insecure_needs_opt_in_and_loopback(url: str) -> None:
    with pytest.raises(ValueError):
        WebSocketClient(url)
    assert WebSocketClient(url, allow_insecure=True).url == url


@pytest.mark.parametrize("url", ["ws://api.cexy.io/api/v1/ws", "ws://example.test/ws", "http://localhost/ws", "wss://"])
def test_ws_url_refused(url: str) -> None:
    with pytest.raises(ValueError):
        WebSocketClient(url, allow_insecure=True)


async def test_ws_error_message_redacts_token(server: FakeServer) -> None:
    async with make(server) as ws:
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.auth("bad-session-token-123")
        assert exc.value.code == "UNAUTHENTICATED"
        assert "bad-session-token-123" not in str(exc.value) and exc.value.message == "invalid token ***"


async def test_failed_reauth_then_reconnect_restores_private_on_next_auth(server: FakeServer) -> None:
    changes: List[cws.Event] = []
    resyncs: List[cws.Event] = []
    async with make(server) as ws:
        ws.on(cws.AUTH_CHANGED, changes.append)
        ws.on(cws.RESYNC, resyncs.append)
        await ws.auth("session-token")
        await ws.subscribe("orders", "ticker:BTC/USDT")
        with pytest.raises(cws.WebSocketError):
            await ws.auth("bad-expired-token")
        await ws.ping()
        assert ws.channels == {"ticker:BTC/USDT"} and not ws.has_token
        assert changes[0].data == {
            "reason": "auth_failed",
            "previous_user_id": "u_1",
            "user_id": None,
            "code": "UNAUTHENTICATED",
            "dropped": ["orders"],
        }
        # while signed out, a private subscribe is not re-sent: it is already pending
        assert (await ws.subscribe("orders")).added == []
        await server.drop()
        await next_event(ws, cws.RECONNECTED)
        assert server.ops(1, "auth") == []
        assert [m["channels"] for m in server.ops(1, "subscribe")] == [["ticker:BTC/USDT"]]
        await ws.auth("session-token-2")
        await ws.ping()
        await ws.ping()
        assert [m["channels"] for m in server.ops(1, "subscribe")][1] == ["orders"]
        assert [e.data for e in resyncs if e.data.get("reason") == "reauth"] == [{"reason": "reauth"}]
        assert ws.channels == {"orders", "ticker:BTC/USDT"}


async def test_reconnect_with_token_keeps_private_without_reauth_resync(server: FakeServer) -> None:
    resyncs: List[cws.Event] = []
    async with make(server) as ws:
        ws.on(cws.RESYNC, resyncs.append)
        await ws.auth("session-token")
        await ws.subscribe("orders", "ticker:BTC/USDT")
        await server.drop()
        await next_event(ws, cws.RECONNECTED)
        ops = [m["op"] for m in server.received[1] if m["op"] != "ping"]
        assert ops.index("auth") < ops.index("subscribe")
        assert sorted(c for m in server.ops(1, "subscribe") for c in m["channels"]) == ["orders", "ticker:BTC/USDT"]
        assert ws.channels == {"orders", "ticker:BTC/USDT"}
        assert not [e for e in resyncs if (e.data or {}).get("reason") == "reauth"]


async def test_session_revoked_current_must_be_true(server: FakeServer) -> None:
    async with make(server) as ws:
        await ws.auth("session-token")
        await ws.subscribe("account", "orders")
        for current in (False, "true", None):
            await server.push({**REVOKED, "data": {**REVOKED["data"], "current": current}})
        await ws.ping()
        await ws.ping()
        assert ws.channels == {"account", "orders"} and ws.has_token and ws.authenticated


async def test_refused_private_resubscribe_goes_back_to_pending(server: FakeServer) -> None:
    async with make(server) as ws:
        await ws.auth("session-token")
        await ws.subscribe("orders")
        await server.push(REVOKED)  # current: true
        await next_event(ws, cws.AUTH_CHANGED)
        assert ws.channels == set()
        server.refuse_subscribe = True
        await ws.auth("session-token-2")
        await ws.ping()
        await ws.ping()
        assert server.ops(-1, "subscribe")[-1]["channels"] == ["orders"]
        assert ws.channels == set()
        # still pending: the next successful auth tries again
        server.refuse_subscribe = False
        await ws.auth("session-token-3")
        await ws.ping()
        await ws.ping()
        assert len(server.ops(-1, "subscribe")) == 3 and ws.channels == {"orders"}


async def test_live_balances_unsequenced_events_apply_and_warn_once(
    server: FakeServer, caplog: pytest.LogCaptureFixture
) -> None:
    async def snapshot() -> List[Any]:
        return [
            BalanceResponse.model_validate(
                {
                    "asset": "USDT",
                    "available": "100",
                    "locked": "0",
                    "pending": "0",
                    "total": "100",
                    "held_incoming": [],
                    "sequence": 40,
                }
            )
        ]

    async with make(server) as ws:
        await ws.auth("session-token")
        lb = await ws.live_balances(snapshot=snapshot, account_id="u_1")
        for _ in range(50):
            if not lb.stale:
                break
            await asyncio.sleep(0.01)
        assert not lb.stale

        def upd(**d: Any) -> Dict[str, Any]:
            return {
                "type": "balance.updated",
                "channel": "balances",
                "data": {"available": "0", "locked": "0", "pending": "0", **d},
            }

        with caplog.at_level("WARNING", logger="cexy.ws"):
            await server.push(upd(asset="USDT", total="90"))
            await server.push(upd(asset="USDT", total="80"))
            await ws.ping()
        row = lb.get("USDT")
        assert row.total == cws.Decimal("80") and row.sequence == 40
        assert sum("without data.sequence" in r.message for r in caplog.records) == 1
        await server.push(upd(asset="USDT", total="70", sequence=41))
        await server.push(upd(asset="USDT", total="60", sequence=41))
        await ws.ping()
        row = lb.get("USDT")
        assert row.total == cws.Decimal("70") and row.sequence == 41
        lb.close()


async def test_live_balances_default_owner_mismatch_never_merges(server: FakeServer) -> None:
    calls: List[str] = []

    class Account:
        async def id(self) -> str:
            calls.append("id")
            return "someone_else"

        async def balances(self) -> List[Any]:
            calls.append("balances")
            return []

    class Rest:
        account = Account()

        async def aclose(self) -> None:
            pass

    async with make(server) as ws:
        ws._rest = Rest()  # type: ignore[assignment]
        await ws.auth("session-token")
        lb = await ws.live_balances()
        for _ in range(100):
            if lb.last_error is not None:
                break
            await asyncio.sleep(0.01)
        assert getattr(lb.last_error, "code", None) == "ACCOUNT_MISMATCH"
        assert calls == ["id"]
        assert lb.all() == [] and lb.stale
        lb.close()


async def test_live_balances_custom_snapshot_needs_an_owner(server: FakeServer) -> None:
    class Account:
        async def id(self) -> str:
            raise AssertionError("must not be used for a custom snapshot source")

    class Rest:
        account = Account()

        async def aclose(self) -> None:
            pass

    async def snapshot() -> List[Any]:
        return []

    async with make(server) as ws:
        ws._rest = Rest()  # type: ignore[assignment]
        await ws.auth("session-token")
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.live_balances(snapshot=snapshot)
        assert exc.value.code == "CONFIG"


async def test_live_balances_drops_events_while_unverified(server: FakeServer) -> None:
    owner = {"id": "someone_else"}

    async def owner_id() -> str:
        return owner["id"]

    async def snapshot() -> List[Any]:
        row = {"asset": "USDT", "available": "5", "locked": "0", "pending": "0", "total": "5", "sequence": 10}
        return [BalanceResponse.model_validate(row)]

    async with make(server) as ws:
        await ws.auth("session-token")
        lb = await ws.live_balances(snapshot=snapshot, owner_id=owner_id, min_snapshot_interval=0)
        for _ in range(100):
            if lb.last_error is not None:
                break
            await asyncio.sleep(0.01)
        assert getattr(lb.last_error, "code", None) == "ACCOUNT_MISMATCH"
        for i in range(500):
            data = {"asset": "USDT", "available": "9", "locked": "0", "pending": "0", "total": "9", "sequence": 50 + i}
            await server.push({"type": "balance.updated", "channel": "balances", "data": data})
        await ws.ping()
        assert len(lb._buffer) == 0
        owner["id"] = "u_1"
        await server.push({"type": "balances.resync", "channel": "balances", "data": {}})
        for _ in range(100):
            if not lb.stale:
                break
            await asyncio.sleep(0.01)
        row = lb.get("USDT")
        assert row is not None and row.total == cws.Decimal("5") and row.sequence == 10
        lb.close()


async def test_signed_out_auth_lost_payload_and_unknown_reason(server: FakeServer) -> None:
    lost: List[cws.Event] = []
    changes: List[cws.Event] = []
    async with make(server) as ws:
        ws.on(cws.AUTH_LOST, lost.append)
        ws.on(cws.AUTH_CHANGED, changes.append)
        await ws.auth("session-token")
        await server.push({"type": "signed_out", "reason": "revoked"})
        await ws.ping()
        await ws.ping()
        assert [(e.channel, e.data) for e in lost] == [
            ("account", {"session_id": None, "reason": "signed_out", "current": True})
        ]
        await ws.auth("session-token-2")
        await server.push({"type": "signed_out"})
        await ws.ping()
        await ws.ping()
        assert changes[-1].data["reason"] == "signed_out" and changes[-1].data["code"] == "unknown"


async def test_live_balances_owner_lookup_events_dropped_on_mismatch(server: FakeServer) -> None:
    answer: asyncio.Future[str] = asyncio.get_running_loop().create_future()

    async def owner_id() -> str:
        return await answer

    async def snapshot() -> List[Any]:
        return []

    async with make(server) as ws:
        await ws.auth("session-token")
        lb = await ws.live_balances(snapshot=snapshot, owner_id=owner_id)
        for i in range(5):
            data = {"asset": "USDT", "available": "1", "locked": "0", "pending": "0", "total": "1", "sequence": i + 1}
            await server.push({"type": "balance.updated", "channel": "balances", "data": data})
        await ws.ping()
        await ws.ping()
        assert len(lb._buffer) == 5  # held while the owner lookup is in flight
        answer.set_result("someone_else")
        for _ in range(100):
            if lb.last_error is not None:
                break
            await asyncio.sleep(0.01)
        assert getattr(lb.last_error, "code", None) == "ACCOUNT_MISMATCH"
        assert len(lb._buffer) == 0
        lb.close()


async def test_live_balances_retry_backoff_is_capped_at_30s(server: FakeServer) -> None:
    from tests.test_ws_live_balances import FakeClock

    clock = FakeClock()
    calls: List[float] = []

    async def snapshot() -> List[Any]:
        calls.append(clock.now())
        raise RuntimeError("REST down")

    async with make(server, clock=clock) as ws:
        await ws.auth("session-token")
        lb = await ws.live_balances(snapshot=snapshot, account_id="u_1", retry=8)
        await until(lambda: len(calls) == 1)
        while len(calls) < 6:
            n = len(calls)
            for _ in range(40):  # 1 s steps: each retry is seen at its due second
                clock.advance(1)
                await ws.ping()
                if len(calls) > n:
                    break
        gaps = [b - a for a, b in zip(calls, calls[1:])]
        assert gaps[:3] == [8, 16, 30] and all(g == 30 for g in gaps[2:]), gaps
        assert isinstance(lb.last_error, RuntimeError)
        lb.close()


async def test_live_balances_close_stops_and_unsubscribes(server: FakeServer) -> None:
    async def snapshot() -> List[Any]:
        return []

    async with make(server) as ws:
        await ws.auth("session-token")
        lb = await ws.live_balances(snapshot=snapshot, account_id="u_1")
        assert "balances" in ws.channels
        lb.close()
        await ws.ping()
        await ws.ping()
        assert "balances" not in ws.channels
        assert server.ops(-1, "unsubscribe")[-1]["channels"] == ["balances"]
        lb.close()  # idempotent


async def test_live_balances_refetches_after_reconnect(server: FakeServer) -> None:
    calls = {"n": 0}

    async def snapshot() -> List[Any]:
        calls["n"] += 1
        return []

    async with make(server, token="session-token") as ws:
        lb = await ws.live_balances(snapshot=snapshot, account_id="u_1", min_snapshot_interval=0)
        await until(lambda: calls["n"] == 1)
        await server.drop()
        await next_event(ws, cws.RECONNECTED)
        await until(lambda: calls["n"] == 2)
        assert not lb.stale
        lb.close()
