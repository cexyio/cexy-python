"""Conformance: conformance/ws/futures.json (futures WebSocket channels), run against a local
scripted server (loopback, OS-assigned port): frames, channel names and scenarios."""

from __future__ import annotations

import asyncio
import collections
import json
from typing import Any, Dict, List, Tuple

import pytest
from websockets.asyncio.server import ServerConnection, serve

from cexy import ws as cws
from cexy.ws import WebSocketClient
from tests.conftest import load

SPEC = load("ws/futures.json")
WELCOME = {**load("ws/welcome.json"), "challenge": "challenge-1"}
FRAMES = SPEC["frames"]
SCENARIOS = SPEC["scenarios"]
ACK_OP = {"subscribed": "subscribe", "unsubscribed": "unsubscribe", "authenticated": "auth_key"}


class Signer:
    def sign_websocket_challenge(self, connection_id: str, challenge: str) -> Tuple[str, str]:
        return "ak_test_key", "00" * 32


class FuturesServer:
    """Answers every request; ``script[(op, n)]`` replaces the reply to the n-th ``op`` request
    (0-based) with the given frames (the request's id is added)."""

    def __init__(self) -> None:
        self.received: List[Dict[str, Any]] = []
        self.script: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        self.counts: Dict[str, int] = collections.defaultdict(int)
        # channel -> error code: refused like the server does (an error frame each, in order,
        # then one `subscribed` ack for the rest, or no ack when nothing was accepted)
        self.refuse: Dict[str, str] = {}
        self.conn: Any = None
        self.port = 0

    async def handler(self, conn: ServerConnection) -> None:
        self.conn = conn
        await conn.send(json.dumps(WELCOME))
        async for raw in conn:
            msg = json.loads(raw)
            self.received.append(msg)
            op, rid = msg["op"], msg.get("id")
            n = self.counts[op]
            self.counts[op] += 1
            scripted = self.script.get((op, n))
            if scripted is not None:
                for frame in scripted:
                    await conn.send(json.dumps({**frame, "id": rid}))
            elif op == "subscribe":
                accepted = []
                for ch in msg["channels"]:
                    if ch in self.refuse:
                        err = {"type": "error", "code": self.refuse[ch], "message": f"refused {ch}", "id": rid}
                        await conn.send(json.dumps(err))
                    else:
                        accepted.append(ch)
                if accepted:
                    await conn.send(json.dumps({"type": "subscribed", "channels": accepted, "id": rid}))
            elif op == "unsubscribe":
                await conn.send(json.dumps({"type": "unsubscribed", "channels": msg["channels"], "id": rid}))
            elif op == "ping":
                await conn.send(json.dumps({"type": "pong", "id": rid}))
            elif op == "auth_key":
                reply = {"type": "authenticated", "user_id": "u1", "auth": "api_key", "challenge": "c2", "id": rid}
                await conn.send(json.dumps(reply))

    async def push(self, frame: Dict[str, Any]) -> None:
        await self.conn.send(json.dumps(frame))

    def requests(self) -> List[Dict[str, Any]]:
        return [m for m in self.received if m["op"] != "ping"]

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/api/v1/ws"


@pytest.fixture
async def server():  # type: ignore[no-untyped-def]
    fs = FuturesServer()
    async with serve(fs.handler, "127.0.0.1", 0) as srv:
        fs.port = next(iter(srv.sockets)).getsockname()[1]
        yield fs


def make(server: FuturesServer) -> WebSocketClient:
    return WebSocketClient(server.url, allow_insecure=True, request_timeout=0.5, key_signer=Signer())


async def until(cond: Any, label: str) -> None:
    for _ in range(2000):
        if cond():
            return
        await asyncio.sleep(0.001)
    raise AssertionError(f"timed out waiting for {label}")


async def drain(ws: WebSocketClient, settle: float = 0.05) -> List[cws.Event]:
    await asyncio.sleep(settle)
    out: List[cws.Event] = []
    assert ws._queue is not None
    while not ws._queue.empty():
        out.append(ws._queue.get_nowait())
    return out


# -- frames ------------------------------------------------------------------------


def check_frame(expect: Dict[str, Any], ev: cws.Event) -> None:
    assert ev.type == expect["event_type"] and ev.channel == expect["channel"]
    data = ev.data
    if "sequence" in expect:
        assert ev.sequence == expect["sequence"]
    if "best_bid" in expect:  # levels are {price, size} objects
        assert [data["bids"][0]["price"], data["bids"][0]["size"]] == expect["best_bid"]
        assert [data["asks"][0]["price"], data["asks"][0]["size"]] == expect["best_ask"]
        assert data["full"] is True
    if "trade_count" in expect:
        assert len(data["trades"]) == expect["trade_count"]
    if "open_time" in expect:
        assert data["candle"]["open_time"] == expect["open_time"]
    if "state" in expect:
        assert data["state"] == expect["state"]
    if "position_count" in expect:
        assert len(data["positions"]["positions"]) == expect["position_count"]
    if "stale" in expect:
        assert data["stale"] is expect["stale"]
    if "order_count" in expect:
        assert len(data["orders"]) == expect["order_count"]


@pytest.mark.parametrize("case", FRAMES, ids=[f["id"] for f in FRAMES])
async def test_frames_are_delivered_as_events(server: FuturesServer, case: Dict[str, Any]) -> None:
    async with make(server) as ws:
        await server.push(case["frame"])
        ev = await asyncio.wait_for(ws.__aiter__().__anext__(), 2)
        check_frame(case["expect"], ev)
        assert ev.type in cws.KNOWN_EVENT_TYPES and ev.raw == case["frame"]
        # Public futures channels are not gap-tracked.
        assert case["frame"]["channel"] == cws.FUTURES_ACCOUNT or case["frame"]["channel"] not in ws._seq


def test_every_futures_event_type_is_known() -> None:
    for t in [f["frame"]["type"] for f in FRAMES] + ["futures.resync"]:
        assert t in cws.KNOWN_EVENT_TYPES
    assert cws.FUTURES_ACCOUNT in cws.PRIVATE_CHANNELS


# -- channel names -----------------------------------------------------------------


@pytest.mark.parametrize("kind,args,name", SPEC["channel_names"]["valid"])
def test_valid_channel_names(kind: str, args: List[str], name: str) -> None:
    assert cws.futures_channel(kind, *args) == name
    helper = getattr(cws, f"futures_{kind}")
    assert helper(*args) == name
    cws._check_futures_channel(name)


@pytest.mark.parametrize("kind,args", SPEC["channel_names"]["invalid"])
async def test_invalid_channel_names_fail_locally(server: FuturesServer, kind: str, args: List[str]) -> None:
    with pytest.raises(cws.ChannelNameError) as exc:
        cws.futures_channel(kind, *args)
    assert exc.value.code == "CONFIG" and isinstance(exc.value, ValueError)
    raw = f"futures.{kind}:" + ":".join(args)
    async with make(server) as ws:
        with pytest.raises(cws.ChannelNameError):
            await ws.subscribe("futures.mids", raw)
        await asyncio.sleep(0.02)
    assert server.requests() == []  # nothing sent, not even the valid channel


# -- scenarios ---------------------------------------------------------------------


def plan(steps: List[Dict[str, Any]]) -> Dict[Tuple[str, int], List[Dict[str, Any]]]:
    """Map each scripted server reply to the request it answers: the latest client request of
    that op still unanswered, else the next request of that op (one the client sends by itself)."""
    script: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    counts: Dict[str, int] = collections.defaultdict(int)
    open_req: Dict[str, int] = {}
    for step in steps:
        if "client" in step:
            op = step["client"]
            open_req[op] = counts[op]
            counts[op] += 1
        elif "server" in step and step["server"]["type"] in ACK_OP:
            op = ACK_OP[step["server"]["type"]]
            script[(op, open_req.pop(op))] = [step["server"]]
        elif "server_reply_to" in step:
            op = step["server_reply_to"]
            if op in open_req:
                idx = open_req.pop(op)
            else:
                idx = counts[op]
                counts[op] += 1
            script[(op, idx)] = [step["frame"]]
    return script


def norm(m: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in m.items() if k != "id"}


@pytest.mark.parametrize("case", SCENARIOS, ids=[c["id"] for c in SCENARIOS])
async def test_scenarios(server: FuturesServer, case: Dict[str, Any]) -> None:
    server.script = plan(case["steps"])
    results: List[Any] = []
    async with make(server) as ws:
        sent_before = 0
        for step in case["steps"]:
            if step.get("client") == "auth_key":
                await ws.auth_key()
                sent_before = len(server.requests())
            elif step.get("client") == "subscribe":
                try:
                    results.append(await ws.subscribe(*step["channels"]))
                except cws.SubscribeRefusedError as exc:  # every channel sent was refused
                    results.append(exc.result)
                sent_before = len(server.requests())
            elif "server" in step and step["server"]["type"] not in ACK_OP:
                await server.push(step["server"])
        exp = case["expect"]
        if "client_sends_after" in exp:
            want = exp["client_sends_after"]
            await until(lambda: len(server.requests()) >= sent_before + len(want), "client requests")
        if "held_channels_after" in exp:
            await until(lambda: not ws.channels and not ws.pending_channels, "channel dropped")
        events = await drain(ws, 0.1)
        if "client_sends_after" in exp:
            assert [norm(m) for m in server.requests()[sent_before:]] == exp["client_sends_after"]
        if "events" in exp:
            assert [e.type for e in events if e.type in cws.KNOWN_EVENT_TYPES] == exp["events"]
        if "resync_channels" in exp:
            resyncs = [e for e in events if e.type == cws.RESYNC]
            assert [e.channel for e in resyncs] == exp["resync_channels"]
            assert all(e.data["reason"] == "futures_resync" for e in resyncs)
        if "errors" in exp:
            refused = [e for e in events if e.type == cws.SUBSCRIBE_REFUSED]
            assert [e.data["code"] for e in refused] == exp["errors"]
        if "held_channels_after" in exp:
            assert sorted(ws.channels | ws.pending_channels) == exp["held_channels_after"]
        if "held_pending_private" in exp:
            assert sorted(ws.pending_channels) == exp["held_pending_private"]
            assert results[-1].held == exp["held_pending_private"] and results[-1].added == []
        if case["id"] == "rate_limited_subscribe_not_retried":
            assert results[-1].refused == ["futures.trades:BTC"]
            assert results[-1].errors["futures.trades:BTC"].code == "RATE_LIMITED"


# -- beyond the fixture --------------------------------------------------------------


async def test_held_account_channel_subscribes_after_auth(server: FuturesServer) -> None:
    async with make(server) as ws:
        res = await ws.subscribe("futures.account")
        assert res.held == ["futures.account"] and server.requests() == []
        await ws.auth_key()
        await until(lambda: "futures.account" in ws.channels and server.requests()[-1]["op"] == "subscribe", "sub")
        assert norm(server.requests()[-1]) == {"op": "subscribe", "channels": ["futures.account"]}


async def test_partly_refused_multi_channel_subscribe(server: FuturesServer) -> None:
    # One frame; the error frames for the refused channels come before the single ack.
    server.refuse = {"futures.trades:btc": "NOT_FOUND"}
    async with make(server) as ws:
        res = await ws.subscribe("ticker:BTC/USDT", "futures.mids", "futures.orderbook:BTC", "futures.trades:btc")
        assert res.added == ["ticker:BTC/USDT", "futures.mids", "futures.orderbook:BTC"]
        assert res.refused == ["futures.trades:btc"] and res.errors["futures.trades:btc"].code == "NOT_FOUND"
        assert ws.channels == {"ticker:BTC/USDT", "futures.mids", "futures.orderbook:BTC"}
        refused = [e for e in await drain(ws) if e.type == cws.SUBSCRIBE_REFUSED]
        assert [(e.channel, e.data["code"]) for e in refused] == [("futures.trades:btc", "NOT_FOUND")]
    subs = [m["channels"] for m in server.requests() if m["op"] == "subscribe"]
    assert subs == [["ticker:BTC/USDT", "futures.mids", "futures.orderbook:BTC", "futures.trades:btc"]]


async def test_public_resync_on_mids(server: FuturesServer) -> None:
    async with make(server) as ws:
        await ws.subscribe(cws.futures_mids())
        n = len(server.requests())
        frame = {"type": "futures.resync", "channel": "futures.mids", "sequence": 3, "data": {}}
        await server.push(frame)
        events = await drain(ws, 0.1)
        assert [e.type for e in events] == ["futures.resync", cws.RESYNC]
        assert len(server.requests()) == n


def test_ping_interval_at_most_60s() -> None:
    assert SPEC["ping_interval_max_seconds"] == 60
    assert WebSocketClient().ping_interval <= SPEC["ping_interval_max_seconds"]
    WebSocketClient(ping_interval=60)
    for bad in (61, 0, -1):
        with pytest.raises(ValueError):
            WebSocketClient(ping_interval=bad)
