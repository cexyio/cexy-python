"""Conformance: conformance/ws/private_signout.json (vendored, see tests/fixtures/SYNC.txt),
run step by step against a scripted server that answers only pings; the script sends every
other frame."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

import pytest
from websockets.asyncio.server import ServerConnection, serve

from cexy import ws as cws
from cexy.ws import WebSocketClient
from tests.conftest import load

SPEC = load("ws/private_signout.json")
SERVER = load("ws/server_signout.json")
WELCOME = load("ws/welcome.json")


class ScriptedServer:
    def __init__(self) -> None:
        self.received: List[Dict[str, Any]] = []
        self.conn: Any = None
        self.port = 0

    async def handler(self, conn: ServerConnection) -> None:
        self.conn = conn
        await conn.send(json.dumps(WELCOME))
        async for raw in conn:
            msg = json.loads(raw)
            self.received.append(msg)
            if msg["op"] == "ping":
                await conn.send(json.dumps({"type": "pong", "id": msg.get("id")}))

    def requests(self) -> List[Dict[str, Any]]:
        return [m for m in self.received if m["op"] != "ping"]


async def until(cond: Any, label: str) -> None:
    for _ in range(2000):
        if cond():
            return
        await asyncio.sleep(0.001)
    raise AssertionError(f"timed out waiting for {label}")


def _norm_sent(m: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"op": m["op"]}
    if "token" in m:
        out["token"] = m["token"]
    if "channels" in m:
        out["channels"] = sorted(m["channels"])
    return out


@pytest.mark.parametrize(
    "case", SPEC["cases"] + SERVER["cases"], ids=[c["id"] for c in SPEC["cases"] + SERVER["cases"]]
)
async def test_private_signout(case: Dict[str, Any]) -> None:
    srv = ScriptedServer()
    async with serve(srv.handler, "127.0.0.1", 0) as server:
        srv.port = next(iter(server.sockets)).getsockname()[1]
        ws = WebSocketClient(f"ws://127.0.0.1:{srv.port}/api/v1/ws", allow_insecure=True, reconnect=False)
        events: List[Dict[str, Any]] = []
        ws.on(
            cws.AUTH_CHANGED,
            lambda e: events.append({"type": "auth_changed", **e.data, "dropped": sorted(e.data["dropped"])}),
        )
        ws.on(cws.RESYNC, lambda e: events.append({"type": "resync", **(e.data or {})}))
        ws.on(cws.AUTH_LOST, lambda e: events.append({"type": "auth_lost"}))
        await ws.connect()
        calls: List[asyncio.Task[Any]] = []
        answered: set[str] = set()
        sent_mark = 0

        async def settle() -> None:
            # Two ping round trips: the client has processed every earlier frame, and the server
            # has recorded everything the client sent in reaction.
            await ws.ping()
            await ws.ping()

        try:
            for step in case["steps"]:
                if "client" in step:
                    before = len(srv.requests())
                    if step["client"] == "auth":
                        calls.append(asyncio.ensure_future(ws.auth(step["token"])))
                    else:
                        calls.append(asyncio.ensure_future(ws.subscribe(*step["channels"])))
                    await until(lambda n=before: len(srv.requests()) > n, step["client"])
                elif "server" in step:
                    frame = dict(step["server"])
                    if "reply_to" in step:
                        req = next(
                            m
                            for m in reversed(srv.requests())
                            if m["op"] == step["reply_to"] and m["id"] not in answered
                        )
                        answered.add(req["id"])
                        frame["id"] = req["id"]
                    await srv.conn.send(json.dumps(frame))
                    await settle()
                elif "expect_sent" in step:
                    await settle()
                    sent = [_norm_sent(m) for m in srv.requests()[sent_mark:]]
                    sent_mark = len(srv.requests())
                    assert sent == [_norm_sent(e) for e in step["expect_sent"]]
                elif "expect_events" in step:
                    await settle()
                    got, events[:] = list(events), []
                    assert len(got) == len(step["expect_events"]), got
                    for want in step["expect_events"]:
                        want = {**want, **({"dropped": sorted(want["dropped"])} if "dropped" in want else {})}
                        assert any(all(g.get(k) == v for k, v in want.items()) for g in got), (want, got)
                elif "expect_held" in step:
                    await settle()
                    assert sorted(ws.channels) == sorted(step["expect_held"])
                elif "expect_token" in step:
                    await settle()
                    assert ws.has_token is step["expect_token"]
                else:
                    raise AssertionError(f"unknown step {step}")
        finally:
            for t in calls:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*calls, return_exceptions=True)
            await ws.close()
