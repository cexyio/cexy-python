"""Conformance: conformance/ws/live_balances.json and private_sequence_gap.json (vendored, see
tests/fixtures/SYNC.txt), run step by step against a scripted server (only pings are answered)
with an injected test clock."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
from websockets.asyncio.server import ServerConnection, serve

from cexy import ws as cws
from cexy._generated import models as m
from cexy.ws import WebSocketClient
from tests.conftest import load

LIVE = load("ws/live_balances.json")
GAPS = load("ws/private_sequence_gap.json")
WELCOME = load("ws/welcome.json")


class FakeClock(cws.Clock):
    """Timers run only when ``advance()`` passes their due time."""

    class _Handle:
        def __init__(self, clock: FakeClock, key: int) -> None:
            self._clock, self._key = clock, key

        def cancel(self) -> None:
            self._clock._timers.pop(self._key, None)

    def __init__(self) -> None:
        self._now = 0.0
        self._seq = 0
        self._timers: Dict[int, Tuple[float, Callable[[], None]]] = {}

    def now(self) -> float:
        return self._now

    def call_later(self, delay: float, fn: Callable[[], None]) -> Any:
        self._seq += 1
        self._timers[self._seq] = (self._now + max(0.0, delay), fn)
        return FakeClock._Handle(self, self._seq)

    def advance(self, seconds: float) -> None:
        end = self._now + seconds
        while True:
            due = [(t, k) for k, (t, _) in self._timers.items() if t <= end]
            if not due:
                break
            t, k = min(due)
            _, fn = self._timers.pop(k)
            self._now = t
            fn()
        self._now = end


class ScriptedServer:
    def __init__(self) -> None:
        self.received: List[Dict[str, Any]] = []
        self.conn: Any = None

    async def handler(self, conn: ServerConnection) -> None:
        self.conn = conn
        await conn.send(json.dumps(WELCOME))
        async for raw in conn:
            msg = json.loads(raw)
            self.received.append(msg)
            if msg["op"] == "ping":
                await conn.send(json.dumps({"type": "pong", "id": msg.get("id")}))

    def requests(self) -> List[Dict[str, Any]]:
        return [x for x in self.received if x["op"] != "ping"]


class Scripted:
    """A source whose calls the script answers one by one."""

    def __init__(self) -> None:
        self.calls = 0
        self.pending: List[asyncio.Future[Any]] = []

    async def __call__(self) -> Any:
        self.calls += 1
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.pending.append(fut)
        return await fut

    def answer(self, value: Any) -> None:
        self.pending.pop(0).set_result(value)


async def until(cond: Callable[[], bool], label: str) -> None:
    for _ in range(2000):
        if cond():
            return
        await asyncio.sleep(0.001)
    raise AssertionError(f"timed out waiting for {label}")


def _norm(x: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"op": x["op"]}
    if "token" in x:
        out["token"] = x["token"]
    if "channels" in x:
        out["channels"] = sorted(x["channels"])
    return out


async def run_case(case: Dict[str, Any]) -> None:
    srv = ScriptedServer()
    opts = case.get("options", {})
    clock = FakeClock()
    async with serve(srv.handler, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        ws = WebSocketClient(
            f"ws://127.0.0.1:{port}/api/v1/ws",
            allow_insecure=True,
            reconnect=False,
            clock=clock,
            reorder_window=opts.get("reorder_window_ms", 250) / 1000,
        )
        events: List[Dict[str, Any]] = []
        errors: List[str] = []
        ws.on(cws.RESYNC, lambda e: events.append({"type": "resync", "reason": (e.data or {}).get("reason")}))
        ws.on(cws.SEQUENCE_GAP, lambda e: events.append({"type": "sequence_gap", "channel": e.channel, **e.data}))
        await ws.connect()
        owner, snapshot = Scripted(), Scripted()
        helper: Optional[cws.LiveBalances] = None
        calls: List[asyncio.Task[Any]] = []
        answered: set[str] = set()
        mark = 0

        async def settle() -> None:
            await ws.ping()
            await ws.ping()

        async def start_helper() -> None:
            nonlocal helper
            helper = await ws.live_balances(
                snapshot=snapshot,
                owner_id=owner,
                min_snapshot_interval=opts.get("min_snapshot_interval_ms", 2000) / 1000,
            )
            helper.on(cws.BALANCE_ERROR, lambda e: errors.append(getattr(e.data, "code", "ERROR")))

        try:
            for i, st in enumerate(case["steps"]):
                at = f"{case['id']} step {i}"
                if "client" in st:
                    before = len(srv.requests())
                    if st["client"] == "auth":
                        calls.append(asyncio.ensure_future(ws.auth(st["token"])))
                    elif st["client"] == "subscribe":
                        calls.append(asyncio.ensure_future(ws.subscribe(*st["channels"])))
                    else:
                        calls.append(asyncio.ensure_future(start_helper()))
                    await until(lambda n=before: len(srv.requests()) > n, at)
                elif "server" in st:
                    frame = dict(st["server"])
                    if "reply_to" in st:
                        req = next(
                            x for x in reversed(srv.requests()) if x["op"] == st["reply_to"] and x["id"] not in answered
                        )
                        answered.add(req["id"])
                        frame["id"] = req["id"]
                    await srv.conn.send(json.dumps(frame))
                    await settle()
                elif "owner" in st:
                    await until(lambda: bool(owner.pending), at)
                    owner.answer(st["owner"])
                    await settle()
                elif "snapshot" in st:
                    await until(lambda: bool(snapshot.pending), at)
                    snapshot.answer([m.BalanceResponse.model_validate(r) for r in st["snapshot"]])
                    await settle()
                elif "advance_ms" in st:
                    clock.advance(st["advance_ms"] / 1000)
                    await settle()
                elif "expect_sent" in st:
                    await settle()
                    got = [_norm(x) for x in srv.requests()[mark:]]
                    mark = len(srv.requests())
                    assert got == [_norm(x) for x in st["expect_sent"]], at
                elif "expect_requests" in st:
                    await settle()
                    assert {"owner": owner.calls, "snapshot": snapshot.calls} == st["expect_requests"], at
                elif "expect_state" in st:
                    await settle()
                    rows = {b.asset: b for b in (helper.all() if helper else [])}
                    assert sorted(rows) == sorted(st["expect_state"]), (at, sorted(rows))
                    for asset, want in st["expect_state"].items():
                        assert rows[asset].total == Decimal(want["total"]), (at, asset)
                        assert rows[asset].sequence == want["sequence"], (at, asset)
                elif "expect_stale" in st:
                    await settle()
                    assert (helper.stale if helper else True) is st["expect_stale"], at
                elif "expect_errors" in st:
                    await settle()
                    got_e, errors[:] = list(errors), []
                    assert got_e == st["expect_errors"], at
                elif "expect_events" in st:
                    await settle()
                    got_ev, events[:] = list(events), []
                    assert len(got_ev) == len(st["expect_events"]), (at, got_ev)
                    for want in st["expect_events"]:
                        assert any(all(g.get(k) == v for k, v in want.items()) for g in got_ev), (at, want, got_ev)
                else:
                    raise AssertionError(f"{at}: unknown step {st}")
        finally:
            for t in calls:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*calls, return_exceptions=True)
            await ws.close()


@pytest.mark.parametrize("case", LIVE["cases"], ids=[c["id"] for c in LIVE["cases"]])
async def test_live_balances(case: Dict[str, Any]) -> None:
    await run_case(case)


@pytest.mark.parametrize("case", GAPS["cases"], ids=[c["id"] for c in GAPS["cases"]])
async def test_private_sequence_gap(case: Dict[str, Any]) -> None:
    await run_case(case)
