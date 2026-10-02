"""Subscribe refusals for every channel (spot and futures): error frames carrying a subscribe's id
are collected until the single ``subscribed`` ack (or until every channel has been refused), and
re-subscribes after a reconnect or a re-auth sort refusals into pending (UNAUTHENTICATED on a
private channel) or dropped and reported (anything else)."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Tuple

import pytest
from websockets.asyncio.server import serve

from cexy import ws as cws
from cexy.ws import WebSocketClient
from tests.conftest import load
from tests.test_ws_futures import FuturesServer, drain, make, until


@pytest.fixture
async def server():  # type: ignore[no-untyped-def]
    fs = FuturesServer()
    async with serve(fs.handler, "127.0.0.1", 0) as srv:
        fs.port = next(iter(srv.sockets)).getsockname()[1]
        yield fs


ERR = {"type": "error", "code": "VALIDATION_FAILED", "message": "unknown market"}


def subscribes(server: FuturesServer) -> List[List[str]]:
    return [m["channels"] for m in server.requests() if m["op"] == "subscribe"]


async def test_partly_refused_spot_batch(server: FuturesServer) -> None:
    server.refuse = {"ticker:NOPE/USDT": "VALIDATION_FAILED"}
    async with make(server) as ws:
        res = await ws.subscribe("ticker:BTC/USDT", "ticker:NOPE/USDT", "trades:BTC/USDT")
        assert res.added == ["ticker:BTC/USDT", "trades:BTC/USDT"]
        assert res.refused == ["ticker:NOPE/USDT"]
        assert res.errors["ticker:NOPE/USDT"].code == "VALIDATION_FAILED"
        assert ws.channels == {"ticker:BTC/USDT", "trades:BTC/USDT"}
        events = await drain(ws, 0.1)
        assert [(e.type, e.channel) for e in events] == [(cws.SUBSCRIBE_REFUSED, "ticker:NOPE/USDT")]
    # one frame, and the refused channel is not retried
    assert subscribes(server) == [["ticker:BTC/USDT", "ticker:NOPE/USDT", "trades:BTC/USDT"]]


async def test_all_refused_raises_without_waiting_for_an_ack(server: FuturesServer) -> None:
    server.refuse = {"ticker:NOPE/USDT": "VALIDATION_FAILED", "futures.trades:btc": "NOT_FOUND"}
    async with make(server) as ws:
        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(cws.SubscribeRefusedError) as exc:
            await ws.subscribe("ticker:NOPE/USDT", "futures.trades:btc")
        assert loop.time() - start < ws.request_timeout / 2  # completed on the last error
        assert exc.value.code == "VALIDATION_FAILED" and isinstance(exc.value, cws.WebSocketError)
        assert exc.value.result.refused == ["ticker:NOPE/USDT", "futures.trades:btc"]
        assert {c: e.code for c, e in exc.value.result.errors.items()} == {
            "ticker:NOPE/USDT": "VALIDATION_FAILED",
            "futures.trades:btc": "NOT_FOUND",
        }
        assert ws.channels == set()


async def test_timeout_after_an_error_counts_as_all_refused(server: FuturesServer) -> None:
    server.script[("subscribe", 0)] = [ERR]  # one error, then nothing
    async with make(server) as ws:
        with pytest.raises(cws.SubscribeRefusedError) as exc:
            await ws.subscribe("ticker:BTC/USDT", "ticker:NOPE/USDT")
        assert exc.value.result.refused == ["ticker:BTC/USDT", "ticker:NOPE/USDT"]
        assert ws.channels == set()


async def test_timeout_without_errors_raises_timeout_and_holds(server: FuturesServer) -> None:
    server.script[("subscribe", 0)] = []
    ws = WebSocketClient(server.url, allow_insecure=True, request_timeout=0.3, backoff_base=0.01, backoff_max=0.05)
    async with ws:
        with pytest.raises(cws.WebSocketError) as exc:
            await ws.subscribe("ticker:BTC/USDT", "futures.mids")
        assert exc.value.code == "TIMEOUT" and not isinstance(exc.value, cws.SubscribeRefusedError)
        assert ws.channels == {"ticker:BTC/USDT", "futures.mids"}  # held
        await server.conn.close()
        await until(lambda: ws.connections == 2 and len(subscribes(server)) == 2, "re-sent after reconnect")
        assert sorted(subscribes(server)[-1]) == ["futures.mids", "ticker:BTC/USDT"]


async def test_futures_names_pair_exactly_spot_names_ignore_case(server: FuturesServer) -> None:
    # futures.trades:btc is refused although the ack lists futures.trades:BTC; the spot name
    # comes back upper-cased and is accepted. The errors pair with the refused channels in order.
    errs = [
        {"type": "error", "code": "NOT_FOUND", "message": "Futures market not found"},
        {"type": "error", "code": "VALIDATION_FAILED", "message": "unknown market"},
    ]
    ack = {"type": "subscribed", "channels": ["futures.trades:BTC", "ticker:BTC/USDT"]}
    server.script[("subscribe", 0)] = [*errs, ack]
    async with make(server) as ws:
        res = await ws.subscribe("futures.trades:btc", "futures.trades:BTC", "ticker:btc/usdt", "ticker:NOPE/USDT")
        assert res.added == ["futures.trades:BTC", "ticker:BTC/USDT"]
        assert {c: e.code for c, e in res.errors.items()} == {
            "futures.trades:btc": "NOT_FOUND",
            "ticker:NOPE/USDT": "VALIDATION_FAILED",
        }


async def test_canonical_names_in_the_ack_are_not_refusals(server: FuturesServer) -> None:
    server.script[("subscribe", 0)] = [{"type": "subscribed", "channels": ["ticker:BTC/USDT"]}]
    async with make(server) as ws:
        res = await ws.subscribe("ticker:btc/usdt")
        assert res.added == ["ticker:BTC/USDT"] and res.refused == [] and res.errors == {}


async def test_stop_processing_refusal_covers_the_rest(server: FuturesServer) -> None:
    # The 100-subscription refusal stops the frame: one error for several channels.
    limit = {"type": "error", "code": "RATE_LIMITED", "message": "at most 100 subscriptions"}
    server.script[("subscribe", 0)] = [limit, {"type": "subscribed", "channels": ["ticker:A/USDT"]}]
    async with make(server) as ws:
        res = await ws.subscribe("ticker:A/USDT", "ticker:B/USDT", "ticker:C/USDT")
        assert res.added == ["ticker:A/USDT"]
        assert {c: e.code for c, e in res.errors.items()} == {
            "ticker:B/USDT": "RATE_LIMITED",
            "ticker:C/USDT": "RATE_LIMITED",
        }


async def test_reconnect_resubscribe_refusals(server: FuturesServer) -> None:
    ws = WebSocketClient(
        server.url,
        allow_insecure=True,
        request_timeout=0.5,
        backoff_base=0.01,
        backoff_max=0.05,
        key_signer=make(server)._key_signer,
    )
    async with ws:
        await ws.auth_key()
        await ws.subscribe("ticker:BTC/USDT", "futures.mids", "orders")
        await drain(ws)
        server.refuse = {"futures.mids": "NOT_FOUND", "orders": "UNAUTHENTICATED"}
        await server.conn.close()
        await until(lambda: ws.connections == 2, "reconnect")
        await until(lambda: "orders" in ws.pending_channels and "futures.mids" not in ws.channels, "refusals")
        events = await drain(ws, 0.1)
        assert ws.channels == {"ticker:BTC/USDT"}
        assert ws.pending_channels == {"orders"}  # UNAUTHENTICATED on a private channel: pending
        refused = [(e.channel, e.data["code"]) for e in events if e.type == cws.SUBSCRIBE_REFUSED]
        # every refusal is reported; only the non-UNAUTHENTICATED one is dropped
        assert sorted(refused) == [("futures.mids", "NOT_FOUND"), ("orders", "UNAUTHENTICATED")]
        # restored in one frame; nothing refused is retried
        assert sorted(subscribes(server)[-1]) == ["futures.mids", "orders", "ticker:BTC/USDT"]
        assert any(e.type == cws.RECONNECTED for e in events)


async def test_reauth_resubscribe_refusals(server: FuturesServer) -> None:
    async with make(server) as ws:
        await ws.auth_key()
        await ws.subscribe("orders", "futures.account", "balances")
        await server.push({"type": "signed_out", "reason": "expired"})
        await until(lambda: ws.pending_channels == {"orders", "futures.account", "balances"}, "signed out")
        server.refuse = {"futures.account": "NOT_FOUND", "balances": "UNAUTHENTICATED"}
        n = len(subscribes(server))
        await ws.auth_key()
        await until(lambda: len(subscribes(server)) == n + 1 and "orders" in ws.channels, "re-subscribe")
        await until(lambda: "balances" in ws.pending_channels, "pending")
        events = await drain(ws, 0.1)
        assert ws.channels == {"orders"}
        assert ws.pending_channels == {"balances"}
        refused = [(e.channel, e.data["code"]) for e in events if e.type == cws.SUBSCRIBE_REFUSED]
        assert sorted(refused) == [("balances", "UNAUTHENTICATED"), ("futures.account", "NOT_FOUND")]
        assert len(subscribes(server)) == n + 1  # not retried


# -- conformance/ws/subscribe_refusals.json ---------------------------------------------

REFUSALS = load("ws/subscribe_refusals.json")
SIMPLE = [c for c in REFUSALS["cases"] if "send" in c]
CONCURRENT = [c for c in REFUSALS["cases"] if "concurrent" in c]


def outcome(res: Any) -> Dict[str, Any]:
    return {"added": res.added, "refused": {c: e.code for c, e in res.errors.items()}}


async def run_subscribe(ws: WebSocketClient, channels: List[str]) -> Tuple[Dict[str, Any], bool]:
    try:
        return outcome(await ws.subscribe(*channels)), False
    except cws.SubscribeRefusedError as exc:
        return outcome(exc.result), True


def test_refusal_fixture_has_every_case() -> None:
    assert len(REFUSALS["cases"]) == 7
    assert "error_for_other_request_not_misattributed" in {c["id"] for c in CONCURRENT}


@pytest.mark.parametrize("case", SIMPLE, ids=[c["id"] for c in SIMPLE])
async def test_subscribe_refusals_conformance(server: FuturesServer, case: Dict[str, Any]) -> None:
    server.script[("subscribe", 0)] = case["server"]
    exp = case["expect"]
    async with make(server) as ws:
        loop = asyncio.get_running_loop()
        start = loop.time()
        if "error_code" in exp:
            with pytest.raises(cws.WebSocketError) as exc:
                await ws.subscribe(*case["send"])
            assert exc.value.code == exp["error_code"]
            assert not isinstance(exc.value, cws.SubscribeRefusedError)
            assert sorted(ws.channels) == sorted(exp["held_after"])
            return
        got, failed = await run_subscribe(ws, case["send"])
        assert got == {"added": exp["added"], "refused": exp["refused"]}
        assert failed is exp["fails"]
        if exp.get("completes_before_timeout"):
            assert loop.time() - start < ws.request_timeout / 2
        assert ws.channels == set(exp["added"])  # refused channels are not held
        events = await drain(ws, 0.05)
        refused = {e.channel: e.data["code"] for e in events if e.type == cws.SUBSCRIBE_REFUSED}
        assert refused == exp["refused"]
    assert len(subscribes(server)) == 1  # one frame, nothing retried


@pytest.mark.parametrize("case", CONCURRENT, ids=[c["id"] for c in CONCURRENT])
async def test_subscribe_refusals_concurrent(server: FuturesServer, case: Dict[str, Any]) -> None:
    reqs = case["concurrent"]
    for i in range(len(reqs)):
        server.script[("subscribe", i)] = []  # answered below, in the case's order
    async with make(server) as ws:
        tasks = {r["request"]: asyncio.ensure_future(run_subscribe(ws, r["send"])) for r in reqs}
        await until(lambda: len(subscribes(server)) == len(reqs), "both requests")
        by_channels = {json.dumps(m["channels"]): m["id"] for m in server.requests() if m["op"] == "subscribe"}
        ids = {r["request"]: by_channels[json.dumps(r["send"])] for r in reqs}
        for item in case["server"]:
            await server.push({**item["frame"], "id": ids[item["to"]]})
        for name, task in tasks.items():
            got, failed = await asyncio.wait_for(task, 2)
            exp = case["expect"][name]
            assert got == {"added": exp["added"], "refused": exp["refused"]}, name
            assert failed is exp["fails"], name
