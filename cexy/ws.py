"""CEXY.io WebSocket client (asyncio).

One multiplexed connection to ``wss://api.cexy.io/api/v1/ws`` carries every
subscription. This client implements the rules in ``asyncapi.yaml``:

- sends ``{"op":"ping"}`` every 30 s (required: only client frames keep the connection
  alive; the server closes an idle connection after 90-120 s);
- accepts unsolicited ``{"type":"pong"}`` frames and reconnects when nothing has been
  received for 75 s;
- correlates every request by ``id``: the server acknowledges each one with the same id
  as ``authenticated``, ``subscribed``, ``unsubscribed``, ``pong`` or ``error``; a request
  that is not acknowledged within ``request_timeout`` raises ``WebSocketError("TIMEOUT")``;
- refuses locally beyond 100 subscriptions and keeps its own send rate at 200 messages
  per minute (the server closes the socket above 240, pings included);
- reconnects with exponential backoff and full jitter, then re-authenticates,
  re-subscribes and takes a fresh order-book snapshot for every book;
- ignores unknown event types and warns once on an unknown ``protocol_version``.

Authentication: private channels (``orders``, ``balances``, ``deposits``,
``withdrawals``, ``account``) need ``auth`` with a *session access token*. **API-key
authentication on the WebSocket is not available yet** (planned after request signing),
so API-key users should poll the REST endpoints for private state.
"""

from __future__ import annotations

import asyncio
import collections
import inspect
import itertools
import json
import logging
import random
import warnings
from dataclasses import dataclass, field
from decimal import Decimal
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from cexy._async.client import AsyncClient
from cexy._common import USER_AGENT, check_scheme
from cexy.auth import REDACTED, redact_text
from cexy.errors import CexyError

logger = logging.getLogger("cexy.ws")

DEFAULT_WS_URL = "wss://api.cexy.io/api/v1/ws"
KNOWN_PROTOCOL_VERSIONS = frozenset({1})
PRIVATE_CHANNELS = frozenset({"orders", "balances", "deposits", "withdrawals", "account"})
MAX_CHANNEL_LENGTH = 64
MAX_MESSAGE_BYTES = 64 * 1024
BOOK_DEPTH = 50

#: Synthetic events emitted by the client itself (not sent by the server).
AUTH_LOST = "auth_lost"
RECONNECTED = "reconnected"
BOOK_STALE = "book_stale"
RESYNC = "resync"
KNOWN_EVENT_TYPES = frozenset(
    {
        "ticker.update",
        "orderbook.update",
        "trade.new",
        "market.status",
        "order.created",
        "order.updated",
        "order.cancelled",
        "order.filled",
        "balance.updated",
        "deposit.detected",
        "deposit.updated",
        "deposit.completed",
        "withdrawal.updated",
        "session.revoked",
    }
)
SYNTHETIC_EVENT_TYPES = frozenset({AUTH_LOST, RECONNECTED, BOOK_STALE, RESYNC})

_warned_versions: Set[int] = set()


class WebSocketError(CexyError):
    """A WebSocket protocol error, or an ``error`` frame answering one of our requests."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message


@dataclass
class Event:
    """A server event (``orderbook.update``, ``trade.new``...) or a synthetic client event
    (``auth_lost``, ``reconnected``, ``book_stale``, ``resync``)."""

    type: str
    channel: Optional[str] = None
    data: Any = None
    sequence: Optional[int] = None
    timestamp: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SubscribeResult:
    added: List[str]
    refused: List[str]


Level = Tuple[Decimal, Decimal]
Handler = Callable[[Event], Optional[Awaitable[None]]]


class OrderBook:
    """A local top-50 order book kept in sync with ``orderbook:{symbol}``.

    Rules (from the API contract):

    - subscribe first, then take a REST snapshot at sequence S; drop updates with
      ``sequence <= S``;
    - every ``orderbook.update`` carries the complete top 50 of both sides (``"full": true``)
      and replaces the book outright; there are no deltas, and REST levels deeper than 50
      are never merged in;
    - a sequence gap marks the book ``stale`` until the next update heals it (no forced
      resync);
    - sequences reset when the server restarts, so a fresh snapshot is taken after every
      reconnect and sequences are never compared across connections.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bids: List[Level] = []
        self.asks: List[Level] = []
        self.sequence: Optional[int] = None
        self.stale = False
        self.synced = False
        self._buffer: List[Event] = []
        self._ready = asyncio.Event()

    @property
    def channel(self) -> str:
        return f"orderbook:{self.symbol}"

    def best_bid(self) -> Optional[Level]:
        return self.bids[0] if self.bids else None

    def best_ask(self) -> Optional[Level]:
        return self.asks[0] if self.asks else None

    async def wait_synced(self, timeout: Optional[float] = None) -> None:
        await asyncio.wait_for(self._ready.wait(), timeout)

    def reset(self) -> None:
        """Forget all state, including buffered updates (after a reconnect: sequences
        from the old connection are meaningless)."""
        self.begin_resync()
        self.sequence = None
        self._buffer.clear()

    def begin_resync(self) -> None:
        """Buffer updates until the next snapshot is applied."""
        self.synced = False
        self._ready.clear()

    def apply_snapshot(self, sequence: int, bids: Iterable[Iterable[Any]], asks: Iterable[Iterable[Any]]) -> None:
        self.bids = _levels(bids)[:BOOK_DEPTH]
        self.asks = _levels(asks)[:BOOK_DEPTH]
        self.sequence = sequence
        self.stale = False
        self.synced = True
        buffered, self._buffer = self._buffer, []
        for ev in buffered:
            self.apply_update(ev)
        self._ready.set()

    def apply_update(self, ev: Event) -> Optional[str]:
        """Apply an ``orderbook.update``: its ``bids``/``asks`` are the complete top 50 and
        replace the book (there are no deltas). Returns ``"stale"`` when a gap was detected."""
        if not self.synced:
            self._buffer.append(ev)
            return None
        seq = ev.sequence
        if seq is None or self.sequence is None:
            return None
        if seq <= self.sequence:
            return None  # already covered by the snapshot or a previous update
        gap = seq != self.sequence + 1
        data = ev.data if isinstance(ev.data, dict) else {}
        self.bids = _levels(data.get("bids", []))[:BOOK_DEPTH]
        self.asks = _levels(data.get("asks", []))[:BOOK_DEPTH]
        self.sequence = seq
        self.stale = gap
        return "stale" if gap else None

    def __repr__(self) -> str:
        return (
            f"OrderBook({self.symbol!r}, sequence={self.sequence}, synced={self.synced}, "
            f"stale={self.stale}, bid={self.best_bid()}, ask={self.best_ask()})"
        )


def _levels(raw: Iterable[Iterable[Any]]) -> List[Level]:
    out: List[Level] = []
    for lvl in raw:
        price, qty = list(lvl)[:2]
        out.append((Decimal(str(price)), Decimal(str(qty))))
    return out


class _SendLimiter:
    """At most ``limit`` messages in any rolling 60 s window."""

    def __init__(self, limit: int, window: float = 60.0) -> None:
        self.limit = limit
        self.window = window
        self._sent: Deque[float] = collections.deque()
        self._lock: Optional[asyncio.Lock] = None

    async def acquire(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            loop = asyncio.get_running_loop()
            while True:
                now = loop.time()
                while self._sent and now - self._sent[0] >= self.window:
                    self._sent.popleft()
                if len(self._sent) < self.limit:
                    self._sent.append(now)
                    return
                await asyncio.sleep(self.window - (now - self._sent[0]))


class WebSocketClient:
    """Async WebSocket client.

    >>> async with WebSocketClient() as ws:
    ...     await ws.subscribe("ticker:BTC/USDT")
    ...     book = await ws.order_book("BTC/USDT")
    ...     async for event in ws:
    ...         print(event.type, book.best_bid())

    ``rest`` is the ``AsyncClient`` used for order-book snapshots (a public one is
    created if omitted).
    """

    def __init__(
        self,
        url: str = DEFAULT_WS_URL,
        *,
        rest: Optional[AsyncClient] = None,
        token: Optional[str] = None,
        ping_interval: float = 30.0,
        liveness_timeout: float = 75.0,
        reconnect: bool = True,
        backoff_base: float = 0.5,
        backoff_max: float = 30.0,
        max_subscriptions: int = 100,
        messages_per_minute: int = 200,
        request_timeout: float = 10.0,
        user_agent_suffix: Optional[str] = None,
        allow_insecure: bool = False,
    ) -> None:
        # wss:// only; ws:// needs allow_insecure=True and a loopback host (local testing).
        check_scheme(url, "wss", "ws", allow_insecure)
        self.url = url
        self._rest = rest
        self._owns_rest = rest is None
        self._token = token
        self.ping_interval = ping_interval
        self.liveness_timeout = liveness_timeout
        self.reconnect = reconnect
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.max_subscriptions = max_subscriptions
        self.request_timeout = request_timeout
        self._ua = USER_AGENT + (f" {user_agent_suffix}" if user_agent_suffix else "")
        self._limiter = _SendLimiter(messages_per_minute)
        self._conn: Optional[ClientConnection] = None
        self._supervisor: Optional[asyncio.Task[None]] = None
        self._ping_task: Optional[asyncio.Task[None]] = None
        self._bg: Set[asyncio.Task[Any]] = set()
        self._closing = False
        self._ids = itertools.count(1)
        self._pending: Dict[str, asyncio.Future[Dict[str, Any]]] = {}
        self._channels: Set[str] = set()
        self._books: Dict[str, OrderBook] = {}
        self._handlers: Dict[str, List[Handler]] = collections.defaultdict(list)
        self._queue: Optional[asyncio.Queue[Event]] = None
        self._last_rx = 0.0
        self.welcome: Optional[Dict[str, Any]] = None
        self.authenticated = False
        self.user_id: Any = None
        self.connections = 0

    # -- lifecycle -----------------------------------------------------------------

    async def connect(self) -> None:
        """Open the connection and wait for the ``welcome`` frame."""
        self._closing = False
        if self._queue is None:
            self._queue = asyncio.Queue()
        await self._open()
        self._supervisor = asyncio.create_task(self._supervise())
        if self._token is not None:
            await self.auth(self._token)

    async def close(self) -> None:
        self._closing = True
        for task in [self._ping_task, *self._bg]:
            if task is not None:
                task.cancel()
        if self._conn is not None:
            await self._conn.close()
        if self._supervisor is not None:
            try:
                await self._supervisor
            except asyncio.CancelledError:
                pass
        if self._owns_rest and self._rest is not None:
            await self._rest.aclose()

    async def __aenter__(self) -> WebSocketClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def __repr__(self) -> str:
        token = REDACTED if self._token else None
        return f"WebSocketClient(url={self.url!r}, token={token!r}, channels={len(self._channels)})"

    async def _open(self) -> None:
        conn = await connect(
            self.url,
            user_agent_header=self._ua,
            ping_interval=None,  # the API needs application-level {"op":"ping"}
            max_size=4 * 1024 * 1024,
            open_timeout=self.request_timeout,
        )
        try:
            raw = await asyncio.wait_for(conn.recv(), self.request_timeout)
            frame = json.loads(raw)
            if frame.get("type") != "welcome":
                raise WebSocketError("PROTOCOL", f"expected welcome, got {frame.get('type')!r}")
        except BaseException:
            await conn.close()
            raise
        self._conn = conn
        self.connections += 1
        self.authenticated = False
        self._last_rx = asyncio.get_running_loop().time()
        self._handle_welcome(frame)
        self._ping_task = asyncio.create_task(self._ping_loop(conn))

    def _handle_welcome(self, frame: Dict[str, Any]) -> None:
        self.welcome = frame
        version = frame.get("protocol_version")
        if isinstance(version, int) and version not in KNOWN_PROTOCOL_VERSIONS and version not in _warned_versions:
            _warned_versions.add(version)
            warnings.warn(
                f"cexy: server WebSocket protocol_version {version} is newer than this SDK knows "
                f"({sorted(KNOWN_PROTOCOL_VERSIONS)}); continuing",
                RuntimeWarning,
                stacklevel=2,
            )
        max_subs = frame.get("max_subscriptions")
        if isinstance(max_subs, int) and 0 < max_subs < self.max_subscriptions:
            self.max_subscriptions = max_subs

    async def _supervise(self) -> None:
        while True:
            conn = self._conn
            if conn is not None:
                try:
                    async for raw in conn:
                        self._last_rx = asyncio.get_running_loop().time()
                        self._on_frame(raw)
                except ConnectionClosed:
                    pass
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._fail_pending(WebSocketError("DISCONNECTED", "connection closed"))
            if self._closing or not self.reconnect:
                return
            attempt = 0
            while not self._closing:
                delay = random.uniform(0, min(self.backoff_max, self.backoff_base * 2**attempt))  # noqa: S311
                attempt += 1
                logger.info("cexy.ws: reconnecting in %.2fs (attempt %d)", delay, attempt)
                await asyncio.sleep(delay)
                try:
                    await self._open()
                    break
                except (OSError, WebSocketException, asyncio.TimeoutError, WebSocketError, ValueError) as exc:
                    logger.info("cexy.ws: reconnect failed: %s", type(exc).__name__)
            if self._closing:
                return
            self._spawn(self._restore())

    async def _restore(self) -> None:
        """After a reconnect: auth, re-subscribe, fresh snapshot for every book."""
        for book in self._books.values():
            book.reset()
        channels = sorted(self._channels)
        self._channels.clear()
        if self._token is not None and any(c in PRIVATE_CHANNELS for c in channels):
            try:
                await self.auth(self._token)
            except WebSocketError as exc:
                logger.warning("cexy.ws: re-auth failed: %s", exc.code)
                channels = [c for c in channels if c not in PRIVATE_CHANNELS]
        if channels:
            await self.subscribe(*channels)
        await self._emit(Event(type=RECONNECTED, data={"channels": channels}))
        for book in list(self._books.values()):
            await self._snapshot(book)

    async def _ping_loop(self, conn: ClientConnection) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                if loop.time() - self._last_rx > self.liveness_timeout:
                    logger.info("cexy.ws: no server frame for %.0fs, reconnecting", self.liveness_timeout)
                    await conn.close()
                    return
                self._spawn(self._ping_once())
        except (ConnectionClosed, asyncio.CancelledError):
            return

    # -- sending -------------------------------------------------------------------

    async def _send(self, msg: Dict[str, Any], conn: Optional[ClientConnection] = None) -> None:
        conn = conn or self._conn
        if conn is None:
            raise WebSocketError("NOT_CONNECTED", "call connect() first")
        text = json.dumps(msg, separators=(",", ":"))
        if len(text.encode()) > MAX_MESSAGE_BYTES:
            raise ValueError("message exceeds 64 KiB")
        await self._limiter.acquire()
        await conn.send(text)  # text frames only

    async def _request(self, msg: Dict[str, Any], expect: str) -> Dict[str, Any]:
        """Send ``msg`` with a fresh ``id`` and wait for the acknowledgement with that id.

        Raises ``WebSocketError`` with the server's code on an ``error`` ack, ``TIMEOUT``
        when no ack arrives within ``request_timeout``, and ``PROTOCOL`` on an unexpected
        ack type.
        """
        req_id = str(next(self._ids))
        msg = {**msg, "id": req_id}
        fut: asyncio.Future[Dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            await self._send(msg)
            reply = await asyncio.wait_for(fut, self.request_timeout)
        except asyncio.TimeoutError:
            raise WebSocketError("TIMEOUT", f"no acknowledgement for {msg['op']}") from None
        finally:
            self._pending.pop(req_id, None)
        rtype = reply.get("type")
        if rtype == "error":
            secrets = (self._token,) if self._token else ()
            raise WebSocketError(str(reply.get("code")), redact_text(str(reply.get("message") or ""), secrets))
        if rtype != expect:
            raise WebSocketError("PROTOCOL", f"expected {expect!r} for {msg['op']}, got {rtype!r}")
        return reply

    async def ping(self) -> float:
        """Send a correlated ping and wait for its ``pong``; returns the round trip in seconds."""
        loop = asyncio.get_running_loop()
        start = loop.time()
        await self._request({"op": "ping"}, "pong")
        return loop.time() - start

    async def _ping_once(self) -> None:
        try:
            await self.ping()
        except WebSocketError as exc:
            # Liveness is judged on any received frame (see _ping_loop), not on this ack.
            logger.debug("cexy.ws: ping not acknowledged: %s", exc.code)

    async def auth(self, token: str) -> None:
        """Authenticate with a session access token (for private channels).

        Waits for the server's ``authenticated`` acknowledgement; raises ``WebSocketError``
        on an ``error`` reply or when no acknowledgement arrives within ``request_timeout``.
        API-key authentication on the WebSocket is not available yet. The token is never
        logged or put in the URL.
        """
        self._token = token
        self.authenticated = False
        reply = await self._request({"op": "auth", "token": token}, "authenticated")
        self.authenticated = True
        self.user_id = reply.get("user_id")

    async def subscribe(self, *channels: str) -> SubscribeResult:
        """Subscribe to channels. Channels beyond the 100-subscription limit are refused
        locally and reported in ``refused``; nothing is sent for them."""
        wanted: List[str] = []
        for ch in channels:
            if not ch or len(ch) > MAX_CHANNEL_LENGTH:
                raise ValueError(f"invalid channel name {ch!r} (1-{MAX_CHANNEL_LENGTH} characters)")
            if ch not in self._channels and ch not in wanted:
                wanted.append(ch)
        room = max(0, self.max_subscriptions - len(self._channels))
        send, refused = wanted[:room], wanted[room:]
        if refused:
            logger.warning("cexy.ws: subscription limit %d reached; refused %s", self.max_subscriptions, refused)
        if not send:
            return SubscribeResult(added=[], refused=refused)
        reply = await self._request({"op": "subscribe", "channels": send}, "subscribed")
        acked = reply.get("channels")
        added = [c for c in (acked if isinstance(acked, list) else send) if isinstance(c, str)]
        self._channels.update(added)
        return SubscribeResult(added=added, refused=refused)

    async def unsubscribe(self, *channels: str) -> None:
        """Unsubscribe and wait for the ``unsubscribed`` acknowledgement."""
        for ch in channels:
            self._channels.discard(ch)
            if ch.startswith("orderbook:"):
                self._books.pop(ch[len("orderbook:") :], None)
        await self._request({"op": "unsubscribe", "channels": list(channels)}, "unsubscribed")

    @property
    def channels(self) -> Set[str]:
        return set(self._channels)

    # -- order books ---------------------------------------------------------------

    async def order_book(self, symbol: str) -> OrderBook:
        """Subscribe to ``orderbook:{symbol}``, then snapshot over REST and return a book
        that stays in sync (see ``OrderBook`` for the rules)."""
        book = self._books.get(symbol)
        if book is None:
            book = OrderBook(symbol)
            self._books[symbol] = book  # register first so early updates are buffered
        await self.subscribe(book.channel)
        await self._snapshot(book)
        return book

    async def _snapshot(self, book: OrderBook) -> None:
        if self._rest is None:
            self._rest = AsyncClient()
        book.begin_resync()
        snap = await self._rest.markets.orderbook(book.symbol, depth=BOOK_DEPTH)
        book.apply_snapshot(snap.sequence, snap.bids, snap.asks)

    async def resync(self) -> None:
        """Fresh REST snapshot for every book (after dropped messages)."""
        for book in list(self._books.values()):
            await self._snapshot(book)

    # -- receiving -----------------------------------------------------------------

    def on(self, event_type: str, handler: Handler) -> None:
        """Call ``handler(event)`` for each event of ``event_type`` (``"*"`` for all)."""
        self._handlers[event_type].append(handler)

    def __aiter__(self) -> AsyncIterator[Event]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[Event]:
        if self._queue is None:
            raise WebSocketError("NOT_CONNECTED", "call connect() first")
        while True:
            yield await self._queue.get()

    def _spawn(self, coro: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
        task.add_done_callback(_log_task_error)

    def _fail_pending(self, exc: Exception) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    def _on_frame(self, raw: Union[str, bytes]) -> None:
        try:
            frame = json.loads(raw)
        except ValueError:
            logger.debug("cexy.ws: ignoring non-JSON frame")
            return
        if not isinstance(frame, dict):
            return
        ftype = frame.get("type")
        fid = frame.get("id")
        if ftype == "welcome":
            self._handle_welcome(frame)
            return
        if isinstance(fid, str) and fid in self._pending:
            fut = self._pending[fid]
            if not fut.done():
                fut.set_result(frame)
            return
        if ftype in ("pong", "subscribed", "unsubscribed", "authenticated"):
            return  # unsolicited pong (no id: liveness only) or a late/uncorrelated ack
        if ftype == "error":
            self._spawn(self._on_error(frame))
            return
        if ftype not in KNOWN_EVENT_TYPES:
            logger.debug("cexy.ws: ignoring unknown frame type %r", ftype)
            return
        ev = Event(
            type=str(ftype),
            channel=frame.get("channel"),
            data=frame.get("data"),
            sequence=frame.get("sequence"),
            timestamp=frame.get("timestamp"),
            raw=frame,
        )
        if ftype == "orderbook.update" and ev.channel and ev.channel.startswith("orderbook:"):
            book = self._books.get(ev.channel[len("orderbook:") :])
            if book is not None and book.apply_update(ev) == "stale":
                self._spawn(self._emit(Event(type=BOOK_STALE, channel=ev.channel, sequence=ev.sequence)))
        self._spawn(self._emit(ev))
        if ftype == "session.revoked":
            self.authenticated = False
            self._token = None  # never re-auth with a revoked session
            self._spawn(self._emit(Event(type=AUTH_LOST, channel=ev.channel, data=ev.data)))

    async def _on_error(self, frame: Dict[str, Any]) -> None:
        code = str(frame.get("code"))
        logger.warning("cexy.ws: server error %s: %s", code, frame.get("message"))
        if code == "CONCURRENT_MODIFICATION" and frame.get("id") is None:
            # Messages were dropped: every book and channel may be out of date.
            await self._emit(Event(type=RESYNC, data={"reason": code}, raw=frame))
            await self.resync()

    async def _emit(self, ev: Event) -> None:
        if self._queue is not None:
            self._queue.put_nowait(ev)
        for handler in [*self._handlers.get(ev.type, []), *self._handlers.get("*", [])]:
            try:
                result = handler(ev)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("cexy.ws: event handler failed")


def _log_task_error(task: asyncio.Task[Any]) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.warning("cexy.ws: background task failed: %r", task.exception())
