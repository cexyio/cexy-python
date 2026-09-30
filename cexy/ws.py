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
from cexy._generated import models as m
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
#: The server ended this connection's private subscriptions. ``data``: ``reason``
#: (``user_changed`` | ``auth_failed`` | ``session_revoked``), ``previous_user_id``, ``user_id``,
#: ``code`` (auth_failed only) and ``dropped`` (the private channels no longer held; they are
#: re-subscribed automatically, followed by ``resync`` with reason ``reauth``).
AUTH_CHANGED = "auth_changed"
# ``auth_changed`` reasons: ``user_changed``, ``auth_failed`` (``code``: the server's error code),
# ``session_revoked``, ``token_expired`` (the ``signed_out`` frame), and ``signed_out`` for a server
# sign-out reason this SDK does not know (``code``: the raw reason). The set may grow.
RECONNECTED = "reconnected"
#: A private channel skipped sequence numbers on this connection (after the reorder window): events
#: were lost. ``channel`` is set; ``data``: ``expected``, ``received``. Followed by ``resync`` with
#: ``{"reason": "sequence_gap", "channel": ...}``; refetch that channel's state.
SEQUENCE_GAP = "sequence_gap"
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
        "balances.resync",
        "deposits.resync",
        "withdrawals.resync",
    }
)
SYNTHETIC_EVENT_TYPES = frozenset({AUTH_LOST, AUTH_CHANGED, RECONNECTED, BOOK_STALE, RESYNC, SEQUENCE_GAP})

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


class Clock:
    """TEST-ONLY time source for the reorder-window timer and ``LiveBalances`` scheduling (minimum
    snapshot interval, retry backoff). Socket timeouts always use the event loop. The default uses
    the running loop's ``time()`` and ``call_later()``."""

    def now(self) -> float:
        return asyncio.get_running_loop().time()

    def call_later(self, delay: float, fn: Callable[[], None]) -> Any:
        """Returns a handle with ``cancel()``."""
        return asyncio.get_running_loop().call_later(delay, fn)


REAL_CLOCK = Clock()


@dataclass
class _SeqState:
    next: int
    holes: Set[int] = field(default_factory=set)
    first: Optional[Tuple[int, int]] = None
    timer: Any = None


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
        reorder_window: float = 0.25,
        clock: Optional[Clock] = None,
    ) -> None:
        """``reorder_window``: seconds a missing private sequence number gets to arrive (channels with
        several publishers can swap adjacent frames) before it counts as a gap. ``clock``: TEST-ONLY
        (see ``Clock``)."""
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
        # op and token of each pending auth request, to act on its reply as it arrives
        self._pending_auth: Dict[str, str] = {}
        self._channels: Set[str] = set()
        # private channels dropped by a server sign-out, re-subscribed after the next successful auth
        self._pending_private: Set[str] = set()
        self._books: Dict[str, OrderBook] = {}
        self._handlers: Dict[str, List[Handler]] = collections.defaultdict(list)
        self._queue: Optional[asyncio.Queue[Event]] = None
        self._last_rx = 0.0
        self.welcome: Optional[Dict[str, Any]] = None
        self.authenticated = False
        self.user_id: Any = None
        self.connections = 0
        self.reorder_window = reorder_window
        self._clock = clock or REAL_CLOCK
        self._seq: Dict[str, _SeqState] = {}
        self._live_balances: List[Any] = []
        self._balances_by_helper = False

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
        self._reset_seq()
        for lb in list(self._live_balances):
            lb.close()
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
        self.user_id = None  # a new connection starts signed out
        self._reset_seq()  # sequences on a new connection are unrelated
        for lb in self._live_balances:
            lb._on_disconnect()
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
        private = [c for c in channels if c in PRIVATE_CHANNELS]
        # Public channels are re-subscribed below; private ones stay held through the re-auth,
        # so a refused token reports them in auth_changed and moves them to pending.
        self._channels.difference_update(c for c in channels if c not in PRIVATE_CHANNELS)
        if private and self._token is None:
            self._channels.difference_update(private)
            self._pending_private.update(private)
        elif private or (self._token is not None and self._pending_private):
            try:
                await self.auth(self._token)  # type: ignore[arg-type]
            except WebSocketError as exc:
                logger.warning("cexy.ws: re-auth failed: %s", exc.code)
        still_held = [c for c in private if c in self._channels]
        self._channels.difference_update(still_held)  # not subscribed on this connection yet
        channels = [c for c in channels if c not in PRIVATE_CHANNELS] + still_held
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
        if msg["op"] == "auth":
            self._pending_auth[req_id] = msg["token"]
        try:
            await self._send(msg)
            reply = await asyncio.wait_for(fut, self.request_timeout)
        except asyncio.TimeoutError:
            raise WebSocketError("TIMEOUT", f"no acknowledgement for {msg['op']}") from None
        finally:
            self._pending.pop(req_id, None)
            self._pending_auth.pop(req_id, None)
        rtype = reply.get("type")
        if rtype == "error":
            # The token of this request too: a refused token is already forgotten here.
            secrets = tuple(t for t in (self._token, msg.get("token")) if t)
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
        await self._request({"op": "auth", "token": token}, "authenticated")
        # authenticated / user_id are set as the reply arrives (see _on_authenticated).

    async def subscribe(self, *channels: str) -> SubscribeResult:
        """Subscribe to channels. Channels beyond the 100-subscription limit are refused
        locally and reported in ``refused``; nothing is sent for them."""
        wanted: List[str] = []
        for ch in channels:
            if not ch or len(ch) > MAX_CHANNEL_LENGTH:
                raise ValueError(f"invalid channel name {ch!r} (1-{MAX_CHANNEL_LENGTH} characters)")
            # Private channels waiting for the next successful auth count as held.
            if ch not in self._channels and ch not in self._pending_private and ch not in wanted:
                wanted.append(ch)
        room = max(0, self.max_subscriptions - len(self._channels) - len(self._pending_private))
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
            self._pending_private.discard(ch)
            self._reset_seq(ch)
            if ch.startswith("orderbook:"):
                self._books.pop(ch[len("orderbook:") :], None)
        await self._request({"op": "unsubscribe", "channels": list(channels)}, "unsubscribed")

    @property
    def channels(self) -> Set[str]:
        return set(self._channels)

    @property
    def has_token(self) -> bool:
        """True while a session token is kept for automatic re-authentication. A refused token
        and a revoked session are forgotten."""
        return self._token is not None

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

    async def live_balances(
        self,
        *,
        snapshot: Optional[Callable[[], Awaitable[List[Any]]]] = None,
        owner_id: Optional[Callable[[], Awaitable[str]]] = None,
        account_id: Optional[str] = None,
        min_snapshot_interval: float = 2.0,
        retry: float = 1.0,
    ) -> LiveBalances:
        """Live balances of the authenticated account (call ``auth()`` first).

        Subscribes ``balances``, takes a REST snapshot, applies newer ``balance.updated`` events and
        refetches by itself when events may be missing (a frame gap, ``balances.resync``,
        ``CONCURRENT_MODIFICATION``, a reconnect, an account change). Snapshots come from
        ``rest.account.balances()`` or ``snapshot``. The snapshot source's owner
        (``rest.account.id()``, or ``owner_id`` / ``account_id``, required with a custom ``snapshot``)
        must equal the WebSocket's authenticated user; otherwise nothing is merged
        (``ACCOUNT_MISMATCH``). It is checked at the start and again after every account change.
        """
        custom_snapshot = snapshot is not None
        if snapshot is None:
            if self._rest is None:
                raise WebSocketError("CONFIG", "live_balances() needs snapshot= or rest=")
            rest = self._rest
            snapshot = rest.account.balances
        if owner_id is None:
            if account_id is not None:
                fixed = account_id

                async def _fixed() -> str:
                    return fixed

                owner_id = _fixed
            elif not custom_snapshot and self._rest is not None and hasattr(self._rest.account, "id"):
                # The REST key's account owns only the REST key's own snapshots: a custom snapshot
                # source must name its owner.
                owner_id = self._rest.account.id
            else:
                raise WebSocketError(
                    "CONFIG", "live_balances() needs owner_id= or account_id= to check the snapshot's account"
                )
        lb = LiveBalances(self, snapshot, owner_id, min_snapshot_interval, retry)
        self._live_balances.append(lb)
        held = "balances" in self._channels or "balances" in self._pending_private
        if not held:
            self._balances_by_helper = True
        try:
            res = await self.subscribe("balances")
        except BaseException:
            lb.close()
            raise
        if res.refused:
            lb.close()
            raise WebSocketError("LOCAL_SUBSCRIPTION_LIMIT", "cannot subscribe to balances: limit reached")
        if held:
            lb._trigger("start")  # no `subscribed` reply comes for a held channel
        return lb

    def _release_balances(self, lb: LiveBalances) -> None:
        if lb in self._live_balances:
            self._live_balances.remove(lb)
        if not self._live_balances and self._balances_by_helper and not self._closing:
            self._balances_by_helper = False
            if self._conn is not None:
                self._spawn(self.unsubscribe("balances"))
            else:
                self._channels.discard("balances")

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
            token = self._pending_auth.pop(fid, None)
            if token is not None:
                # Act on an auth reply as it arrives, before any later frame.
                if ftype == "authenticated":
                    uid = frame.get("user_id")
                    self._on_authenticated(uid if isinstance(uid, str) else None)
                elif ftype == "error":
                    # Any error on an auth frame signs the connection out.
                    if self._token == token:
                        self._token = None  # a refused token is not re-sent on reconnect
                    self._signed_out("auth_failed", str(frame.get("code")))
            if ftype == "subscribed":
                acked = [c for c in frame.get("channels") or [] if isinstance(c, str)]
                for c in acked:
                    self._reset_seq(c)  # the next frame is the new baseline
                if "balances" in acked:
                    for lb in list(self._live_balances):
                        lb._trigger("resubscribed")
            fut = self._pending[fid]
            if not fut.done():
                fut.set_result(frame)
            return
        if ftype == "signed_out":
            # signed_out (a planned server frame): the server signed this connection out (token
            # expired, session revoked, or a future reason). Private subscriptions are gone; a fresh
            # auth on this socket restores them.
            raw_reason = frame.get("reason")
            reason = raw_reason if isinstance(raw_reason, str) and raw_reason else "unknown"
            self._token = None
            if reason == "revoked":
                self._signed_out("session_revoked")
                lost = {"session_id": None, "reason": "signed_out", "current": True}
                self._spawn(self._emit(Event(type=AUTH_LOST, channel="account", data=lost, raw=frame)))
            elif reason == "expired":
                self._signed_out("token_expired")
            else:
                self._signed_out("signed_out", reason)
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
        if ev.channel in PRIVATE_CHANNELS and isinstance(ev.sequence, int) and not isinstance(ev.sequence, bool):
            self._track_seq(ev.channel, ev.sequence)
        if ftype in ("balances.resync", "deposits.resync", "withdrawals.resync"):
            reason = str(ftype).replace(".", "_")  # balances_resync, deposits_resync, withdrawals_resync
            self._spawn(self._emit(ev))
            self._spawn(self._emit(Event(type=RESYNC, channel=ev.channel, data={"reason": reason})))
            if ftype == "balances.resync":
                for lb in list(self._live_balances):
                    lb._trigger("balances_resync")
            return
        if ftype == "balance.updated" and isinstance(ev.data, dict):
            for lb in list(self._live_balances):
                lb._on_event(ev.data)
        if ftype == "orderbook.update" and ev.channel and ev.channel.startswith("orderbook:"):
            book = self._books.get(ev.channel[len("orderbook:") :])
            if book is not None and book.apply_update(ev) == "stale":
                self._spawn(self._emit(Event(type=BOOK_STALE, channel=ev.channel, sequence=ev.sequence)))
        self._spawn(self._emit(ev))
        # Only this connection's own session signs it out (the server checks current == true
        # exactly); current false, missing or not a boolean changes nothing.
        if ftype == "session.revoked" and isinstance(ev.data, dict) and ev.data.get("current") is True:
            self._token = None  # never re-auth with a revoked session
            self._signed_out("session_revoked")
            self._spawn(self._emit(Event(type=AUTH_LOST, channel=ev.channel, data=ev.data)))

    def _reset_seq(self, channel: Optional[str] = None) -> None:
        """Forget the sequence baseline of ``channel`` (all channels when None)."""
        states = list(self._seq.values()) if channel is None else [s for s in [self._seq.get(channel)] if s]
        for st in states:
            if st.timer is not None:
                st.timer.cancel()
        if channel is None:
            self._seq.clear()
        else:
            self._seq.pop(channel, None)

    def _track_seq(self, channel: str, n: int) -> None:
        """First frame: baseline. Lower than expected: late, never a gap. Higher: holes that must
        fill within the reorder window."""
        st = self._seq.get(channel)
        if st is None:
            self._seq[channel] = _SeqState(next=n + 1)
            return
        if n < st.next:
            if n in st.holes:
                st.holes.discard(n)
                if not st.holes and st.timer is not None:
                    st.timer.cancel()
                    st.timer = None
                    st.first = None
            return
        if n > st.next and st.first is None:
            st.first = (st.next, n)
        st.holes.update(range(st.next, n))
        st.next = n + 1
        if st.holes and st.timer is None:
            state = st

            def fire() -> None:
                state.timer = None
                if not state.holes or self._seq.get(channel) is not state:
                    return
                expected, received = state.first or (min(state.holes), state.next - 1)
                state.holes.clear()
                state.first = None
                self._spawn(
                    self._emit(
                        Event(type=SEQUENCE_GAP, channel=channel, data={"expected": expected, "received": received})
                    )
                )
                self._spawn(
                    self._emit(Event(type=RESYNC, channel=channel, data={"reason": "sequence_gap", "channel": channel}))
                )
                if channel == "balances":
                    for lb in list(self._live_balances):
                        lb._trigger("sequence_gap")

            st.timer = self._clock.call_later(self.reorder_window, fire)

    def _drop_private(self) -> List[str]:
        dropped = sorted(c for c in self._channels if c in PRIVATE_CHANNELS)
        self._channels.difference_update(dropped)
        self._pending_private.update(dropped)
        return dropped

    def _signed_out(self, reason: str, code: Optional[str] = None) -> None:
        """The server signed the connection out and ended every private subscription."""
        data: Dict[str, Any] = {"reason": reason, "previous_user_id": self.user_id, "user_id": None}
        if code is not None:
            data["code"] = code
        self.authenticated = False
        self.user_id = None
        for c in PRIVATE_CHANNELS:
            self._reset_seq(c)
        data["dropped"] = self._drop_private()
        for lb in list(self._live_balances):
            lb._on_auth_changed(reason)
        self._spawn(self._emit(Event(type=AUTH_CHANGED, data=data)))

    def _on_authenticated(self, user_id: Optional[str]) -> None:
        """A successful auth: detect an account switch, then restore pending private channels."""
        previous = self.user_id if self.authenticated else None
        self.authenticated = True
        self.user_id = user_id
        if previous is not None and user_id != previous:
            for c in PRIVATE_CHANNELS:
                self._reset_seq(c)
            for lb in list(self._live_balances):
                lb._on_auth_changed("user_changed")
            dropped = self._drop_private()
            data = {"reason": "user_changed", "previous_user_id": previous, "user_id": user_id, "dropped": dropped}
            self._spawn(self._emit(Event(type=AUTH_CHANGED, data=data)))
        if not self._pending_private:
            return
        channels = sorted(self._pending_private)
        self._pending_private.clear()
        self._channels.update(channels)
        self._spawn(self._resubscribe(channels))
        self._spawn(self._emit(Event(type=RESYNC, data={"reason": "reauth"})))

    async def _resubscribe(self, channels: List[str]) -> None:
        try:
            await self._request({"op": "subscribe", "channels": channels}, "subscribed")
        except WebSocketError as exc:
            if exc.code not in ("TIMEOUT", "DISCONNECTED", "NOT_CONNECTED"):
                # Refused by the server (e.g. signed out again meanwhile): back to pending.
                for c in channels:
                    if c in self._channels:
                        self._channels.discard(c)
                        self._pending_private.add(c)
            logger.warning("cexy.ws: private re-subscribe failed: %s", exc.code)

    async def _on_error(self, frame: Dict[str, Any]) -> None:
        code = str(frame.get("code"))
        logger.warning("cexy.ws: server error %s: %s", code, frame.get("message"))
        if code == "CONCURRENT_MODIFICATION" and frame.get("id") is None:
            # Messages were dropped: every book and channel may be out of date.
            await self._emit(Event(type=RESYNC, data={"reason": code}, raw=frame))
            for lb in list(self._live_balances):
                lb._trigger("concurrent_modification")
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


class AccountMismatchError(WebSocketError):
    """The snapshot source belongs to another account than the WebSocket session."""

    def __init__(self, websocket_user_id: str, snapshot_user_id: str) -> None:
        super().__init__(
            "ACCOUNT_MISMATCH",
            f"the snapshot source belongs to {snapshot_user_id}, the WebSocket to {websocket_user_id}; not merging",
        )
        self.websocket_user_id = websocket_user_id
        self.snapshot_user_id = snapshot_user_id


#: ``LiveBalances`` events (``lb.on(...)``): one asset changed (``data``: the row, or None when removed).
BALANCE_UPDATE = "update"
#: A snapshot was applied (``data``: ``{"reason": ...}``).
BALANCE_SNAPSHOT = "snapshot"
#: ``ACCOUNT_MISMATCH`` (nothing merged) or a failed snapshot or owner lookup (retried with backoff).
BALANCE_ERROR = "error"


class LiveBalances:
    """Live balances of the authenticated account. Created by ``WebSocketClient.live_balances()``.

    An event applies only if its ``data.sequence`` is greater than the stored one for that asset;
    a total of 0 removes the row (a snapshot row at or below that sequence cannot bring it back).
    A new snapshot is taken on a frame gap, ``balances.resync``, ``CONCURRENT_MODIFICATION``, a
    reconnect and after an account change, never because ``data.sequence`` skipped values. At
    the start and after every account change the snapshot source's owner is checked against the WebSocket user.
    """

    def __init__(
        self,
        ws: WebSocketClient,
        snapshot: Callable[[], Awaitable[List[Any]]],
        owner_id: Callable[[], Awaitable[str]],
        min_snapshot_interval: float,
        retry: float,
    ) -> None:
        self._ws = ws
        self._clock = ws._clock
        self._snapshot = snapshot
        self._owner_id = owner_id
        self._min_interval = min_snapshot_interval
        self._retry = retry
        #: True until the first snapshot, and from every refetch trigger until the next one is applied.
        self.stale = True
        #: The last error (also emitted as ``error``; the first owner check can fail before a handler
        #: is attached). ``AccountMismatchError`` (code ``ACCOUNT_MISMATCH``): nothing was merged.
        #: Cleared by the next snapshot.
        self.last_error: Optional[Exception] = None
        self._rows: Dict[str, Any] = {}
        self._tombstones: Dict[str, int] = {}
        self._buffer: List[Dict[str, Any]] = []
        self._verified_user: Optional[str] = None
        self._fetching = False
        self._again: Optional[str] = None
        self._last_success: Optional[float] = None
        self._timer: Any = None
        self._attempt = 0
        self._closed = False
        self._generation = 0
        self._warned_no_sequence = False
        self._handlers: Dict[str, List[Handler]] = collections.defaultdict(list)

    def on(self, event_type: str, handler: Handler) -> None:
        """``update``, ``snapshot`` or ``error``."""
        self._handlers[event_type].append(handler)

    def get(self, asset: str) -> Any:
        """A copy of one asset's balance, or None."""
        row = self._rows.get(asset)
        return row.model_copy() if row is not None else None

    def all(self) -> List[Any]:
        """Copies of every non-zero balance."""
        return [r.model_copy() for r in self._rows.values()]

    def close(self) -> None:
        """Stop following (unsubscribes ``balances`` unless something else on this socket needs it)."""
        if self._closed:
            return
        self._closed = True
        self._cancel_timer()
        self._ws._release_balances(self)

    # -- hooks called by the client ------------------------------------------------

    def _on_disconnect(self) -> None:
        self._mark_stale()

    def _on_auth_changed(self, reason: str) -> None:
        self._verified_user = None
        self._mark_stale()
        if reason == "user_changed":
            # Another account's balances must never show: forget everything until the owner check.
            self._rows.clear()
            self._tombstones.clear()

    def _on_event(self, data: Dict[str, Any]) -> None:
        if self._closed or not isinstance(data.get("asset"), str):
            return
        # Buffered only while a snapshot is in flight (it is applied on top). Without a verified owner
        # and no fetch (mismatch, retry backoff, signed out), events are dropped: the next snapshot
        # is complete anyway.
        if self._fetching:
            self._buffer.append(data)
            return
        if self._verified_user is None:
            return
        self._apply(data, emit=True)

    def _trigger(self, reason: str) -> None:
        if self._closed:
            return
        self.stale = True
        if self._fetching:
            if self._again is None:
                self._again = reason
            return
        if self._timer is not None:
            return
        wait = 0.0 if self._last_success is None else self._last_success + self._min_interval - self._clock.now()
        if wait > 0:

            def later() -> None:
                self._timer = None
                self._ws._spawn(self._fetch(reason))

            self._timer = self._clock.call_later(wait, later)
            return
        self._fetching = True  # claimed now, so triggers until the task runs coalesce
        self._ws._spawn(self._fetch(reason, claimed=True))

    # -- internals -----------------------------------------------------------------

    def _mark_stale(self) -> None:
        self.stale = True
        self._generation += 1
        self._buffer = []

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
        self._timer = None

    async def _fetch(self, reason: str, claimed: bool = False) -> None:
        if self._closed:
            self._fetching = False
            return
        if not claimed and self._fetching:
            if self._again is None:
                self._again = reason
            return
        ws_user = self._ws.user_id if self._ws.authenticated else None
        if ws_user is None:
            self._fetching = False  # signed out: the re-subscribe after the next auth triggers again
            return
        self._fetching = True
        self._buffer = []
        self._generation += 1
        gen = self._generation
        try:
            if self._verified_user != ws_user:
                owner = await self._owner_id()
                if self._closed or gen != self._generation:
                    return self._done()
                if owner != ws_user:
                    self._rows.clear()
                    self._tombstones.clear()
                    self._buffer = []  # events that arrived during the owner lookup
                    self._fetching = False
                    self._again = None
                    self.last_error = AccountMismatchError(ws_user, owner)
                    await self._emit(BALANCE_ERROR, self.last_error)
                    return
                self._verified_user = ws_user
            rows = await self._snapshot()
            if self._closed or gen != self._generation:
                return self._done()
            self._apply_snapshot(rows)
            self._attempt = 0
            self._last_success = self._clock.now()
            self.stale = False
            self.last_error = None
            self._fetching = False
            await self._emit(BALANCE_SNAPSHOT, {"reason": reason})
            again, self._again = self._again, None
            if again is not None:
                self._trigger(again)
        except Exception as exc:  # reported, then retried
            self._fetching = False
            if self._closed:
                return
            self.last_error = exc
            await self._emit(BALANCE_ERROR, exc)
            delay = min(30.0, self._retry * 2**self._attempt)
            self._attempt += 1
            self._cancel_timer()

            def retry() -> None:
                self._timer = None
                self._trigger("retry")

            self._timer = self._clock.call_later(delay, retry)

    def _done(self) -> None:
        self._fetching = False
        again, self._again = self._again, None
        if again is not None and not self._closed:
            self._trigger(again)

    def _apply_snapshot(self, rows: List[Any]) -> None:
        fresh: Dict[str, Any] = {}
        for r in rows:
            tomb = self._tombstones.get(r.asset)
            if tomb is not None:
                if r.sequence <= tomb:
                    continue
                del self._tombstones[r.asset]
            if Decimal(r.total) == 0:
                continue
            fresh[r.asset] = r
        self._rows = fresh
        buffered, self._buffer = self._buffer, []
        for d in buffered:
            self._apply(d, emit=False)

    def _apply(self, d: Dict[str, Any], emit: bool) -> None:
        asset = d["asset"]
        prev = self._rows.get(asset)
        current: int = (prev.sequence or 0) if prev is not None else self._tombstones.get(asset, -1)
        # A server that predates live balances sends no data.sequence: such an event always applies
        # and keeps the stored sequence (a later sequenced snapshot or event takes over).
        raw_seq: Any = d.get("sequence")
        sequenced = isinstance(raw_seq, int) and not isinstance(raw_seq, bool)
        if not sequenced and not self._warned_no_sequence:
            self._warned_no_sequence = True
            logger.warning("cexy.ws: balance.updated without data.sequence; applying every event in arrival order")
        seq: int = raw_seq if sequenced else max(current, 0)
        if sequenced and seq <= current:
            return  # duplicate or older
        if Decimal(str(d.get("total", "0"))) == 0:
            self._rows.pop(asset, None)
            if sequenced:
                self._tombstones[asset] = seq
            if emit:
                self._ws._spawn(self._emit(BALANCE_UPDATE, {"asset": asset, "balance": None}))
            return
        amounts: Dict[str, Decimal] = {
            k: Decimal(str(d[k])) for k in ("available", "locked", "pending", "total") if k in d
        }
        if prev is not None:
            update: Dict[str, Any] = {**amounts, "sequence": seq}
            row = prev.model_copy(update=update)
        else:
            row = m.BalanceResponse(
                asset=asset,
                held_incoming=[],
                available=amounts.get("available", Decimal(0)),
                locked=amounts.get("locked", Decimal(0)),
                pending=amounts.get("pending", Decimal(0)),
                total=amounts.get("total", Decimal(0)),
                sequence=seq,
            )
        self._rows[asset] = row
        self._tombstones.pop(asset, None)
        if emit:
            self._ws._spawn(self._emit(BALANCE_UPDATE, {"asset": asset, "balance": row.model_copy()}))

    async def _emit(self, event_type: str, data: Any) -> None:
        ev = Event(type=event_type, channel="balances", data=data)
        for handler in list(self._handlers.get(event_type, [])):
            try:
                result = handler(ev)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("cexy.ws: live balances handler failed")


def _log_task_error(task: asyncio.Task[Any]) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.warning("cexy.ws: background task failed: %r", task.exception())
