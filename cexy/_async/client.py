"""The async client (``AsyncClient``).

Async source; the synchronous ``cexy/_sync/client.py`` is generated from it by ``scripts/unasync.py``.
"""

from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

import httpx

from cexy._async.resources import (
    AsyncAccount,
    AsyncAssets,
    AsyncExports,
    AsyncFees,
    AsyncMarkets,
    AsyncNetworks,
    AsyncPools,
    AsyncTrading,
    AsyncWallet,
)
from cexy._async.transport import AsyncTransport
from cexy._common import (
    DEFAULT_BASE_URL,
    DEFAULT_RATE_LIMIT_ANONYMOUS,
    DEFAULT_RATE_LIMIT_WITH_KEY,
    user_agent,
    validate_base_url,
)
from cexy._generated import models as m
from cexy._opmap import operation
from cexy.auth import REDACTED, Authenticator, build_authenticator
from cexy.errors import ConfigurationError


class AsyncClient:
    """CEXY.io REST client (asyncio).

    Public market data needs no credentials. For account and trading endpoints pass an
    API key pair; both values are required together. Keys are sent only on endpoints that
    need them, only as ``X-API-Key``/``X-API-Secret`` headers, and are redacted from
    ``repr``, logs and exceptions.

    The client-side rate limiter defaults to 100 requests/minute without a key and 300 with
    one (``rate_limit_per_minute`` overrides it); it adapts to ``X-RateLimit-*`` headers.

    ``base_url`` must be ``https://``. ``allow_insecure=True`` permits ``http://`` for a
    loopback host only (local testing).

    Use as ``async with AsyncClient() as client: ...`` or call ``await client.aclose()``.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 10,
        max_retries: int = 3,
        user_agent_suffix: Optional[str] = None,
        *,
        rate_limit_per_minute: Optional[float] = None,
        authenticator: Optional[Authenticator] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        allow_insecure: bool = False,
        auth: str = "hmac",
    ) -> None:
        """``auth``: how ``api_key``/``api_secret`` authenticate. ``"hmac"`` (default): every
        private request is signed and the secret never leaves the process; a key issued before
        signing existed fails with ``KEY_NOT_SIGNABLE`` (no fallback). ``"headers"``: the
        ``X-API-Key`` and ``X-API-Secret`` headers, which the API is switching off
        (``SIGNATURE_REQUIRED``); kept only for servers that still accept it."""
        mode = auth
        auth_obj: Optional[Authenticator]
        try:
            auth_obj = build_authenticator(api_key, api_secret, mode)
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from None
        if authenticator is not None:
            if auth_obj is not None:
                raise ConfigurationError("pass either api_key/api_secret or authenticator, not both")
            if not isinstance(authenticator, Authenticator):
                raise ConfigurationError("authenticator must implement cexy.auth.Authenticator")
            auth_obj = authenticator
        self._auth = auth_obj
        if rate_limit_per_minute is None:
            # Server limits: ~120/min per IP for anonymous requests, ~600/min per key.
            rate_limit_per_minute = (
                DEFAULT_RATE_LIMIT_WITH_KEY if auth_obj is not None else DEFAULT_RATE_LIMIT_ANONYMOUS
            )
        self._transport = AsyncTransport(
            base_url=validate_base_url(base_url, allow_insecure),
            auth=auth_obj,
            timeout=timeout,
            max_retries=max_retries,
            user_agent=user_agent(user_agent_suffix),
            rate_limit_per_minute=rate_limit_per_minute,
            http_client=http_client,
        )
        self.markets = AsyncMarkets(self._transport)
        self.assets = AsyncAssets(self._transport)
        self.networks = AsyncNetworks(self._transport)
        self.fees = AsyncFees(self._transport)
        self.pools = AsyncPools(self._transport)
        self.account = AsyncAccount(self._transport)
        self.exports = AsyncExports(self._transport)
        self.wallet = AsyncWallet(self._transport)
        self.trading = AsyncTrading(self._transport)

    @property
    def base_url(self) -> str:
        return self._transport.base_url

    @property
    def has_credentials(self) -> bool:
        return self._auth is not None

    @operation("server_time")
    async def time(self) -> m.ServerTimeResponse:
        """The server clock (public)."""
        payload = await self._transport.request("server_time")
        return m.ServerTimeResponse.model_validate(payload["data"])

    @operation("exchange_config")
    async def config(self) -> m.ExchangeConfigResponse:
        """Public exchange configuration: maintenance mode, page sizes, intervals (public)."""
        payload = await self._transport.request("exchange_config")
        return m.ExchangeConfigResponse.model_validate(payload["data"])

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        auth = REDACTED if self._auth is not None else None
        return f"{type(self).__name__}(base_url={self.base_url!r}, credentials={auth!r})"

    __str__ = __repr__

    def __reduce__(self) -> str:
        raise TypeError(f"{type(self).__name__} cannot be pickled (it may hold a secret)")
