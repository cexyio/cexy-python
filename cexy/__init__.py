"""CEXY.io Python SDK.

>>> import cexy
>>> client = cexy.Client()                       # public market data
>>> client.markets.orderbook("BTC/USDT", depth=10)

Private endpoints need an API key pair: ``cexy.Client(api_key=..., api_secret=...)``.
"""

from cexy._async.client import AsyncClient
from cexy._async.pagination import AsyncPage
from cexy._cancel_all import CancelAllResult
from cexy._common import DEFAULT_BASE_URL, USER_AGENT
from cexy._sync.client import Client
from cexy._sync.pagination import Page
from cexy._version import __version__
from cexy.auth import Authenticator, HeaderKeyAuth
from cexy.errors import (
    AuthenticationError,
    CexyApiError,
    CexyConnectionError,
    CexyError,
    ConfigurationError,
    ConflictError,
    ForbiddenError,
    JurisdictionBlockedError,
    MissingCredentialsError,
    NotFoundError,
    PagingStalledError,
    RateLimitError,
    ServerError,
    UnprocessableError,
    ValidationError,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "USER_AGENT",
    "AsyncClient",
    "AsyncPage",
    "AuthenticationError",
    "Authenticator",
    "CancelAllResult",
    "CexyApiError",
    "CexyConnectionError",
    "CexyError",
    "Client",
    "ConfigurationError",
    "ConflictError",
    "ForbiddenError",
    "JurisdictionBlockedError",
    "HeaderKeyAuth",
    "MissingCredentialsError",
    "NotFoundError",
    "Page",
    "PagingStalledError",
    "RateLimitError",
    "ServerError",
    "UnprocessableError",
    "ValidationError",
    "__version__",
]
