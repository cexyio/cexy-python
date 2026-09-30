"""Shared, I/O-free request building used by the sync and async transports."""

from __future__ import annotations

import json
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote, urlsplit

from cexy._generated.operations import OPERATIONS, Operation
from cexy._version import __version__
from cexy.errors import ConfigurationError

DEFAULT_BASE_URL = "https://api.cexy.io"
USER_AGENT = f"cexy-python/{__version__}"

#: Client-side limiter defaults, below the server's limits (about 120/min per IP for
#: anonymous requests, about 600/min per API key, configurable server-side).
DEFAULT_RATE_LIMIT_ANONYMOUS = 100
DEFAULT_RATE_LIMIT_WITH_KEY = 300

#: Sentinel meaning "generate an Idempotency-Key for this call".
AUTO = "auto"


LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def check_scheme(url: str, secure: str, insecure: str, allow_insecure: bool) -> None:
    """Require ``secure`` (https/wss). ``insecure`` (http/ws) is allowed only with an explicit
    ``allow_insecure=True`` and only for a loopback host (local testing)."""
    parts = urlsplit(url)
    if not parts.hostname:
        raise ConfigurationError(f"invalid URL: {url!r}")
    if parts.scheme == secure:
        return
    if parts.scheme == insecure and allow_insecure and parts.hostname in LOOPBACK_HOSTS:
        return
    if parts.scheme == insecure and allow_insecure:
        raise ConfigurationError(f"{insecure}:// is only allowed for localhost, 127.0.0.1 or ::1")
    raise ConfigurationError(f"URL must use {secure}:// (got {parts.scheme or 'no scheme'!r})")


def validate_base_url(base_url: str, allow_insecure: bool = False) -> str:
    check_scheme(base_url, "https", "http", allow_insecure)
    parts = urlsplit(base_url)
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ConfigurationError("base_url must not contain credentials, a query or a fragment")
    return base_url.rstrip("/")


def user_agent(suffix: Optional[str]) -> str:
    if suffix is None:
        return USER_AGENT
    if any(c in suffix for c in "\r\n"):
        raise ConfigurationError("user_agent_suffix must not contain line breaks")
    return f"{USER_AGENT} {suffix}"


def operation(op_id: str) -> Operation:
    return OPERATIONS[op_id]


def build_path(op: Operation, path_params: Optional[Mapping[str, str]]) -> str:
    path = op.path
    for name in op.path_params:
        value = (path_params or {}).get(name)
        if value is None or value == "":
            raise ValueError(f"{name} is required")
        # "." and ".." would be dot segments: the URL layer resolves them (even as %2E), so the
        # request would silently go to a different route.
        if value in (".", ".."):
            raise ValueError(f'{name} must not be "." or ".."')
        path = path.replace("{" + name + "}", quote(str(value), safe=""))
    return path


def build_query_string(op: Operation, query: Optional[Mapping[str, Any]]) -> str:
    """The query string the SDK sends (RFC 3986: ``%20`` for a space, ``%2B`` for a plus), built
    here rather than by httpx (which writes ``+`` for a space), so a signed request is exactly the
    request that is sent."""
    from cexy.auth import encode_component

    return "&".join(f"{encode_component(k)}={encode_component(v)}" for k, v in build_query(op, query).items())


def _query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("datetimes must be timezone-aware")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        raise TypeError("floats are not accepted in query parameters")
    return str(value)


def build_query(op: Operation, query: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, value in (query or {}).items():
        if key not in op.query_params:
            raise ValueError(f"{op.operation_id} has no query parameter {key!r}")
        if value is not None:
            out[key] = _query_value(value)
    return out


def encode_body(body: Optional[Mapping[str, Any]]) -> Optional[bytes]:
    if body is None:
        return None
    return json.dumps({k: v for k, v in body.items() if v is not None}, separators=(",", ":")).encode()


def lower_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}
