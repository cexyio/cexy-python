"""Authentication.

The ``Authenticator`` protocol is the extension point for request authentication.
Two schemes ship: ``HmacAuth`` signs every request (the default, ``auth="hmac"``), and
``HeaderKeyAuth`` sends the static ``X-API-Key`` and ``X-API-Secret`` headers (``auth="headers"``,
which the API is switching off with ``SIGNATURE_REQUIRED``). ``Client`` selects one without
changing the public resource API.

The SDK never sends ``Authorization`` headers and has no bearer/session support.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import math
import re
import secrets as _secrets
import time
from typing import Any, Callable, Mapping, MutableMapping, Optional, Protocol, Sequence, runtime_checkable
from urllib.parse import unquote_to_bytes, urlsplit

REDACTED = "***"


@runtime_checkable
class Authenticator(Protocol):
    """Adds credentials to an outgoing request.

    ``apply`` is called once per attempt (so a signature can include a fresh timestamp),
    only for operations that require an API key. It must mutate ``headers`` in place and
    must never put credentials in the URL.
    """

    def apply(self, method: str, url: str, headers: MutableMapping[str, str], body: Optional[bytes]) -> None:
        """Add authentication to ``headers``."""
        ...

    def secrets(self) -> tuple[str, ...]:
        """Values that must never appear in logs, reprs or exception messages."""
        ...


class HeaderKeyAuth:
    """Static API-key headers: ``X-API-Key`` and ``X-API-Secret`` on every private request."""

    __slots__ = ("_key", "_secret")

    def __init__(self, api_key: str, api_secret: str) -> None:
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret must both be non-empty")
        self._key = api_key
        self._secret = api_secret

    @property
    def api_key_hint(self) -> str:
        """A safe-to-log hint: the first 6 characters of the key id."""
        return self._key[:6] + "…"

    def apply(self, method: str, url: str, headers: MutableMapping[str, str], body: Optional[bytes]) -> None:
        headers["X-API-Key"] = self._key
        headers["X-API-Secret"] = self._secret

    def secrets(self) -> tuple[str, ...]:
        return (self._key, self._secret)

    def __repr__(self) -> str:
        return f"HeaderKeyAuth(api_key={REDACTED!r}, api_secret={REDACTED!r})"

    __str__ = __repr__

    def __reduce__(self) -> str:
        raise TypeError("HeaderKeyAuth cannot be pickled (it holds a secret)")


#: The signing scheme (accepted by the API since 2026-10-01).
SIGNING_SCHEME = "CEXY-HMAC-SHA256-v1"
#: The furthest the client clock may be corrected after ``SIGNATURE_EXPIRED``.
MAX_CLOCK_OFFSET_MS = 3_600_000

_UNRESERVED = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")


def _encode_bytes(data: bytes) -> str:
    """RFC 3986: unreserved bytes kept, everything else ``%XX`` with uppercase hex."""
    return "".join(chr(b) if b in _UNRESERVED else "%%%02X" % b for b in data)


def encode_component(value: str) -> str:
    """RFC 3986 encoding of a string (UTF-8), for the paths and queries the SDK sends."""
    return _encode_bytes(value.encode("utf-8"))


def canonical_path(path: str) -> str:
    """Split on "/" BEFORE decoding; each segment percent-decoded, then re-encoded."""
    return "/".join(_encode_bytes(unquote_to_bytes(seg)) for seg in path.split("/"))


def canonical_query(query: str) -> str:
    """Split on "&"; decode and re-encode names and values; sort bytewise by name, then value."""
    # ``query`` is everything after the first "?" (already split off): a further "?" is data.
    if not query:
        return ""
    pairs = []
    for part in query.split("&"):
        if not part:
            continue  # empty parts are dropped: "a=1&&b=2&" is "a=1&b=2"
        name, _, value = part.partition("=")
        pairs.append((_encode_bytes(unquote_to_bytes(name)), _encode_bytes(unquote_to_bytes(value))))
    pairs.sort(key=lambda p: (p[0].encode(), p[1].encode()))
    return "&".join(f"{n}={v}" for n, v in pairs)


def canonical_request(method: str, path: str, query: str, timestamp: str, nonce: str, body: Optional[bytes]) -> str:
    return "\n".join(
        [
            SIGNING_SCHEME,
            method.upper(),
            canonical_path(path),
            canonical_query(query),
            timestamp,
            nonce,
            hashlib.sha256(body or b"").hexdigest(),
        ]
    )


def new_nonce() -> str:
    """16 bytes from the OS CSPRNG, base64url without padding (22 characters)."""
    return base64.urlsafe_b64encode(_secrets.token_bytes(16)).rstrip(b"=").decode()


class HmacAuth:
    """Signs every private request (see ``SIGNING_SCHEME``).

    Only ``X-API-Key``, ``X-API-Timestamp``, ``X-API-Nonce`` and ``X-API-Signature`` are sent; the
    secret never leaves the process. ``apply`` runs once per attempt, so every retry is signed with
    a fresh timestamp and nonce. The HMAC key is the UTF-8 bytes of the secret string as issued.
    """

    __slots__ = ("_key", "_nonce", "_now", "_secret", "clock_offset_ms")

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        now_ms: Optional[Callable[[], int]] = None,
        nonce: Optional[Callable[[], str]] = None,
    ) -> None:
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret must both be non-empty")
        self._key = api_key
        self._secret = api_secret
        self._now = now_ms or (lambda: time.time_ns() // 1_000_000)
        self._nonce = nonce or new_nonce
        #: The correction applied to the local clock after ``SIGNATURE_EXPIRED`` (diagnostics).
        self.clock_offset_ms = 0

    @property
    def api_key_hint(self) -> str:
        return self._key[:6] + "…"

    def adjust_clock(self, server_time_ms: float) -> bool:
        """Adopt the server clock; False (nothing changed) beyond ``MAX_CLOCK_OFFSET_MS``."""
        if not math.isfinite(server_time_ms):
            return False
        offset = round(server_time_ms - self._now())
        if abs(offset) > MAX_CLOCK_OFFSET_MS:
            return False
        self.clock_offset_ms = offset
        return True

    def _sign(self, message: str) -> str:
        return hmac.new(self._secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

    def apply(self, method: str, url: str, headers: MutableMapping[str, str], body: Optional[bytes]) -> None:
        parts = urlsplit(url)
        timestamp = str(self._now() + self.clock_offset_ms)
        nonce = self._nonce()
        canonical = canonical_request(method, parts.path, parts.query, timestamp, nonce, body)
        headers.pop("X-API-Secret", None)
        headers["X-API-Key"] = self._key
        headers["X-API-Timestamp"] = timestamp
        headers["X-API-Nonce"] = nonce
        headers["X-API-Signature"] = self._sign(canonical)

    def sign_websocket_challenge(self, connection_id: str, challenge: str) -> tuple[str, str]:
        """``(key_id, signature)`` for a WebSocket ``auth_key``."""
        return self._key, self._sign(f"CEXY-WS-AUTH-v1\n{connection_id}\n{challenge}")

    def secrets(self) -> tuple[str, ...]:
        return (self._key, self._secret)

    def __repr__(self) -> str:
        return f"HmacAuth(api_key={REDACTED!r}, api_secret={REDACTED!r})"

    __str__ = __repr__

    def __reduce__(self) -> str:
        raise TypeError("HmacAuth cannot be pickled (it holds a secret)")


SENSITIVE_HEADERS = frozenset({"x-api-key", "x-api-secret", "x-api-signature", "authorization"})


def redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {k: (REDACTED if k.lower() in SENSITIVE_HEADERS else v) for k, v in headers.items()}


#: Anything shaped like a CEXY API key id, redacted even when it is not the configured key.
_KEY_ID_PATTERN = re.compile(r"ak_[A-Za-z0-9]{16,}")


def redact_text(text: str, secrets: Sequence[str] = ()) -> str:
    """Replace the configured secrets, and anything shaped like a key id, with ``***``."""
    for s in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(s, REDACTED)
    return _KEY_ID_PATTERN.sub(REDACTED, text)


def redact_value(value: Any, secrets: Sequence[str] = ()) -> Any:
    """``redact_text`` applied to every string inside nested dicts, lists and tuples
    (keys included)."""
    if isinstance(value, str):
        return redact_text(value, secrets)
    if isinstance(value, dict):
        return {redact_value(k, secrets): redact_value(v, secrets) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(redact_value(v, secrets) for v in value)
    return value


def build_authenticator(
    api_key: Optional[str], api_secret: Optional[str], mode: str = "hmac"
) -> Optional[Authenticator]:
    """Validate the key pair locally. Exactly both or neither must be supplied.

    ``mode``: ``"hmac"`` (default, request signing) or ``"headers"`` (``X-API-Key`` +
    ``X-API-Secret``, which the API is switching off)."""
    if mode not in ("headers", "hmac"):
        raise ValueError('auth must be "headers" or "hmac"')
    if bool(api_key) != bool(api_secret):
        raise ValueError("api_key and api_secret must be supplied together (got only one of them)")
    if not api_key or not api_secret:
        return None
    return HmacAuth(api_key, api_secret) if mode == "hmac" else HeaderKeyAuth(api_key, api_secret)
