"""Authentication.

The ``Authenticator`` protocol is the extension point for request authentication.
Today CEXY API keys are two static headers (``X-API-Key`` and ``X-API-Secret``), which
``HeaderKeyAuth`` implements. HMAC request signing is planned before SDK 1.0: it will
ship as another ``Authenticator`` (it receives the method, URL and body, so it can sign
them) and ``Client`` will select it without changing the public resource API.

The SDK never sends ``Authorization`` headers and has no bearer/session support.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, MutableMapping, Optional, Protocol, Sequence, runtime_checkable

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


SENSITIVE_HEADERS = frozenset({"x-api-key", "x-api-secret", "authorization"})


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


def build_authenticator(api_key: Optional[str], api_secret: Optional[str]) -> Optional[HeaderKeyAuth]:
    """Validate the key pair locally. Exactly both or neither must be supplied."""
    if bool(api_key) != bool(api_secret):
        raise ValueError("api_key and api_secret must be supplied together (got only one of them)")
    if not api_key or not api_secret:
        return None
    return HeaderKeyAuth(api_key, api_secret)
