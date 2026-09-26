"""Exceptions.

Every API failure returns ``{"error": {"code", "message", "details", "fields",
"request_id", "retryable"}}``. It is raised as ``CexyApiError`` or one of its
subclasses, chosen by ``code`` (then by HTTP status for the transport-level classes).
Branch on ``err.code``, never on the message.

A code this SDK version does not know maps to the base ``CexyApiError``: new codes never
crash the client.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence, Type

from cexy._generated.models import ErrorCode
from cexy.auth import redact_value

#: Every code in errors.yaml (provisional until the served spec includes ErrorCode).
KNOWN_ERROR_CODES = frozenset(m.value for m in ErrorCode)


class CexyError(Exception):
    """Base class for every exception raised by this SDK."""


class ConfigurationError(CexyError, ValueError):
    """Invalid client configuration, detected locally before any request."""


class MissingCredentialsError(CexyError):
    """An API-key operation was called on a client built without a key pair."""


class CexyConnectionError(CexyError):
    """The request could not be completed at the network level (after retries)."""


class CexyApiError(CexyError):
    """An error response from the API."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int,
        details: Optional[Mapping[str, Any]] = None,
        fields: Optional[Mapping[str, str]] = None,
        request_id: Optional[str] = None,
        retryable: bool = False,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        super().__init__(f"{status} {code}: {message}" + (f" (request_id={request_id})" if request_id else ""))
        self.code = code
        self.message = message
        self.status = status
        self.details: Dict[str, Any] = dict(details or {})
        self.fields: Dict[str, str] = dict(fields or {})
        self.request_id = request_id
        self.retryable = retryable
        self.headers: Dict[str, str] = dict(headers or {})

    @property
    def is_known_code(self) -> bool:
        return self.code in KNOWN_ERROR_CODES

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(code={self.code!r}, status={self.status}, "
            f"message={self.message!r}, request_id={self.request_id!r}, retryable={self.retryable})"
        )


class AuthenticationError(CexyApiError):
    """401: missing, invalid or expired credentials."""


class ForbiddenError(CexyApiError):
    """403: the key lacks the scope (``FORBIDDEN``), or the route is session-only
    (``API_KEY_NOT_ALLOWED``), or the account/region is restricted."""


class ValidationError(CexyApiError):
    """400: the request failed validation. ``fields`` maps field names to problems."""


class NotFoundError(CexyApiError):
    """404: the resource does not exist."""


class ConflictError(CexyApiError):
    """409: conflicts such as ``ALREADY_EXISTS``, ``INVALID_STATE``,
    ``IDEMPOTENCY_KEY_CONFLICT`` or ``CONCURRENT_MODIFICATION``."""


class UnprocessableError(CexyApiError):
    """422: well-formed but refused, e.g. ``INSUFFICIENT_FUNDS``, ``MARKET_UNAVAILABLE``."""


class RateLimitError(CexyApiError):
    """429: too many requests. ``retry_after`` is the server's requested wait in seconds."""

    @property
    def retry_after(self) -> Optional[float]:
        return retry_after_seconds(self.headers, self.details)


class ServerError(CexyApiError):
    """5xx: server-side failure, maintenance or overload."""


_BY_CODE: Dict[str, Type[CexyApiError]] = {}
for _cls, _codes in {
    ValidationError: (
        "VALIDATION_FAILED",
        "MALFORMED_REQUEST",
        "INVALID_CURSOR",
        "PRECISION_EXCEEDED",
        "BELOW_MINIMUM",
        "ABOVE_MAXIMUM",
        "INVALID_ADDRESS",
        "MEMO_REQUIRED",
    ),
    AuthenticationError: (
        "UNAUTHENTICATED",
        "INVALID_CREDENTIALS",
        "TOKEN_EXPIRED",
        "SESSION_REVOKED",
    ),
    ForbiddenError: (
        "FORBIDDEN",
        "API_KEY_NOT_ALLOWED",
        "TWO_FACTOR_REQUIRED",
        "TWO_FACTOR_INVALID",
        "FRESH_TWO_FACTOR_REQUIRED",
        "FUTURES_RESTRICTED",
        "ACCOUNT_FROZEN",
        "ACCOUNT_ON_HOLD",
        "EMAIL_NOT_VERIFIED",
        "REGION_BLOCKED",
    ),
    NotFoundError: ("NOT_FOUND",),
    ConflictError: (
        "ALREADY_EXISTS",
        "INVALID_STATE",
        "IDEMPOTENCY_KEY_CONFLICT",
        "CONCURRENT_MODIFICATION",
        "WINDOW_OPEN",
        "EVIDENCE_CONTRADICTS",
        "AMOUNT_MISMATCH",
    ),
    UnprocessableError: (
        "INSUFFICIENT_FUNDS",
        "INSUFFICIENT_FEE_FUNDS",
        "MARKET_UNAVAILABLE",
        "DEPOSIT_DISABLED",
        "WITHDRAWAL_DISABLED",
        "SELF_TRADE_BLOCKED",
        "LIMIT_EXCEEDED",
    ),
    RateLimitError: ("RATE_LIMITED",),
    ServerError: ("INTERNAL", "SERVICE_UNAVAILABLE", "UNDER_MAINTENANCE", "ENGINE_OVERLOADED"),
}.items():
    for _code in _codes:
        _BY_CODE[_code] = _cls

_BY_STATUS: Dict[int, Type[CexyApiError]] = {
    400: ValidationError,
    401: AuthenticationError,
    403: ForbiddenError,
    404: NotFoundError,
    409: ConflictError,
    422: UnprocessableError,
    429: RateLimitError,
}


def error_class_for(code: str, status: int) -> Type[CexyApiError]:
    """Pick the exception class. Known code wins; an unknown code is the base class,
    except that 429 and 5xx keep their transport meaning (rate limit / server error)."""
    if code in _BY_CODE:
        return _BY_CODE[code]
    if code in KNOWN_ERROR_CODES:
        return _BY_STATUS.get(status, ServerError if status >= 500 else CexyApiError)
    if status == 429:
        return RateLimitError
    return CexyApiError


def retry_after_seconds(headers: Mapping[str, str], details: Mapping[str, Any]) -> Optional[float]:
    """The larger of the ``Retry-After`` header and ``details.retry_after_seconds``."""
    values = []
    for k, v in headers.items():
        if k.lower() == "retry-after":
            try:
                values.append(float(v))
            except ValueError:
                pass
    d = details.get("retry_after_seconds")
    if isinstance(d, (int, float)) and not isinstance(d, bool):
        values.append(float(d))
    return max(values) if values else None


def from_response(status: int, body: Any, headers: Mapping[str, str], secrets: Sequence[str] = ()) -> CexyApiError:
    """Build the exception for an error response. Never raises on a malformed body.

    The server's message, details, fields, request id and headers are kept, except that
    the configured ``secrets`` (the API key and secret) and anything shaped like a key id
    are replaced with ``***``, so an echoed credential never reaches logs or tracebacks.
    """
    body = redact_value(body, secrets)
    headers = redact_value(dict(headers), secrets)
    err = body.get("error") if isinstance(body, dict) else None
    if not isinstance(err, dict):
        code = "HTTP_%d" % status
        cls: Type[CexyApiError] = _BY_STATUS.get(status, ServerError if status >= 500 else CexyApiError)
        return cls(
            code,
            f"HTTP {status} without an error envelope",
            status=status,
            retryable=status in (429, 502, 503, 504),
            headers=headers,
        )
    code = str(err.get("code") or "HTTP_%d" % status)
    details = err.get("details") if isinstance(err.get("details"), dict) else {}
    fields = err.get("fields") if isinstance(err.get("fields"), dict) else {}
    rid = err.get("request_id")
    return error_class_for(code, status)(
        code,
        str(err.get("message") or ""),
        status=status,
        details=details,
        fields={str(k): str(v) for k, v in (fields or {}).items()},
        request_id=str(rid) if rid is not None else headers.get("x-request-id"),
        retryable=bool(err.get("retryable", False)),
        headers=headers,
    )
