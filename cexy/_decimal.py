"""Decimal helpers. Every amount on the wire is a decimal string, never a JSON number."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Optional, Union

DecimalLike = Union[Decimal, str, int]


def to_wire(value: DecimalLike, field: str = "amount") -> str:
    """Convert an amount to its wire form (a plain decimal string).

    Accepts ``Decimal``, ``str`` or ``int``. A ``float`` raises ``TypeError`` locally:
    a binary float cannot represent most decimal amounts exactly (``0.1``), so sending
    one would silently change the amount.
    """
    if isinstance(value, bool):
        raise TypeError(f"{field}: expected Decimal, str or int, got bool")
    if isinstance(value, float):
        raise TypeError(
            f"{field}: floats are not accepted for amounts (precision loss); pass a Decimal or a string such as '0.1'"
        )
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        try:
            parsed = Decimal(value)
        except InvalidOperation:
            raise ValueError(f"{field}: {value!r} is not a decimal number") from None
    elif isinstance(value, Decimal):
        parsed = value
    else:
        raise TypeError(f"{field}: expected Decimal, str or int, got {type(value).__name__}")
    if not parsed.is_finite():
        raise ValueError(f"{field}: amount must be finite")
    return format(parsed, "f")


def to_wire_opt(value: Optional[DecimalLike], field: str) -> Optional[str]:
    return None if value is None else to_wire(value, field)
