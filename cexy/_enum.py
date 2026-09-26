"""Enum base that tolerates values added by the server after this SDK was built."""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional


class OpenEnum(str, Enum):
    """A ``str`` enum whose unknown values parse to a pseudo-member named ``UNKNOWN``.

    ``OrderStatus("some_new_status")`` returns a member with ``.name == "UNKNOWN"`` and
    ``.value == "some_new_status"`` instead of raising, so a new server value never
    breaks response parsing. Compare with ``member.is_unknown``.
    """

    @classmethod
    def _missing_(cls, value: object) -> Optional[Any]:
        if not isinstance(value, str):
            return None
        member = str.__new__(cls, value)
        member._name_ = "UNKNOWN"
        member._value_ = value
        return member

    @property
    def is_unknown(self) -> bool:
        return self._name_ == "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
