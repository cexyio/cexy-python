"""Marks facade methods with the API operation they call (used by the coverage test)."""

from __future__ import annotations

from typing import Callable, TypeVar

F = TypeVar("F", bound=Callable[..., object])


def operation(op_id: str) -> Callable[[F], F]:
    def mark(fn: F) -> F:
        fn._cexy_operation = op_id  # type: ignore[attr-defined]
        return fn

    return mark
