"""Cursor pagination: ``items`` + ``next_cursor`` + ``has_more``.

Async source; the synchronous ``cexy/_sync/pagination.py`` is generated from it by ``scripts/unasync.py``.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class AsyncPage(Generic[T]):
    """One page of results.

    ``async for item in page.auto_paging_iter()`` walks this page and every later one,
    fetching lazily. ``await page.next_page()`` fetches just the next page.
    """

    def __init__(
        self,
        items: List[T],
        next_cursor: Optional[str],
        has_more: bool,
        fetch: Callable[[str], Any],
    ) -> None:
        self.items = items
        self.next_cursor = next_cursor
        self.has_more = has_more
        self._fetch = fetch

    @classmethod
    def parse(cls, payload: Any, model: Type[T], fetch: Callable[[str], Any]) -> AsyncPage[T]:
        items = [model.model_validate(x) for x in payload.get("items", [])]
        cursor = payload.get("next_cursor")
        has_more = bool(payload.get("has_more", False)) and bool(cursor)
        return cls(items, cursor, has_more, fetch)

    async def next_page(self) -> Optional[AsyncPage[T]]:
        if not self.has_more or not self.next_cursor:
            return None
        page: AsyncPage[T] = await self._fetch(self.next_cursor)
        return page

    async def auto_paging_iter(self) -> AsyncIterator[T]:
        page: Optional[AsyncPage[T]] = self
        while page is not None:
            for item in page.items:
                yield item
            page = await page.next_page()

    def __len__(self) -> int:
        return len(self.items)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(items={len(self.items)}, has_more={self.has_more})"
