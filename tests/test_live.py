"""Opt-in smoke tests against the public, unauthenticated API on api.cexy.io.

Skipped unless CEXY_LIVE_TESTS=1. Public GET endpoints only, no credentials, the SDK's
own User-Agent, three requests in total.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest

import cexy
import cexy.models

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("CEXY_LIVE_TESTS") != "1", reason="set CEXY_LIVE_TESTS=1 to run"),
]


@pytest.fixture(scope="module")
def client():  # type: ignore[no-untyped-def]
    with cexy.Client(max_retries=1) as c:  # never any credentials
        assert not c.has_credentials
        yield c


@pytest.fixture(scope="module")
def markets(client: cexy.Client):  # type: ignore[no-untyped-def]
    return client.markets.list()


def test_time(client: cexy.Client) -> None:
    t = client.time()
    assert t.epoch_ms > 1_700_000_000_000


def test_markets_list(markets) -> None:  # type: ignore[no-untyped-def]
    assert markets, "expected at least one market"
    assert all(isinstance(m.last_price, Decimal) for m in markets)


def test_orderbook(client: cexy.Client, markets) -> None:  # type: ignore[no-untyped-def]
    active = [m for m in markets if m.status == cexy.models.MarketStatus.ACTIVE] or markets
    book = client.markets.orderbook(active[0].symbol, depth=5)
    assert book.symbol == active[0].symbol
    assert book.sequence >= 0
    assert len(book.bids) <= 5 and len(book.asks) <= 5
    for price, qty in [*book.bids, *book.asks]:
        assert isinstance(price, Decimal) and isinstance(qty, Decimal)
