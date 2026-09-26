from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List

import pytest

import cexy

ROOT = Path(__file__).resolve().parent.parent
# Vendored copy of cexy-api-spec/conformance (see tests/fixtures/SYNC.txt).
CONFORMANCE = ROOT / "tests" / "fixtures" / "conformance"
SPEC = ROOT / "spec" / "openapi.sdk.json"
BASE = "https://api.cexy.io"

KEY = "ak_test_key"
SECRET = "test_secret"


def load(rel: str) -> Any:
    return json.loads((CONFORMANCE / rel).read_text())


def spec() -> Any:
    return json.loads(SPEC.read_text())


class SleepRecorder:
    def __init__(self) -> None:
        self.calls: List[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


class AsyncSleepRecorder(SleepRecorder):
    async def __call__(self, seconds: float) -> None:  # type: ignore[override]
        self.calls.append(seconds)


@pytest.fixture
def sleeps() -> SleepRecorder:
    return SleepRecorder()


@pytest.fixture
def client(sleeps: SleepRecorder) -> cexy.Client:
    c = cexy.Client(api_key=KEY, api_secret=SECRET)
    c._transport._sleep = sleeps
    return c


@pytest.fixture
def public_client(sleeps: SleepRecorder) -> cexy.Client:
    c = cexy.Client()
    c._transport._sleep = sleeps
    return c


# Minimal valid payloads for response models.
ORDER = {
    "id": "o1",
    "symbol": "BTC/USDT",
    "side": "buy",
    "type": "limit",
    "time_in_force": "gtc",
    "status": "open",
    "quantity": "0.5",
    "filled_quantity": "0",
    "remaining_quantity": "0.5",
    "filled_quote_quantity": "0",
    "fee_paid": "0",
    "reserved_remaining": "30000.00",
    "created_at": "2026-09-10T11:42:31Z",
    "updated_at": "2026-09-10T11:42:31Z",
    "price": "60000.00",
}
BALANCE = {"asset": "BTC", "available": "1.50000000", "locked": "0.1", "pending": "0", "total": "1.6"}
LEDGER = {
    "id": "l1",
    "asset": "BTC",
    "kind": "deposit",
    "available_delta": "1",
    "locked_delta": "0",
    "pending_delta": "0",
    "available_after": "1",
    "sequence": 1,
    "reference": {"deposit_id": "d1"},
    "created_at": "2026-09-10T11:42:31Z",
}
