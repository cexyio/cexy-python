"""The facade exposes exactly the 51 allowlisted operations in spec/openapi.sdk.json."""

from __future__ import annotations

import inspect
import re
from typing import Any, Dict, Set

import httpx
import pytest
import respx

import cexy
from cexy._generated.operations import OPERATIONS
from tests.conftest import BASE, KEY, SECRET, spec

SPEC = spec()
SPEC_OPS = {
    op["operationId"]: (method.upper(), path, op)
    for path, item in SPEC["paths"].items()
    for method, op in item.items()
    if method in {"get", "post", "put", "patch", "delete"}
}


def facade_methods(client: Any) -> Dict[str, Any]:
    found: Dict[str, Any] = {}
    targets = [client] + [v for v in vars(client).values() if hasattr(v, "_t")]
    for obj in targets:
        for _name, fn in inspect.getmembers(obj, callable):
            op = getattr(fn, "_cexy_operation", None)
            if op is not None:
                assert op not in found, f"{op} exposed twice"
                found[op] = fn
    return found


def test_spec_has_51_operations() -> None:
    assert len(SPEC_OPS) == 51


@pytest.mark.parametrize("cls", [cexy.Client, cexy.AsyncClient])
def test_facade_covers_exactly_the_spec(cls: Any) -> None:
    methods = facade_methods(cls(api_key=KEY, api_secret=SECRET))
    assert set(methods) == set(SPEC_OPS)


def test_operation_table_matches_spec() -> None:
    assert set(OPERATIONS) == set(SPEC_OPS)
    for op_id, (method, path, op) in SPEC_OPS.items():
        gen = OPERATIONS[op_id]
        assert (gen.method, gen.path) == (method, path)
        assert gen.scope == op.get("x-required-scope", "-")
        assert gen.auth == ("api_key" if op.get("security") else "none")
        assert gen.scope in ("-", "read", "trade")
        assert (gen.auth == "none") == (gen.scope == "-")


def test_every_schema_is_generated() -> None:
    from cexy._generated import models

    # Scalar aliases are inlined by the generator rather than emitted as classes: Amount (a
    # decimal string, which becomes decimal.Decimal) and DetailValue (string | integer | boolean).
    inlined = {"Amount", "DetailValue"}
    missing = [n for n in SPEC["components"]["schemas"] if n not in inlined and not hasattr(models, n)]
    assert missing == []


# Dummy values for required arguments, by parameter name.
ARGS: Dict[str, Any] = {
    "symbol": "BTC/USDT",
    "asset": "BTC",
    "network": "bitcoin-mainnet",
    "interval": "1h",
    "side": "buy",
    "type": "limit",
    "base_amount": "1",
    "quote_amount": "1",
    "shares": "1",
    "deposit_id": "d1",
    "withdrawal_id": "w1",
    "order_id": "o1",
    "client_order_id": "c1",
    "id": "sub_1",
    "coin": "BTC",
}


def _path_regex(path: str) -> str:
    return "^" + BASE + re.sub(r"\{[^}]+\}", "[^/]+", path) + r"(\?.*)?$"


@pytest.mark.parametrize("op_id", sorted(SPEC_OPS))
def test_each_method_calls_its_operation(op_id: str) -> None:
    client = cexy.Client(api_key=KEY, api_secret=SECRET, max_retries=0)
    fn = facade_methods(client)[op_id]
    method, path, op = SPEC_OPS[op_id]
    kwargs = {}
    for name, p in inspect.signature(fn).parameters.items():
        if p.default is inspect.Parameter.empty:
            kwargs[name] = ARGS[name]
    seen: Set[str] = set()
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(side_effect=lambda req: (seen.add(f"{req.method} {req.url}"), httpx.Response(599))[1])
        with pytest.raises(cexy.CexyApiError):
            fn(**kwargs)
    assert len(seen) == 1
    sent_method, sent_url = next(iter(seen)).split(" ", 1)
    assert sent_method == method
    assert re.match(_path_regex(path), sent_url), (sent_url, path)
    for p in op.get("parameters", []):
        if p["in"] == "query" and p.get("required"):
            assert p["name"] + "=" in sent_url


def test_sync_and_async_have_same_signatures() -> None:
    s = facade_methods(cexy.Client(api_key=KEY, api_secret=SECRET))
    a = facade_methods(cexy.AsyncClient(api_key=KEY, api_secret=SECRET))
    for op_id in s:
        assert inspect.iscoroutinefunction(a[op_id]) and not inspect.iscoroutinefunction(s[op_id])
        assert list(inspect.signature(s[op_id]).parameters) == list(inspect.signature(a[op_id]).parameters)
        assert s[op_id].__name__ == a[op_id].__name__


def test_deposit_address_documents_side_effect() -> None:
    doc = cexy.Client().wallet.deposit_address.__doc__ or ""
    assert "CREATES" in doc
