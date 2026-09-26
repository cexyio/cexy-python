"""conformance/auth.json: header rules, redaction, local key-pair validation, error mapping."""

from __future__ import annotations

import logging
import re

import httpx
import pytest
import respx

import cexy
from tests.conftest import BALANCE, BASE, KEY, SECRET, load

AUTH = load("auth.json")
CASES = {c["id"]: c for c in AUTH["cases"]}
ERROR_CLASS = {
    "AuthenticationError": cexy.AuthenticationError,
    "PermissionError": cexy.ForbiddenError,  # the SDK's name for "permission denied"
    "CexyApiError": cexy.CexyApiError,
}
HANDLED = {
    "key_headers_on_private",
    "no_credentials_on_public",
    "never_in_url",
    "redaction",
    "half_pair_rejected_locally",
    "user_agent",
}


def test_every_case_is_handled() -> None:
    # A new upstream case must get an implementation here.
    assert set(CASES) == HANDLED


def _route(op: str) -> tuple[str, str]:
    method, path = op.split(" ", 1)
    return method, BASE + path


def _call(client: cexy.Client, op: str) -> None:
    calls = {
        "GET /api/v1/account/balances": client.account.balances,
        "GET /api/v1/markets": client.markets.list,
        "GET /api/v1/time": client.time,
    }
    calls[op]()


@respx.mock
def test_key_headers_on_private(client: cexy.Client) -> None:
    case = CASES["key_headers_on_private"]
    method, url = _route(case["request"]["operation"])
    route = respx.route(method=method, url=url).respond(json={"data": [BALANCE]})
    _call(client, case["request"]["operation"])
    headers = route.calls.last.request.headers
    for h in case["expect"]["headers_present"]:
        assert h in headers
    for h in case["expect"]["headers_absent"]:
        assert h not in headers
    assert headers["X-API-Key"] == KEY and headers["X-API-Secret"] == SECRET


@respx.mock
def test_no_credentials_on_public(client: cexy.Client) -> None:
    # Even a client that HAS keys must not send them to public endpoints.
    case = CASES["no_credentials_on_public"]
    method, url = _route(case["request"]["operation"])
    route = respx.route(method=method, url=url).respond(json={"data": []})
    _call(client, case["request"]["operation"])
    headers = route.calls.last.request.headers
    for h in case["expect"]["headers_absent"]:
        assert h not in headers


@respx.mock
def test_never_in_url(client: cexy.Client) -> None:
    case = CASES["never_in_url"]
    method, url = _route(case["request"]["operation"])
    route = respx.route(method=method, url=url).respond(json={"data": [BALANCE]})
    _call(client, case["request"]["operation"])
    sent = str(route.calls.last.request.url)
    for s in case["expect"]["url_must_not_contain"]:
        assert s not in sent


@respx.mock
def test_redaction(client: cexy.Client, caplog: pytest.LogCaptureFixture) -> None:
    case = CASES["redaction"]
    assert case["expect"]["secret_not_in"]
    method, url = _route(case["request"]["operation"])
    respx.route(method=method, url=url).mock(
        side_effect=[
            httpx.Response(401, json={"error": {"code": "UNAUTHENTICATED", "message": "no", "retryable": False}}),
            httpx.ConnectError(f"boom {KEY} {SECRET}"),
            httpx.ConnectError(f"boom {KEY} {SECRET}"),
            httpx.ConnectError(f"boom {KEY} {SECRET}"),
            httpx.ConnectError(f"boom {KEY} {SECRET}"),
        ]
    )
    caplog.set_level(logging.DEBUG)
    texts = [repr(client), str(client), repr(client._transport.auth), str(client._transport.auth)]
    with pytest.raises(cexy.AuthenticationError) as e1:
        client.account.balances()
    with pytest.raises(cexy.CexyConnectionError) as e2:
        client.account.balances()
    for exc in (e1.value, e2.value):
        texts += [str(exc), repr(exc), repr(exc.args)]
    texts.append(caplog.text)
    blob = "\n".join(texts)
    assert SECRET not in blob
    assert KEY not in blob
    assert "***" in repr(client)


def test_client_cannot_be_pickled() -> None:
    import pickle

    with pytest.raises(TypeError):
        pickle.dumps(cexy.Client(api_key=KEY, api_secret=SECRET))


@respx.mock(assert_all_called=False)
def test_half_pair_rejected_locally(respx_mock: respx.MockRouter) -> None:
    case = CASES["half_pair_rejected_locally"]
    spec = case["client"]
    route = respx_mock.route().respond(200)
    for cls in (cexy.Client, cexy.AsyncClient):
        with pytest.raises(ValueError):
            cls(api_key=spec["api_key"], api_secret=spec["api_secret"])
        with pytest.raises(ValueError):
            cls(api_key=None, api_secret=SECRET)
        with pytest.raises(ValueError):
            cls(api_key=KEY, api_secret="")
    assert case["expect"]["construct_error"] is True
    assert route.call_count == case["expect"]["requests_sent"] == 0


@respx.mock
def test_user_agent(public_client: cexy.Client) -> None:
    case = CASES["user_agent"]
    method, url = _route(case["request"]["operation"])
    route = respx.route(method=method, url=url).respond(json={"data": {"iso": "x", "epoch_ms": 1}})
    _call(public_client, case["request"]["operation"])
    ua = route.calls.last.request.headers["User-Agent"]
    for header, pattern in case["expect"]["header_matches"].items():
        assert re.match(pattern, route.calls.last.request.headers[header])
    assert ua == f"cexy-python/{cexy.__version__}"


@respx.mock
def test_user_agent_suffix() -> None:
    route = respx.get(BASE + "/api/v1/time").respond(json={"data": {"iso": "x", "epoch_ms": 1}})
    cexy.Client(user_agent_suffix="mybot/1.2").time()
    assert route.calls.last.request.headers["User-Agent"] == f"cexy-python/{cexy.__version__} mybot/1.2"


@respx.mock
def test_never_sends_authorization_header(client: cexy.Client) -> None:
    route = respx.route().respond(json={"data": [BALANCE]})
    client.account.balances()
    assert "Authorization" not in route.calls.last.request.headers


@pytest.mark.parametrize("resp", AUTH["server_responses"], ids=lambda r: r["id"])
@respx.mock
def test_server_responses(client: cexy.Client, resp: dict) -> None:
    respx.get(BASE + "/api/v1/account/balances").respond(resp["status"], json=resp["body"])
    with pytest.raises(cexy.CexyApiError) as exc:
        client.account.balances()
    expected = ERROR_CLASS[resp["expect"]["error_class"]]
    assert type(exc.value) is expected
    assert exc.value.code == resp["body"]["error"]["code"]
    assert exc.value.status == resp["status"]


@respx.mock(assert_all_called=False)
def test_private_operation_without_keys_fails_locally(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.route().respond(200)
    with pytest.raises(cexy.MissingCredentialsError):
        cexy.Client().account.balances()
    assert route.call_count == 0


@respx.mock
def test_server_echoed_credentials_are_redacted(client: cexy.Client, caplog: pytest.LogCaptureFixture) -> None:
    # A server (or proxy) that echoes the credentials must not leak them into the exception.
    # anything shaped like a key id is redacted too (built at runtime so no file contains one)
    other_key = "".join(["ak", "_", *(["Z"] * 20)])
    body = {
        "error": {
            "code": "INVALID_CREDENTIALS",
            "message": f"bad key {KEY} with secret {SECRET}",
            "details": {"key": KEY, "nested": [{"secret": SECRET}, f"x{SECRET}y"], SECRET: 1, "other": other_key},
            "fields": {"X-API-Secret": SECRET},
            "request_id": f"req-{KEY}",
            "retryable": False,
        }
    }
    respx.get(BASE + "/api/v1/account/balances").respond(401, json=body, headers={"X-Echo": SECRET})
    caplog.set_level(logging.DEBUG)
    with pytest.raises(cexy.AuthenticationError) as exc:
        client.account.balances()
    err = exc.value
    parts = [str(err), repr(err), err.message, repr(err.details), repr(err.fields), str(err.request_id)]
    blob = "\n".join([*parts, repr(err.headers), caplog.text])
    for s in (KEY, SECRET, other_key):
        assert s not in blob
    assert err.message == "bad key *** with secret ***"
    assert err.details["key"] == "***" and err.details["nested"] == [{"secret": "***"}, "x***y"]
    assert err.fields == {"X-API-Secret": "***"}
    assert err.code == "INVALID_CREDENTIALS"  # everything else is kept verbatim


@respx.mock
def test_custom_authenticator_secrets_are_redacted_from_errors() -> None:
    class Custom:
        def apply(self, method, url, headers, body):  # type: ignore[no-untyped-def]
            headers["X-API-Key"] = "custom-id"

        def secrets(self):  # type: ignore[no-untyped-def]
            return ("custom-id",)

    respx.get(BASE + "/api/v1/account/balances").respond(
        403, json={"error": {"code": "FORBIDDEN", "message": "key custom-id lacks scope", "retryable": False}}
    )
    with pytest.raises(cexy.ForbiddenError) as exc:
        cexy.Client(authenticator=Custom()).account.balances()
    assert "custom-id" not in str(exc.value) and exc.value.message == "key *** lacks scope"
