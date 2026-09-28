"""Consumer smoke check: run with an interpreter that has ONLY the built distribution installed
(no extras, no dev dependencies, no repository on sys.path). Offline by default; with
CEXY_LIVE_TESTS=1 it also makes one public time() call.

    python ci/consumer/smoke.py <expected-version>
"""

from __future__ import annotations

import asyncio
import os
import sys


def require(ok: bool, what: str) -> None:
    # Not `assert`: this must also fail under python -O.
    if not ok:
        raise SystemExit(f"smoke: {what}")


def main() -> int:
    expected = sys.argv[1]
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(os.path.dirname(here))
    if any(os.path.abspath(p or ".") == repo for p in sys.path):
        print("smoke: the repository is on sys.path; run from outside it", file=sys.stderr)
        return 1

    import cexy

    require(cexy.__version__ == expected, f"version {cexy.__version__} != {expected}")
    require(f"cexy-python/{expected}" == cexy.USER_AGENT, f"User-Agent {cexy.USER_AGENT}")
    for name in ("Client", "AsyncClient", "CexyApiError", "RateLimitError", "NotFoundError", "CancelAllResult"):
        require(hasattr(cexy, name), f"cexy.{name} is missing")
    require("site-packages" in os.path.dirname(cexy.__file__), f"imported from {cexy.__file__}")

    client = cexy.Client()
    aclient = cexy.AsyncClient()
    keyed = cexy.Client(api_key="ak_smoke", api_secret="smoke_secret")  # noqa: S106 - a dummy value
    require("smoke_secret" not in repr(keyed), "the secret appears in repr(Client)")

    if os.environ.get("CEXY_LIVE_TESTS") == "1":
        print("live time():", client.time())

        async def live() -> None:
            async with aclient as c:
                print("live async time():", await c.time())

        asyncio.run(live())
    else:
        asyncio.run(aclient.aclose())
    client.close()
    keyed.close()
    print(f"consumer smoke ok: cexy {cexy.__version__} from {os.path.dirname(cexy.__file__)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
