"""Account balances and recent ledger entries (needs a key with the `read` scope).

    export CEXY_API_KEY=ak_your_key_here
    export CEXY_API_SECRET=your_secret_here
    python examples/balances.py

Use a read-only key restricted to your IP addresses (allowed_ips) for scripts like this.
"""

import os

from cexy import Client


def main() -> None:
    with Client(api_key=os.environ["CEXY_API_KEY"], api_secret=os.environ["CEXY_API_SECRET"]) as client:
        for b in client.account.balances():
            if b.total:
                print(f"{b.asset:8} total={b.total} available={b.available} locked={b.locked}")
        print("\nlast 20 ledger entries:")
        page = client.account.ledger(limit=20)
        for entry in page.items:
            print(f"  {entry.created_at:%Y-%m-%d %H:%M} {entry.asset:6} {entry.kind.value:20} {entry.available_delta}")


if __name__ == "__main__":
    main()
