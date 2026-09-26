"""WARNING: THIS PLACES A REAL ORDER on the live exchange with real funds.

It places a small post-only limit buy far below the market, then cancels it. Even so, an
order that reaches the book can fill if the market moves. Read the code before running it.
There is no sandbox yet.

    export CEXY_API_KEY=ak_your_key_here        # needs the `trade` scope
    export CEXY_API_SECRET=your_secret_here
    python examples/place_and_cancel_order.py BTC/USDT --yes-place-a-real-order
"""

import os
import sys
from decimal import ROUND_DOWN, Decimal

from cexy import Client, UnprocessableError


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[2] != "--yes-place-a-real-order":
        sys.exit("usage: place_and_cancel_order.py SYMBOL --yes-place-a-real-order  (places a REAL order)")
    symbol = sys.argv[1]
    with Client(api_key=os.environ["CEXY_API_KEY"], api_secret=os.environ["CEXY_API_SECRET"]) as client:
        market = client.markets.get(symbol)
        # 50% below the best bid, rounded to the market's tick; amounts are Decimal, never float.
        price = (market.best_bid * Decimal("0.5")).quantize(market.tick_size, rounding=ROUND_DOWN)
        quantity = max(market.min_quantity, (market.min_notional / price).quantize(market.lot_size) + market.lot_size)
        try:
            placed = client.trading.place_order(
                symbol, "buy", "limit", quantity=quantity, price=price, time_in_force="post_only"
            )
        except UnprocessableError as err:
            sys.exit(f"refused: {err.code} {err.details}")
        order = placed.order
        print(f"placed {order.id} (client_order_id={order.client_order_id}) {order.status}")
        cancelled = client.trading.cancel_order(order.id)
        print(f"cancelled: {cancelled.status}")


if __name__ == "__main__":
    main()
