"""Public market data: no API key needed.

pip install cexy
python examples/public_market_data.py
"""

from cexy import Client


def main() -> None:
    with Client() as client:
        print("server time:", client.time().iso)
        markets = client.markets.list()
        for m in markets[:10]:
            print(f"{m.symbol:12} last={m.last_price} bid={m.best_bid} ask={m.best_ask} status={m.status}")
        if markets:
            book = client.markets.orderbook(markets[0].symbol, depth=5)
            print(f"\n{book.symbol} order book (sequence {book.sequence})")
            for price, qty in book.asks[::-1]:
                print(f"  ask {price} x {qty}")
            for price, qty in book.bids:
                print(f"  bid {price} x {qty}")


if __name__ == "__main__":
    main()
