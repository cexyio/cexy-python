"""Live order book over the WebSocket (public, no key needed).

python examples/websocket_orderbook.py BTC/USDT
"""

import asyncio
import sys

from cexy.ws import AUTH_LOST, BOOK_STALE, RECONNECTED, WebSocketClient


async def main(symbol: str) -> None:
    async with WebSocketClient() as ws:
        book = await ws.order_book(symbol)
        await ws.subscribe(f"trades:{symbol}")
        async for event in ws:
            if event.type == "orderbook.update":
                print(f"seq={book.sequence} bid={book.best_bid()} ask={book.best_ask()} stale={book.stale}")
            elif event.type == "trade.new":
                print("trade", event.data)
            elif event.type in (BOOK_STALE, RECONNECTED, AUTH_LOST):
                print("--", event.type)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "BTC/USDT"))
