"""Resource facade: ``client.markets.orderbook("BTC/USDT")`` and friends.

Async source; the synchronous ``cexy/_sync/resources.py`` is generated from it by ``scripts/unasync.py``.
Every public method maps to exactly one allowlisted API operation (``@operation``);
``tests/test_coverage.py`` checks the set against ``spec/openapi.sdk.json``.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, List, Optional, Union

from cexy._async.pagination import AsyncPage
from cexy._async.transport import AsyncTransport
from cexy._common import AUTO
from cexy._decimal import DecimalLike, to_wire, to_wire_opt
from cexy._generated import models as m
from cexy._opmap import operation
from cexy.errors import CexyApiError, NotFoundError

DateLike = Union[datetime, str]
Direction = Union[m.SortDirection, str]


def _data(payload: Any) -> Any:
    return payload["data"]


class _Resource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport


class AsyncMarkets(_Resource):
    """Public market data. No credentials are sent."""

    @operation("list_markets")
    async def list(self) -> List[m.MarketResponse]:
        """Every market with its current ticker."""
        payload = await self._t.request("list_markets")
        return [m.MarketResponse.model_validate(x) for x in _data(payload)]

    @operation("get_market")
    async def get(self, symbol: str) -> m.MarketResponse:
        """One market, e.g. ``"BTC/USDT"``."""
        payload = await self._t.request("get_market", path={"symbol": symbol})
        return m.MarketResponse.model_validate(_data(payload))

    @operation("get_order_book")
    async def orderbook(self, symbol: str, depth: Optional[int] = None) -> m.OrderBookResponse:
        """Order book snapshot. ``sequence`` lets you align it with the WebSocket stream."""
        payload = await self._t.request("get_order_book", path={"symbol": symbol}, query={"depth": depth})
        return m.OrderBookResponse.model_validate(_data(payload))

    @operation("get_market_trades")
    async def trades(
        self,
        symbol: str,
        *,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        direction: Optional[Direction] = None,
    ) -> AsyncPage[m.PublicTradeResponse]:
        """Recent public trades (cursor-paginated)."""

        async def fetch(c: str) -> AsyncPage[m.PublicTradeResponse]:
            return await self.trades(symbol, cursor=c, limit=limit, direction=direction)

        payload = await self._t.request(
            "get_market_trades",
            path={"symbol": symbol},
            query={"cursor": cursor, "limit": limit, "direction": direction},
        )
        return AsyncPage.parse(payload, m.PublicTradeResponse, fetch)

    @operation("get_candles")
    async def candles(
        self,
        symbol: str,
        interval: Union[m.CandleInterval, str],
        *,
        start_time: Optional[DateLike] = None,
        end_time: Optional[DateLike] = None,
        limit: Optional[int] = None,
    ) -> List[m.CandleResponse]:
        """OHLCV candles. ``interval`` is one of ``1m 5m 15m 30m 1h 4h 1d 1w``."""
        payload = await self._t.request(
            "get_candles",
            path={"symbol": symbol},
            query={"interval": interval, "start_time": start_time, "end_time": end_time, "limit": limit},
        )
        return [m.CandleResponse.model_validate(x) for x in _data(payload)]


class AsyncAssets(_Resource):
    """Public asset catalogue."""

    @operation("list_assets")
    async def list(self) -> List[m.AssetResponse]:
        payload = await self._t.request("list_assets")
        return [m.AssetResponse.model_validate(x) for x in _data(payload)]

    @operation("get_asset")
    async def get(self, symbol: str) -> m.AssetResponse:
        payload = await self._t.request("get_asset", path={"symbol": symbol})
        return m.AssetResponse.model_validate(_data(payload))


class AsyncNetworks(_Resource):
    """Public blockchain network status."""

    @operation("list_networks")
    async def list(self) -> List[m.NetworkResponse]:
        payload = await self._t.request("list_networks")
        return [m.NetworkResponse.model_validate(x) for x in _data(payload)]


class AsyncFees(_Resource):
    """Public fee schedule."""

    @operation("list_fee_schedules")
    async def list(self) -> List[m.FeeScheduleResponse]:
        payload = await self._t.request("list_fee_schedules")
        return [m.FeeScheduleResponse.model_validate(x) for x in _data(payload)]


class AsyncPools(_Resource):
    """Liquidity pools. ``list``/``get`` are public; ``join``/``exit`` need a ``trade`` key."""

    @operation("list_pools")
    async def list(self) -> List[m.PoolResponse]:
        payload = await self._t.request("list_pools")
        return [m.PoolResponse.model_validate(x) for x in _data(payload)]

    @operation("get_pool")
    async def get(self, symbol: str) -> m.PoolResponse:
        payload = await self._t.request("get_pool", path={"symbol": symbol})
        return m.PoolResponse.model_validate(_data(payload))

    @operation("join_pool")
    async def join(
        self,
        symbol: str,
        *,
        base_amount: DecimalLike,
        quote_amount: DecimalLike,
        max_ratio_deviation_percent: Optional[DecimalLike] = None,
        idempotency_key: Optional[str] = None,
    ) -> m.JoinPoolResponse:
        """Add liquidity. Moves funds. Sends an ``Idempotency-Key`` (generated unless given),
        reused on every retry, so a retry never joins twice."""
        body = {
            "base_amount": to_wire(base_amount, "base_amount"),
            "quote_amount": to_wire(quote_amount, "quote_amount"),
            "max_ratio_deviation_percent": to_wire_opt(max_ratio_deviation_percent, "max_ratio_deviation_percent"),
        }
        payload = await self._t.request(
            "join_pool", path={"symbol": symbol}, body=body, idempotency_key=idempotency_key or AUTO
        )
        return m.JoinPoolResponse.model_validate(_data(payload))

    @operation("exit_pool")
    async def exit(
        self, symbol: str, *, shares: DecimalLike, idempotency_key: Optional[str] = None
    ) -> m.ExitPoolResponse:
        """Remove liquidity by burning ``shares``. Sends an ``Idempotency-Key``."""
        payload = await self._t.request(
            "exit_pool",
            path={"symbol": symbol},
            body={"shares": to_wire(shares, "shares")},
            idempotency_key=idempotency_key or AUTO,
        )
        return m.ExitPoolResponse.model_validate(_data(payload))


class AsyncAccount(_Resource):
    """Account reads (``read`` scope)."""

    @operation("list_balances")
    async def balances(self) -> List[m.BalanceResponse]:
        payload = await self._t.request("list_balances")
        return [m.BalanceResponse.model_validate(x) for x in _data(payload)]

    @operation("get_balance")
    async def balance(self, asset: str) -> m.BalanceResponse:
        payload = await self._t.request("get_balance", path={"asset": asset})
        return m.BalanceResponse.model_validate(_data(payload))

    @operation("get_ledger")
    async def ledger(
        self,
        *,
        asset: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        direction: Optional[Direction] = None,
    ) -> AsyncPage[m.LedgerEntryResponse]:
        async def fetch(c: str) -> AsyncPage[m.LedgerEntryResponse]:
            return await self.ledger(asset=asset, cursor=c, limit=limit, direction=direction)

        payload = await self._t.request(
            "get_ledger", query={"asset": asset, "cursor": cursor, "limit": limit, "direction": direction}
        )
        return AsyncPage.parse(payload, m.LedgerEntryResponse, fetch)

    @operation("list_notifications")
    async def notifications(
        self,
        *,
        unread_only: Optional[bool] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        direction: Optional[Direction] = None,
    ) -> AsyncPage[m.NotificationResponse]:
        async def fetch(c: str) -> AsyncPage[m.NotificationResponse]:
            return await self.notifications(unread_only=unread_only, cursor=c, limit=limit, direction=direction)

        payload = await self._t.request(
            "list_notifications",
            query={"unread_only": unread_only, "cursor": cursor, "limit": limit, "direction": direction},
        )
        return AsyncPage.parse(payload, m.NotificationResponse, fetch)

    @operation("list_sub_accounts")
    async def sub_accounts(self) -> List[m.SubAccountResponse]:
        payload = await self._t.request("list_sub_accounts")
        return [m.SubAccountResponse.model_validate(x) for x in _data(payload)]

    @operation("list_api_keys")
    async def api_keys(self) -> List[m.ApiKeyResponse]:
        """Your API keys (metadata only; secrets are never returned)."""
        payload = await self._t.request("list_api_keys")
        return [m.ApiKeyResponse.model_validate(x) for x in _data(payload)]


class AsyncExports(_Resource):
    """CSV exports (``read`` scope). Each method returns the CSV as bytes.
    ``from_``/``to`` default server-side to the last 30 days."""

    async def _export(self, op_id: str, from_: Optional[DateLike], to: Optional[DateLike]) -> bytes:
        result: bytes = await self._t.request(op_id, query={"from": from_, "to": to}, raw=True)
        return result

    @operation("export_deposits")
    async def deposits(self, *, from_: Optional[DateLike] = None, to: Optional[DateLike] = None) -> bytes:
        return await self._export("export_deposits", from_, to)

    @operation("export_ledger")
    async def ledger(self, *, from_: Optional[DateLike] = None, to: Optional[DateLike] = None) -> bytes:
        return await self._export("export_ledger", from_, to)

    @operation("export_orders")
    async def orders(self, *, from_: Optional[DateLike] = None, to: Optional[DateLike] = None) -> bytes:
        return await self._export("export_orders", from_, to)

    @operation("export_trades")
    async def trades(self, *, from_: Optional[DateLike] = None, to: Optional[DateLike] = None) -> bytes:
        return await self._export("export_trades", from_, to)

    @operation("export_withdrawals")
    async def withdrawals(self, *, from_: Optional[DateLike] = None, to: Optional[DateLike] = None) -> bytes:
        return await self._export("export_withdrawals", from_, to)


class AsyncWallet(_Resource):
    """Wallet reads (``read`` scope). API keys can never withdraw or transfer."""

    @operation("list_deposits")
    async def deposits(
        self,
        *,
        asset: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        direction: Optional[Direction] = None,
    ) -> AsyncPage[m.DepositResponse]:
        async def fetch(c: str) -> AsyncPage[m.DepositResponse]:
            return await self.deposits(asset=asset, cursor=c, limit=limit, direction=direction)

        payload = await self._t.request(
            "list_deposits", query={"asset": asset, "cursor": cursor, "limit": limit, "direction": direction}
        )
        return AsyncPage.parse(payload, m.DepositResponse, fetch)

    @operation("get_deposit")
    async def deposit(self, deposit_id: str) -> m.DepositResponse:
        payload = await self._t.request("get_deposit", path={"deposit_id": deposit_id})
        return m.DepositResponse.model_validate(_data(payload))

    @operation("list_withdrawals")
    async def withdrawals(
        self,
        *,
        asset: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        direction: Optional[Direction] = None,
    ) -> AsyncPage[m.WithdrawalResponse]:
        async def fetch(c: str) -> AsyncPage[m.WithdrawalResponse]:
            return await self.withdrawals(asset=asset, cursor=c, limit=limit, direction=direction)

        payload = await self._t.request(
            "list_withdrawals", query={"asset": asset, "cursor": cursor, "limit": limit, "direction": direction}
        )
        return AsyncPage.parse(payload, m.WithdrawalResponse, fetch)

    @operation("get_withdrawal")
    async def withdrawal(self, withdrawal_id: str) -> m.WithdrawalResponse:
        payload = await self._t.request("get_withdrawal", path={"withdrawal_id": withdrawal_id})
        return m.WithdrawalResponse.model_validate(_data(payload))

    @operation("list_withdrawal_addresses")
    async def withdrawal_addresses(self) -> List[m.WithdrawalAddressResponse]:
        payload = await self._t.request("list_withdrawal_addresses")
        return [m.WithdrawalAddressResponse.model_validate(x) for x in _data(payload)]

    @operation("deposit_address")
    async def deposit_address(self, asset: str, network: str) -> m.DepositAddressResponse:
        """Your deposit address for ``asset`` on ``network``.

        **Side effect: this CREATES an address the first time it is called for an
        asset/network pair** (the server derives and stores one), and returns the same
        address on every later call. It is a GET, but it is not a pure read.
        If ``memo`` is set, deposits without it may be unrecoverable.
        """
        payload = await self._t.request("deposit_address", query={"asset": asset, "network": network})
        return m.DepositAddressResponse.model_validate(_data(payload))


class AsyncTrading(_Resource):
    """Orders and fills. Reads need ``read``; placing and cancelling need ``trade``."""

    @operation("list_open_orders")
    async def open_orders(
        self, *, symbol: Optional[str] = None, status: Optional[Union[m.OrderStatus, str]] = None
    ) -> List[m.OrderResponse]:
        payload = await self._t.request("list_open_orders", query={"symbol": symbol, "status": status})
        return [m.OrderResponse.model_validate(x) for x in _data(payload)]

    @operation("get_order")
    async def order(self, order_id: str) -> m.OrderResponse:
        payload = await self._t.request("get_order", path={"order_id": order_id})
        return m.OrderResponse.model_validate(_data(payload))

    @operation("get_order_by_client_id")
    async def order_by_client_id(self, client_order_id: str) -> m.OrderResponse:
        payload = await self._t.request("get_order_by_client_id", path={"client_order_id": client_order_id})
        return m.OrderResponse.model_validate(_data(payload))

    @operation("order_history")
    async def order_history(
        self,
        *,
        symbol: Optional[str] = None,
        status: Optional[Union[m.OrderStatus, str]] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        direction: Optional[Direction] = None,
    ) -> AsyncPage[m.OrderResponse]:
        async def fetch(c: str) -> AsyncPage[m.OrderResponse]:
            return await self.order_history(symbol=symbol, status=status, cursor=c, limit=limit, direction=direction)

        payload = await self._t.request(
            "order_history",
            query={"symbol": symbol, "status": status, "cursor": cursor, "limit": limit, "direction": direction},
        )
        return AsyncPage.parse(payload, m.OrderResponse, fetch)

    @operation("trade_history")
    async def trades(
        self,
        *,
        symbol: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        direction: Optional[Direction] = None,
    ) -> AsyncPage[m.FillResponse]:
        """Your own fills (cursor-paginated)."""

        async def fetch(c: str) -> AsyncPage[m.FillResponse]:
            return await self.trades(symbol=symbol, cursor=c, limit=limit, direction=direction)

        payload = await self._t.request(
            "trade_history", query={"symbol": symbol, "cursor": cursor, "limit": limit, "direction": direction}
        )
        return AsyncPage.parse(payload, m.FillResponse, fetch)

    @operation("place_order")
    async def place_order(
        self,
        symbol: str,
        side: Union[m.OrderSide, str],
        type: Union[m.OrderType, str],
        *,
        quantity: Optional[DecimalLike] = None,
        price: Optional[DecimalLike] = None,
        quote_quantity: Optional[DecimalLike] = None,
        stop_price: Optional[DecimalLike] = None,
        time_in_force: Optional[Union[m.TimeInForce, str]] = None,
        trigger_direction: Optional[Union[m.TriggerDirection, str]] = None,
        client_order_id: Optional[str] = None,
    ) -> m.PlaceOrderResponse:
        """Place a REAL order. Amounts must be ``Decimal``, ``str`` or ``int`` (floats raise).

        Retry safety rests on ``client_order_id`` (the server does not honour
        ``Idempotency-Key`` on order endpoints, so none is sent). A ``client_order_id`` (a
        UUID) is generated when you do not pass one; it is unique per account, so the server
        refuses a repeat before any funds move.
        After an ambiguous failure (timeout, dropped connection, 500/502/504) the SDK first
        looks the order up with ``GET /trading/orders/by-client-id/{id}``; if it exists that
        order is returned (``fills`` is then empty: query ``trading.trades`` for them),
        otherwise the order is resent with the same ``client_order_id``. If a retry is
        refused as ``ALREADY_EXISTS``, the existing order is fetched and returned.
        """
        cid = client_order_id or str(uuid.uuid4())
        body = {
            "symbol": symbol,
            "side": _enum_value(side),
            "type": _enum_value(type),
            "quantity": to_wire_opt(quantity, "quantity"),
            "price": to_wire_opt(price, "price"),
            "quote_quantity": to_wire_opt(quote_quantity, "quote_quantity"),
            "stop_price": to_wire_opt(stop_price, "stop_price"),
            "time_in_force": _enum_value(time_in_force),
            "trigger_direction": _enum_value(trigger_direction),
            "client_order_id": cid,
        }

        async def recover() -> Optional[Any]:
            try:
                existing = await self.order_by_client_id(cid)
            except NotFoundError:
                return None
            return {"data": {"order": existing.model_dump(mode="json"), "fills": []}}

        async def on_retry_error(err: CexyApiError) -> Optional[Any]:
            # The duplicate client_order_id means an earlier attempt placed the order.
            return await recover() if err.code == "ALREADY_EXISTS" else None

        payload = await self._t.request("place_order", body=body, recover=recover, on_retry_error=on_retry_error)
        return m.PlaceOrderResponse.model_validate(_data(payload))

    @operation("cancel_order")
    async def cancel_order(self, order_id: str) -> m.OrderResponse:
        """Cancel one order and return its state.

        Cancelling is naturally repeatable, so it is retried like a read. If a *retry* is
        refused with ``INVALID_STATE`` (an earlier attempt already cancelled it, or it filled
        meanwhile), the SDK fetches and returns the order's current state instead of
        raising. Check ``status`` on the result. (The server does not honour
        ``Idempotency-Key`` on order endpoints, so none is sent.)
        """

        async def on_retry_error(err: CexyApiError) -> Optional[Any]:
            if err.code != "INVALID_STATE":
                return None
            current = await self.order(order_id)
            return {"data": current.model_dump(mode="json")}

        payload = await self._t.request("cancel_order", path={"order_id": order_id}, on_retry_error=on_retry_error)
        return m.OrderResponse.model_validate(_data(payload))

    @operation("cancel_all")
    async def cancel_all(self, *, symbol: Optional[str]) -> m.CancelAllResponse:
        """Cancel every open order in ``symbol``.

        ``symbol`` is a required keyword so that cancelling everywhere is always explicit:
        ``symbol=None`` cancels open orders in ALL markets. The server allows 30 calls per
        minute per account. Repeating the call is harmless (it cancels whatever is still
        open), so it is retried like a read.
        """
        payload = await self._t.request("cancel_all", body={"symbol": symbol})
        return m.CancelAllResponse.model_validate(_data(payload))


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)
