from ..clients.metaApi.synchronizationListener import SynchronizationListener
from ..clients.metaApi.reconnectListener import ReconnectListener
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from .metatraderAccountModel import MetatraderAccountModel
from .models import MetatraderTradeResponse, MarketTradeOptions, PendingTradeOptions, StopOptions
from typing import Coroutine
import asyncio


class MetaApiConnection(SynchronizationListener, ReconnectListener):
    """Exposes MetaApi MetaTrader API connection to consumers."""

    def __init__(self, websocket_client: MetaApiWebsocketClient, account: MetatraderAccountModel,
                 application: str = None):
        """Inits MetaApi MetaTrader Api connection.

        Args:
            websocket_client: MetaApi websocket client.
            account: MetaTrader account id to connect to.
            application: Application to use.
        """
        super().__init__()
        self._websocketClient = websocket_client
        self._account = account
        self._application = application

    def create_market_buy_order(self, symbol: str, volume: float, stop_loss: float or StopOptions = None,
                                take_profit: float or StopOptions = None,
                                options: MarketTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Creates a market buy order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_BUY', 'symbol': symbol, 'volume': volume,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def create_market_sell_order(self, symbol: str, volume: float, stop_loss: float or StopOptions = None,
                                 take_profit: float or StopOptions = None,
                                 options: MarketTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Creates a market sell order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_SELL', 'symbol': symbol, 'volume': volume,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def create_limit_buy_order(self, symbol: str, volume: float, open_price: float,
                               stop_loss: float or StopOptions = None,
                               take_profit: float or StopOptions = None, options: PendingTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Creates a limit buy order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            open_price: Order limit price.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_BUY_LIMIT', 'symbol': symbol, 'volume': volume,
                        'openPrice': open_price,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def create_limit_sell_order(self, symbol: str, volume: float, open_price: float,
                                stop_loss: float or StopOptions = None,
                                take_profit: float or StopOptions = None, options: PendingTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Creates a limit sell order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            open_price: Order limit price.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_SELL_LIMIT', 'symbol': symbol, 'volume': volume,
                        'openPrice': open_price,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def create_stop_buy_order(self, symbol: str, volume: float, open_price: float,
                              stop_loss: float or StopOptions = None,
                              take_profit: float or StopOptions = None, options: PendingTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Creates a stop buy order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            open_price: Order limit price.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_BUY_STOP', 'symbol': symbol, 'volume': volume,
                        'openPrice': open_price,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def create_stop_sell_order(self, symbol: str, volume: float, open_price: float,
                               stop_loss: float or StopOptions = None,
                               take_profit: float or StopOptions = None, options: PendingTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Creates a stop sell order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            open_price: Order limit price.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_SELL_STOP', 'symbol': symbol, 'volume': volume,
                        'openPrice': open_price,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def create_stop_limit_buy_order(self, symbol: str, volume: float, open_price: float, stop_limit_price: float,
                                    stop_loss: float or StopOptions = None, take_profit: float or StopOptions = None,
                                    options: PendingTradeOptions = None):
        """Creates a stop limit buy order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            open_price: Order limit price.
            stop_limit_price: The limit order price for the stop limit order.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_BUY_STOP_LIMIT', 'symbol': symbol, 'volume': volume,
                        'openPrice': open_price, 'stopLimitPrice': stop_limit_price,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def create_stop_limit_sell_order(self, symbol: str, volume: float, open_price: float, stop_limit_price: float,
                                     stop_loss: float or StopOptions = None, take_profit: float or StopOptions = None,
                                     options: PendingTradeOptions = None):
        """Creates a stop limit sell order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            volume: Order volume.
            open_price: Order limit price.
            stop_limit_price: The limit order price for the stop limit order.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_TYPE_SELL_STOP_LIMIT', 'symbol': symbol, 'volume': volume,
                        'openPrice': open_price, 'stopLimitPrice': stop_limit_price,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def modify_position(self, position_id: str, stop_loss: float or StopOptions = None,
                        take_profit: float or StopOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Modifies a position (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            position_id: Position id to modify.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error.
        """
        trade_params = {'actionType': 'POSITION_MODIFY', 'positionId': position_id,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def close_position_partially(self, position_id: str, volume: float, options: MarketTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Partially closes a position (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            position_id: Position id to modify.
            volume: Volume to close.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'POSITION_PARTIAL', 'positionId': position_id, 'volume': volume}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def close_position(self, position_id: str, options: MarketTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Fully closes a position (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            position_id: Position id to modify.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'POSITION_CLOSE_ID', 'positionId': position_id}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def close_by(self, position_id: str, opposite_position_id: str, options: MarketTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Fully closes a position (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            position_id: Position id to close by opposite position.
            opposite_position_id: Opposite position id to close.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'POSITION_CLOSE_BY', 'positionId': position_id,
                        'closeByPositionId': opposite_position_id}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def close_positions_by_symbol(self, symbol: str, options: MarketTradeOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Closes positions by a symbol (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            symbol: Symbol to trade.
            options: Optional trade options.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'POSITIONS_CLOSE_SYMBOL', 'symbol': symbol}
        trade_params.update(options or {})
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def modify_order(self, order_id: str, open_price: float, stop_loss: float or StopOptions = None,
                     take_profit: float or StopOptions = None) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Modifies a pending order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            order_id: Order id (ticket number).
            open_price: Order stop price.
            stop_loss: Optional stop loss price.
            take_profit: Optional take profit price.

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        trade_params = {'actionType': 'ORDER_MODIFY', 'orderId': order_id, 'openPrice': open_price,
                        **self._generate_stop_options(stop_loss=stop_loss, take_profit=take_profit)}
        return self._websocketClient.trade(self._account.id, trade_params, self._application)

    def cancel_order(self, order_id: str) -> \
            'Coroutine[asyncio.Future[MetatraderTradeResponse]]':
        """Cancels order (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            order_id: Order id (ticket number).

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        return self._websocketClient.trade(self._account.id, {'actionType': 'ORDER_CANCEL', 'orderId': order_id},
                                           self._application)

    def reconnect(self) -> Coroutine:
        """Reconnects to the Metatrader terminal (see https://metaapi.cloud/docs/client/websocket/api/reconnect/).

        Returns:
            A coroutine which resolves when reconnection started.
        """
        return self._websocketClient.reconnect(self._account.id)

    def on_reconnected(self):
        pass

    @property
    def account(self) -> MetatraderAccountModel:
        """Returns MetaApi account.

        Returns:
            MetaApi account.
        """
        return self._account

    @staticmethod
    def _generate_stop_options(stop_loss, take_profit):
        trade = {}
        if isinstance(stop_loss, int) or isinstance(stop_loss, float):
            trade['stopLoss'] = stop_loss
        elif stop_loss:
            trade['stopLoss'] = stop_loss['value']
            trade['stopLossUnits'] = stop_loss['units']
        if isinstance(take_profit, int) or isinstance(take_profit, float):
            trade['takeProfit'] = take_profit
        elif take_profit:
            trade['takeProfit'] = take_profit['value']
            trade['takeProfitUnits'] = take_profit['units']
        return trade
