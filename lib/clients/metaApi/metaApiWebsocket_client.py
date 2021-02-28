from ..timeoutException import TimeoutException
from .tradeException import TradeException
from ..errorHandler import ValidationException, NotFoundException, InternalException, UnauthorizedException
from .notSynchronizedException import NotSynchronizedException
from .notConnectedException import NotConnectedException
from .synchronizationListener import SynchronizationListener
from .reconnectListener import ReconnectListener
from ...metaApi.models import MetatraderHistoryOrders, MetatraderDeals, date, random_id, \
    MetatraderSymbolSpecification, MetatraderTradeResponse, MetatraderSymbolPrice, MetatraderAccountInformation, \
    MetatraderPosition, MetatraderOrder, format_date
from .latencyListener import LatencyListener
from .packetOrderer import PacketOrderer
from .packetLogger import PacketLogger
from .synchronizationThrottler import SynchronizationThrottler
import socketio
import asyncio
import re
from random import random
from datetime import datetime, timedelta
from typing import Coroutine, List, Dict


class MetaApiWebsocketClient:
    """MetaApi websocket API client (see https://metaapi.cloud/docs/client/websocket/overview/)"""

    def __init__(self, token: str, opts: Dict = None):
        """Inits MetaApi websocket API client instance.

        Args:
            token: Authorization token.
            opts: Websocket client options.
        """
        opts = opts or {}
        opts['packetOrderingTimeout'] = opts['packetOrderingTimeout'] if 'packetOrderingTimeout' in opts else 60
        self._application = opts['application'] if 'application' in opts else 'MetaApi'
        self._url = f'https://mt-client-api-v1.{opts["domain"] if "domain" in opts else "agiliumtrade.agiliumtrade.ai"}'
        self._request_timeout = opts['requestTimeout'] if 'requestTimeout' in opts else 60
        self._connect_timeout = opts['connectTimeout'] if 'connectTimeout' in opts else 60
        self._maxConcurrentSynchronizations = opts['maxConcurrentSynchronizations'] if 'maxConcurrentSynchronizations'\
                                                                                       in opts else 10
        retry_opts = opts['retryOpts'] if 'retryOpts' in opts else {}
        self._retries = retry_opts['retries'] if 'retries' in opts else 5
        self._minRetryDelayInSeconds = retry_opts['minDelayInSeconds'] if 'minDelayInSeconds' in retry_opts else 1
        self._maxRetryDelayInSeconds = retry_opts['maxDelayInSeconds'] if 'maxDelayInSeconds' in retry_opts else 30
        self._token = token
        self._requestResolves = {}
        self._synchronizationListeners = {}
        self._latencyListeners = []
        self._connected = False
        self._socket = None
        self._sessionId = None
        self._reconnectListeners = []
        self._connectedHosts = {}
        self._resubscriptionTriggerTimes = {}
        self._synchronizationThrottler = SynchronizationThrottler(self, self._maxConcurrentSynchronizations)
        self._synchronizationThrottler.start()
        self._packetOrderer = PacketOrderer(self, opts['packetOrderingTimeout'])
        if 'packetLogger' in opts and 'enabled' in opts['packetLogger'] and opts['packetLogger']['enabled']:
            self._packetLogger = PacketLogger(opts['packetLogger'])
            self._packetLogger.start()
        else:
            self._packetLogger = None

    async def on_out_of_order_packet(self, account_id: str, instance_index: int, expected_sequence_number: int,
                                     actual_sequence_number: int, packet: Dict, received_at: datetime):
        """Restarts the account synchronization process on an out of order packet.

        Args:
            account_id: Account id.
            instance_index: Instance index.
            expected_sequence_number: Expected s/n.
            actual_sequence_number: Actual s/n.
            packet: Packet data.
            received_at: Time the packet was received at.
        """
        print(f'[{datetime.now().isoformat()}] MetaApi websocket client received an out of order packet type ' +
              f'{packet["type"]} for account id {account_id}. Expected s/n {expected_sequence_number} does not ' +
              f'match the actual of {actual_sequence_number}')
        try:
            await self.subscribe(account_id, instance_index)
        except Exception as err:
            if err.__class__.__name__ != 'TimeoutException':
                print((f'[{datetime.now().isoformat()}] MetaApi websocket client failed to receive ' +
                       'subscribe response for account id ' + account_id + ':' + str(instance_index), err))

    def set_url(self, url: str):
        """Patch server URL for use in unit tests

        Args:
            url: Patched server URL.
        """
        self._url = url

    async def connect(self) -> asyncio.Future:
        """Connects to MetaApi server via socket.io protocol

        Returns:
            A coroutine which resolves when connection is established.
        """
        if not self._connected:
            self._connected = True
            self._requestResolves = {}
            self._resolved = False
            self._connectResult = asyncio.Future()
            self._packetOrderer.start()
            url = f'{self._url}?auth-token={self._token}'
            self._socket = socketio.AsyncClient(reconnection=False, request_timeout=self._request_timeout)

            while not self._socket.connected:
                try:
                    self._sessionId = random_id()
                    await asyncio.wait_for(
                        self._socket.connect(url, socketio_path='ws',
                                             headers={'Client-id': '{:01.10f}'.format(random())}),
                        timeout=self._connect_timeout)
                except Exception:
                    pass

            @self._socket.on('connect')
            async def on_connect():
                print(f'[{datetime.now().isoformat()}] MetaApi websocket client connected to the MetaApi server')
                if not self._resolved:
                    self._resolved = True
                    self._connectResult.set_result(None)

                if not self._connected:
                    self._socket.disconnect()

            @self._socket.on('connect_error')
            def on_connect_error(err):
                print(f'[{datetime.now().isoformat()}] MetaApi websocket client connection error', err)
                if not self._resolved:
                    self._resolved = True
                    self._connectResult.set_exception(Exception(err))

            @self._socket.on('connect_timeout')
            def on_connect_timeout(timeout):
                print(f'[{datetime.now().isoformat()}] MetaApi websocket client connection timeout')
                if not self._resolved:
                    self._resolved = True
                    self._connectResult.set_exception(TimeoutException('MetaApi websocket client connection timed out'))

            @self._socket.on('disconnect')
            async def on_disconnect():
                self._synchronizationThrottler.on_disconnect()
                print(f'[{datetime.now().isoformat()}] MetaApi websocket client disconnected from the MetaApi server')
                await self._reconnect()

            @self._socket.on('error')
            async def on_error(error):
                print(f'[{datetime.now().isoformat()}] MetaApi websocket client error', error)
                await self._reconnect()

            @self._socket.on('response')
            async def on_response(data):
                if data['requestId'] in self._requestResolves:
                    request_resolve = self._requestResolves[data['requestId']]
                    del self._requestResolves[data['requestId']]
                else:
                    request_resolve = asyncio.Future()
                self._convert_iso_time_to_date(data)
                if not request_resolve.done():
                    request_resolve.set_result(data)
                if 'timestamps' in data and hasattr(request_resolve, 'type'):
                    data['timestamps']['clientProcessingFinished'] = datetime.now()
                    for listener in self._latencyListeners:
                        try:
                            if request_resolve.type == 'trade':
                                await listener.on_trade(data['accountId'], data['timestamps'])
                            else:
                                await listener.on_response(data['accountId'], request_resolve.type, data['timestamps'])
                        except Exception as error:
                            print(f'[{datetime.now().isoformat()}] Failed to process on_response event for account ' +
                                  data['accountId'] + ', request type ' + request_resolve.type, error)

            @self._socket.on('processingError')
            def on_processing_error(data):
                if data['requestId'] in self._requestResolves:
                    request_resolve = self._requestResolves[data['requestId']]
                    del self._requestResolves[data['requestId']]
                    if not request_resolve.done():
                        request_resolve.set_exception(self._convert_error(data))

            @self._socket.on('synchronization')
            async def on_synchronization(data):
                if self._packetLogger:
                    self._packetLogger.log_packet(data)
                self._convert_iso_time_to_date(data)
                await self._process_synchronization_packet(data)

            return self._connectResult

    async def close(self):
        """Closes connection to MetaApi server"""
        if self._connected:
            self._connected = False
            await self._socket.disconnect()
            for request_resolve in self._requestResolves:
                if not self._requestResolves[request_resolve].done():
                    self._requestResolves[request_resolve].set_exception(Exception('MetaApi connection closed'))
            self._requestResolves = {}
            self._synchronizationListeners = {}
            self._latencyListeners = []
            self._reconnectListeners = []
            self._packetOrderer.stop()

    async def get_account_information(self, account_id: str) -> 'asyncio.Future[MetatraderAccountInformation]':
        """Returns account information for a specified MetaTrader account
        (see https://metaapi.cloud/docs/client/websocket/api/readTradingTerminalState/readAccountInformation/).

        Args:
            account_id: Id of the MetaTrader account to return information for.

        Returns:
            A coroutine resolving with account information.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getAccountInformation'})
        return response['accountInformation']

    async def get_positions(self, account_id: str) -> 'asyncio.Future[List[MetatraderPosition]]':
        """Returns positions for a specified MetaTrader account
        (see https://metaapi.cloud/docs/client/websocket/api/readTradingTerminalState/readPositions/).

        Args:
            account_id: Id of the MetaTrader account to return information for.

        Returns:
            A coroutine resolving with array of open positions.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getPositions'})
        return response['positions']

    async def get_position(self, account_id: str, position_id: str) -> 'asyncio.Future[MetatraderPosition]':
        """Returns specific position for a MetaTrader account
        (see https://metaapi.cloud/docs/client/websocket/api/readTradingTerminalState/readPosition/).

        Args:
            account_id: Id of the MetaTrader account to return information for.
            position_id: Position id.

        Returns:
            A coroutine resolving with MetaTrader position found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getPosition',
                                                        'positionId': position_id})
        return response['position']

    async def get_orders(self, account_id: str) -> 'asyncio.Future[List[MetatraderOrder]]':
        """Returns open orders for a specified MetaTrader account
        (see https://metaapi.cloud/docs/client/websocket/api/readTradingTerminalState/readOrders/).

        Args:
            account_id: Id of the MetaTrader account to return information for.

        Returns:
            A coroutine resolving with open MetaTrader orders.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getOrders'})
        return response['orders']

    async def get_order(self, account_id: str, order_id: str) -> 'asyncio.Future[MetatraderOrder]':
        """Returns specific open order for a MetaTrader account
        (see https://metaapi.cloud/docs/client/websocket/api/readTradingTerminalState/readOrder/).

        Args:
            account_id: Id of the MetaTrader account to return information for.
            order_id: Order id (ticket number).

        Returns:
            A coroutine resolving with metatrader order found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getOrder', 'orderId': order_id})
        return response['order']

    async def get_history_orders_by_ticket(self, account_id: str, ticket: str) -> MetatraderHistoryOrders:
        """Returns the history of completed orders for a specific ticket number
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveHistoricalData/readHistoryOrdersByTicket/).

        Args:
            account_id: Id of the MetaTrader account to return information for.
            ticket: Ticket number (order id).

        Returns:
            A coroutine resolving with request results containing history orders found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getHistoryOrdersByTicket',
                                                        'ticket': ticket})
        return {
            'historyOrders': response['historyOrders'],
            'synchronizing': response['synchronizing']
        }

    async def get_history_orders_by_position(self, account_id: str, position_id: str) -> MetatraderHistoryOrders:
        """Returns the history of completed orders for a specific position id
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveHistoricalData/readHistoryOrdersByPosition/)

        Args:
            account_id: Id of the MetaTrader account to return information for.
            position_id: Position id.

        Returns:
            A coroutine resolving with request results containing history orders found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getHistoryOrdersByPosition',
                                                        'positionId': position_id})
        return {
            'historyOrders': response['historyOrders'],
            'synchronizing': response['synchronizing']
        }

    async def get_history_orders_by_time_range(self, account_id: str, start_time: datetime, end_time: datetime,
                                               offset=0, limit=1000) -> MetatraderHistoryOrders:
        """Returns the history of completed orders for a specific time range
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveHistoricalData/readHistoryOrdersByTimeRange/)

        Args:
            account_id: Id of the MetaTrader account to return information for.
            start_time: Start of time range, inclusive.
            end_time: End of time range, exclusive.
            offset: Pagination offset, default is 0.
            limit: Pagination limit, default is 1000.

        Returns:
            A coroutine resolving with request results containing history orders found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getHistoryOrdersByTimeRange',
                                                        'startTime': format_date(start_time),
                                                        'endTime': format_date(end_time),
                                                        'offset': offset, 'limit': limit})
        return {
            'historyOrders': response['historyOrders'],
            'synchronizing': response['synchronizing']
        }

    async def get_deals_by_ticket(self, account_id: str, ticket: str) -> MetatraderDeals:
        """Returns history deals with a specific ticket number
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveHistoricalData/readDealsByTicket/).

        Args:
            account_id: Id of the MetaTrader account to return information for.
            ticket: Ticket number (deal id for MT5 or order id for MT4).

        Returns:
            A coroutine resolving with request results containing deals found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getDealsByTicket',
                                                        'ticket': ticket})
        return {
            'deals': response['deals'],
            'synchronizing': response['synchronizing']
        }

    async def get_deals_by_position(self, account_id: str, position_id: str) -> MetatraderDeals:
        """Returns history deals for a specific position id
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveHistoricalData/readDealsByPosition/).

        Args:
            account_id: Id of the MetaTrader account to return information for.
            position_id: Position id.

        Returns:
            A coroutine resolving with request results containing deals found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getDealsByPosition',
                                                        'positionId': position_id})
        return {
            'deals': response['deals'],
            'synchronizing': response['synchronizing']
        }

    async def get_deals_by_time_range(self, account_id: str, start_time: datetime, end_time: datetime, offset: int = 0,
                                      limit: int = 1000) -> MetatraderDeals:
        """Returns history deals with for a specific time range
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveHistoricalData/readDealsByTimeRange/).

        Args:
            account_id: Id of the MetaTrader account to return information for.
            start_time: Start of time range, inclusive.
            end_time: End of time range, exclusive.
            offset: Pagination offset, default is 0.
            limit: Pagination limit, default is 1000.

        Returns:
            A coroutine resolving with request results containing deals found.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getDealsByTimeRange',
                                                        'startTime': format_date(start_time),
                                                        'endTime': format_date(end_time),
                                                        'offset': offset, 'limit': limit})
        return {
            'deals': response['deals'],
            'synchronizing': response['synchronizing']
        }

    def remove_history(self, account_id: str, application: str = None) -> Coroutine:
        """Clears the order and transaction history of a specified application so that it can be synchronized from
        scratch (see https://metaapi.cloud/docs/client/websocket/api/removeHistory/).

        Args:
            account_id: Id of the MetaTrader account to remove history for.
            application: Application to remove history for.

        Returns:
            A coroutine resolving when the history is cleared.
        """
        params = {'type': 'removeHistory'}
        if application:
            params['application'] = application
        return self._rpc_request(account_id, params)

    def remove_application(self, account_id: str) -> Coroutine:
        """Clears the order and transaction history of a specified application and removes the application
        (see https://metaapi.cloud/docs/client/websocket/api/removeApplication/).

        Args:
            account_id: Id of the MetaTrader account to remove history and application for.

        Returns:
            A coroutine resolving when the history is cleared.
        """
        return self._rpc_request(account_id, {'type': 'removeApplication'})

    async def trade(self, account_id: str, trade) -> 'asyncio.Future[MetatraderTradeResponse]':
        """Execute a trade on a connected MetaTrader account
        (see https://metaapi.cloud/docs/client/websocket/api/trade/).

        Args:
            account_id: Id of the MetaTrader account to execute trade for.
            trade: Trade to execute (see docs for possible trade types).

        Returns:
            A coroutine resolving with trade result.

        Raises:
            TradeException: On trade error, check error properties for error code details.
        """
        response = await self._rpc_request(account_id, {'type': 'trade', 'trade': trade})
        if 'response' not in response:
            response['response'] = {}
        if 'stringCode' not in response['response']:
            response['response']['stringCode'] = response['response']['description']
        if 'numericCode' not in response['response']:
            response['response']['numericCode'] = response['response']['error']
        if response['response']['stringCode'] in ['ERR_NO_ERROR', 'TRADE_RETCODE_PLACED', 'TRADE_RETCODE_DONE',
                                                  'TRADE_RETCODE_DONE_PARTIAL', 'TRADE_RETCODE_NO_CHANGES']:
            return response['response']
        else:
            raise TradeException(response['response']['message'], response['response']['numericCode'],
                                 response['response']['stringCode'])

    def subscribe(self, account_id: str, instance_index: int = None):
        """Subscribes to the Metatrader terminal events
        (see https://metaapi.cloud/docs/client/websocket/api/subscribe/).

        Args:
            account_id: Id of the MetaTrader account to subscribe to.
            instance_index: Instance index.

        Returns:
            A coroutine which resolves when subscription started.
        """
        packet = {'type': 'subscribe'}
        if instance_index is not None:
            packet['instanceIndex'] = instance_index
        return self._rpc_request(account_id, packet)

    def reconnect(self, account_id: str) -> Coroutine:
        """Reconnects to the Metatrader terminal (see https://metaapi.cloud/docs/client/websocket/api/reconnect/).

        Args:
            account_id: Id of the MetaTrader account to reconnect.

        Returns:
            A coroutine which resolves when reconnection started
        """
        return self._rpc_request(account_id, {'type': 'reconnect'})

    def synchronize(self, account_id: str, instance_index: int, synchronization_id: str,
                    starting_history_order_time: datetime, starting_deal_time: datetime) -> Coroutine:
        """Requests the terminal to start synchronization process.
        (see https://metaapi.cloud/docs/client/websocket/synchronizing/synchronize/).

        Args:
            account_id: Id of the MetaTrader account to synchronize.
            instance_index: Instance index.
            synchronization_id: Synchronization request id.
            starting_history_order_time: From what date to start synchronizing history orders from. If not specified,
            the entire order history will be downloaded.
            starting_deal_time: From what date to start deal synchronization from. If not specified, then all
            history deals will be downloaded.

        Returns:
            A coroutine which resolves when synchronization is started.
        """
        return self._synchronizationThrottler.schedule_synchronize(account_id, {
            'requestId': synchronization_id, 'type': 'synchronize',
            'startingHistoryOrderTime': format_date(starting_history_order_time),
            'startingDealTime': format_date(starting_deal_time),
            'instanceIndex': instance_index})

    def wait_synchronized(self, account_id: str, instance_index: int, application_pattern: str,
                          timeout_in_seconds: float):
        """Waits for server-side terminal state synchronization to complete.
        (see https://metaapi.cloud/docs/client/websocket/synchronizing/waitSynchronized/).

        Args:
            account_id: Id of the MetaTrader account to synchronize.
            instance_index: Instance index.
            application_pattern: MetaApi application regular expression pattern, default is .*
            timeout_in_seconds: Timeout in seconds, default is 300 seconds.
        """
        return self._rpc_request(account_id, {'type': 'waitSynchronized', 'applicationPattern': application_pattern,
                                              'timeoutInSeconds': timeout_in_seconds, 'instanceIndex': instance_index},
                                 timeout_in_seconds + 1)

    def subscribe_to_market_data(self, account_id: str, instance_index: int, symbol: str) -> Coroutine:
        """Subscribes on market data of specified symbol
        (see https://metaapi.cloud/docs/client/websocket/marketDataStreaming/subscribeToMarketData/).

        Args:
            account_id: Id of the MetaTrader account.
            instance_index: Instance index.
            symbol: Symbol (e.g. currency pair or an index).

        Returns:
            A coroutine which resolves when subscription request was processed.
        """
        packet = {'type': 'subscribeToMarketData', 'symbol': symbol}
        if instance_index is not None:
            packet['instanceIndex'] = instance_index
        return self._rpc_request(account_id, packet)

    def unsubscribe_from_market_data(self, account_id: str, instance_index: int, symbol: str) -> Coroutine:
        """Unsubscribes from market data of specified symbol
        (see https://metaapi.cloud/docs/client/websocket/marketDataStreaming/unsubscribeFromMarketData/).

        Args:
            account_id: Id of the MetaTrader account.
            instance_index: Instance index.
            symbol: Symbol (e.g. currency pair or an index).

        Returns:
            A coroutine which resolves when unsubscription request was processed.
        """
        packet = {'type': 'unsubscribeFromMarketData', 'symbol': symbol}
        if instance_index is not None:
            packet['instanceIndex'] = instance_index
        return self._rpc_request(account_id, packet)

    async def get_symbol_specification(self, account_id: str, symbol: str) -> \
            'asyncio.Future[MetatraderSymbolSpecification]':
        """Retrieves specification for a symbol
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveMarketData/getSymbolSpecification/).

        Args:
            account_id: Id of the MetaTrader account to retrieve symbol specification for.
            symbol: Symbol to retrieve specification for.

        Returns:
            A coroutine which resolves when specification is retrieved.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getSymbolSpecification',
                                                        'symbol': symbol})
        return response['specification']

    async def get_symbol_price(self, account_id: str, symbol: str) -> 'asyncio.Future[MetatraderSymbolPrice]':
        """Retrieves price for a symbol
        (see https://metaapi.cloud/docs/client/websocket/api/retrieveMarketData/getSymbolPrice/).

        Args:
            account_id: Id of the MetaTrader account to retrieve symbol price for.
            symbol: Symbol to retrieve price for.

        Returns:
            A coroutine which resolves when price is retrieved.
        """
        response = await self._rpc_request(account_id, {'application': 'RPC', 'type': 'getSymbolPrice',
                                                        'symbol': symbol})
        return response['price']

    def save_uptime(self, account_id: str, uptime: Dict):
        """Sends client uptime stats to the server.

        Args:
            account_id: Id of the MetaTrader account to retrieve symbol price for.
            uptime: Uptime statistics to send to the server.

        Returns:
            A coroutine which resolves when uptime statistics is submitted.
        """
        return self._rpc_request(account_id, {'type': 'saveUptime', 'uptime': uptime})

    def unsubscribe(self, account_id: str):
        """Unsubscribe from account (see https://metaapi.cloud/docs/client/websocket/api/synchronizing/unsubscribe).

        Args:
            account_id: Id of the MetaTrader account to retrieve symbol price for.

        Returns:
            A coroutine which resolves when socket is unsubscribed."""
        return self._rpc_request(account_id, {'type': 'unsubscribe'})

    def add_synchronization_listener(self, account_id: str, listener):
        """Adds synchronization listener for specific account.

        Args:
            account_id: Account id.
            listener: Synchronization listener to add.
        """
        if account_id in self._synchronizationListeners:
            listeners = self._synchronizationListeners[account_id]
        else:
            listeners = []
            self._synchronizationListeners[account_id] = listeners
        listeners.append(listener)

    def remove_synchronization_listener(self, account_id: str, listener: SynchronizationListener):
        """Removes synchronization listener for specific account.

        Args:
            account_id: Account id.
            listener: Synchronization listener to remove.
        """
        listeners = self._synchronizationListeners[account_id]

        if not listeners:
            listeners = []
        elif listeners.__contains__(listener):
            listeners.remove(listener)
        self._synchronizationListeners[account_id] = listeners

    def add_latency_listener(self, listener: LatencyListener):
        """Adds latency listener.

        Args:
            listener: Latency listener to add."""
        self._latencyListeners.append(listener)

    def remove_latency_listener(self, listener: LatencyListener):
        """Removes latency listener.

        Args:
            listener: Latency listener to remove."""
        self._latencyListeners = list(filter(lambda l: l != listener, self._latencyListeners))

    def add_reconnect_listener(self, listener: ReconnectListener):
        """Adds reconnect listener.

        Args:
            listener: Reconnect listener to add.
        """

        self._reconnectListeners.append(listener)

    def remove_reconnect_listener(self, listener: ReconnectListener):
        """Removes reconnect listener.

        Args:
            listener: Listener to remove.
        """

        if self._reconnectListeners.__contains__(listener):
            self._reconnectListeners.remove(listener)

    def remove_all_listeners(self):
        """Removes all listeners. Intended for use in unit tests."""

        self._synchronizationListeners = {}
        self._reconnectListeners = []

    async def _reconnect(self):
        reconnected = False
        while self._connected and not reconnected:
            try:
                await self._socket.disconnect()
                url = f'{self._url}?auth-token={self._token}'
                self._connectResult = asyncio.Future()
                self._resolved = False
                self._sessionId = random_id()
                await asyncio.wait_for(self._socket.connect(url, socketio_path='ws',
                                                            headers={'Client-id': '{:01.10f}'.format(random())}),
                                       timeout=self._connect_timeout)
                await self._connectResult
                reconnected = True
                await self._fire_reconnected()
                await self._socket.wait()
            except Exception:
                self._connectResult.cancel()
                self._connectResult = None
                await asyncio.sleep(1)

    async def _rpc_request(self, account_id: str, request: dict, timeout_in_seconds: float = None) -> Coroutine:
        if not self._connected:
            await self.connect()
        start_time = datetime.now()
        while not self._resolved and (start_time + timedelta(seconds=self._connect_timeout) > datetime.now()):
            await asyncio.sleep(1)
        if not self._resolved:
            raise TimeoutException(f"MetaApi websocket client request of account {account_id} timed because socket "
                                   f"client failed to connect to the server.")
        if request['type'] == 'subscribe':
            request['sessionId'] = self._sessionId
        if request['type'] in ['trade', 'subscribe']:
            return await self._make_request(account_id, request, timeout_in_seconds)
        retry_counter = 0
        while True:
            try:
                return await self._make_request(account_id, request, timeout_in_seconds)
            except Exception as err:
                if err.__class__.__name__ in ['NotSynchronizedException', 'TimeoutException',
                                              'NotAuthenticatedException', 'InternalException'] and retry_counter < \
                        self._retries:
                    await asyncio.sleep(min(pow(2, retry_counter) * self._minRetryDelayInSeconds,
                                            self._maxRetryDelayInSeconds))
                    retry_counter += 1
                else:
                    raise err

    async def _make_request(self, account_id: str, request: dict, timeout_in_seconds: float = None):
        if 'requestId' in request:
            request_id = request['requestId']
        else:
            request_id = random_id()
            request['requestId'] = request_id
        request['timestamps'] = {'clientProcessingStarted': format_date(datetime.now())}
        self._requestResolves[request_id] = asyncio.Future()
        self._requestResolves[request_id].type = request['type']
        request['accountId'] = account_id
        request['application'] = request['application'] if 'application' in request else self._application
        await self._socket.emit('request', request)
        try:
            resolve = await asyncio.wait_for(self._requestResolves[request_id], timeout=timeout_in_seconds or
                                             self._request_timeout)
        except asyncio.TimeoutError:
            raise TimeoutException(f"MetaApi websocket client request {request['requestId']} of type "
                                   f"{request['type']} timed out. Please make sure your account is connected "
                                   f"to broker before retrying your request.")
        return resolve

    def _convert_error(self, data) -> Exception:
        if data['error'] == 'ValidationError':
            return ValidationException(data['message'], data['details'])
        elif data['error'] == 'NotFoundError':
            return NotFoundException(data['message'])
        elif data['error'] == 'NotSynchronizedError':
            return NotSynchronizedException(data['message'])
        elif data['error'] == 'TimeoutError':
            return TimeoutException(data['message'])
        elif data['error'] == 'NotAuthenticatedError':
            return NotConnectedException(data['message'])
        elif data['error'] == 'TradeError':
            return TradeException(data['message'], data['numericCode'], data['stringCode'])
        elif data['error'] == 'UnauthorizedError':
            self.close()
            return UnauthorizedException(data['message'])
        else:
            return InternalException(data['message'])

    def _convert_iso_time_to_date(self, packet):
        if not isinstance(packet, str):
            for field in packet:
                value = packet[field]
                if isinstance(value, str) and re.search('time|Time', field) and not \
                        re.search('brokerTime|BrokerTime', field):
                    packet[field] = date(value)
                if isinstance(value, list):
                    for item in value:
                        self._convert_iso_time_to_date(item)
                if isinstance(value, dict):
                    self._convert_iso_time_to_date(value)
            if packet and 'timestamps' in packet:
                for field in packet['timestamps']:
                    packet['timestamps'][field] = date(packet['timestamps'][field])
            if packet and 'type' in packet and packet['type'] == 'prices':
                if 'prices' in packet:
                    for price in packet['prices']:
                        if 'timestamps' in price:
                            for field in price['timestamps']:
                                if isinstance(price['timestamps'][field], str):
                                    price['timestamps'][field] = date(price['timestamps'][field])

    async def _process_synchronization_packet(self, packet):
        try:
            packets = self._packetOrderer.restore_order(packet)
            for data in packets:
                if 'synchronizationId' in data:
                    self._synchronizationThrottler.update_synchronization_id(data['synchronizationId'])
                instance_id = data['accountId'] + ':' + str(data['instanceIndex'] if 'instanceIndex' in data else 0)
                instance_index = data['instanceIndex'] if 'instanceIndex' in data else 0
                if data['type'] == 'authenticated':
                    if 'sessionId' not in data or data['sessionId'] == self._sessionId:
                        if 'host' in data:
                            self._connectedHosts[instance_id] = data['host']

                        on_connected_tasks: List[asyncio.Task] = []

                        async def run_on_connected(listener):
                            try:
                                await listener.on_connected(instance_index, data['replicas'])
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about connected event', err)

                        if data['accountId'] in self._synchronizationListeners:
                            for listener in self._synchronizationListeners[data['accountId']]:
                                on_connected_tasks.append(asyncio.create_task(run_on_connected(listener)))
                        if len(on_connected_tasks) > 0:
                            await asyncio.wait(on_connected_tasks)
                elif data['type'] == 'disconnected':
                    if instance_id in self._connectedHosts and self._connectedHosts[instance_id] == data['host']:
                        on_disconnected_tasks: List[asyncio.Task] = []

                        async def run_on_disconnected(listener):
                            try:
                                await listener.on_disconnected(instance_index)
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about disconnected event', err)

                        if data['accountId'] in self._synchronizationListeners:
                            for listener in self._synchronizationListeners[data['accountId']]:
                                on_disconnected_tasks.append(asyncio.create_task(run_on_disconnected(listener)))
                        if len(on_disconnected_tasks) > 0:
                            await asyncio.wait(on_disconnected_tasks)
                        del self._connectedHosts[instance_id]
                elif data['type'] == 'synchronizationStarted':
                    on_sync_started_tasks: List[asyncio.Task] = []

                    async def run_on_sync_started(listener):
                        try:
                            await listener.on_synchronization_started(instance_index)
                        except Exception as err:
                            print(f'{data["accountId"]}: Failed to notify listener about synchronization started ' +
                                  'event', err)

                    if data['accountId'] in self._synchronizationListeners:
                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_sync_started_tasks.append(asyncio.create_task(run_on_sync_started(listener)))
                    if len(on_sync_started_tasks) > 0:
                        await asyncio.wait(on_sync_started_tasks)
                elif data['type'] == 'accountInformation':
                    if data['accountInformation'] and (data['accountId'] in self._synchronizationListeners):
                        on_account_information_updated_tasks: List[asyncio.Task] = []

                        async def run_on_account_info(listener):
                            try:
                                await listener.on_account_information_updated(instance_index,
                                                                              data['accountInformation'])
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about ' +
                                      'accountInformation event', err)

                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_account_information_updated_tasks.append(asyncio
                                                                        .create_task(run_on_account_info(listener)))
                        if len(on_account_information_updated_tasks) > 0:
                            await asyncio.wait(on_account_information_updated_tasks)
                elif data['type'] == 'deals':
                    if 'deals' in data:
                        for deal in data['deals']:
                            on_deal_added_tasks: List[asyncio.Task] = []

                            async def run_on_deal_added(listener):
                                try:
                                    await listener.on_deal_added(instance_index, deal)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about deals event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_deal_added_tasks.append(asyncio.create_task(run_on_deal_added(listener)))
                            if len(on_deal_added_tasks) > 0:
                                await asyncio.wait(on_deal_added_tasks)
                elif data['type'] == 'orders':
                    on_order_updated_tasks: List[asyncio.Task] = []

                    async def run_on_order_updated(listener):
                        try:
                            if 'orders' in data:
                                await listener.on_orders_replaced(instance_index, data['orders'])
                        except Exception as err:
                            print(f'{data["accountId"]}: Failed to notify listener about orders event', err)

                    if data['accountId'] in self._synchronizationListeners:
                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_order_updated_tasks.append(asyncio.create_task(run_on_order_updated(listener)))
                    if len(on_order_updated_tasks) > 0:
                        await asyncio.wait(on_order_updated_tasks)
                elif data['type'] == 'historyOrders':
                    if 'historyOrders' in data:
                        for historyOrder in data['historyOrders']:
                            on_history_order_added_tasks: List[asyncio.Task] = []

                            async def run_on_order_added(listener):
                                try:
                                    await listener.on_history_order_added(instance_index, historyOrder)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about historyOrders ' +
                                          'event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_history_order_added_tasks.append(asyncio
                                                                        .create_task(run_on_order_added(listener)))
                            if len(on_history_order_added_tasks) > 0:
                                await asyncio.wait(on_history_order_added_tasks)
                elif data['type'] == 'positions':
                    on_position_updated_tasks: List[asyncio.Task] = []

                    async def run_on_position_updated(listener):
                        try:
                            if 'positions' in data:
                                await listener.on_positions_replaced(instance_index, data['positions'])
                        except Exception as err:
                            print(f'{data["accountId"]}: Failed to notify listener about positions event', err)

                    if data['accountId'] in self._synchronizationListeners:
                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_position_updated_tasks.append(asyncio
                                                             .create_task(run_on_position_updated(listener)))
                    if len(on_position_updated_tasks) > 0:
                        await asyncio.wait(on_position_updated_tasks)
                elif data['type'] == 'update':
                    if 'accountInformation' in data and (data['accountId'] in self._synchronizationListeners):
                        on_account_information_updated_tasks: List[asyncio.Task] = []

                        async def run_on_account_information_updated(listener):
                            try:
                                await listener.on_account_information_updated(instance_index,
                                                                              data['accountInformation'])
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about update event', err)

                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_account_information_updated_tasks.append(
                                asyncio.create_task(run_on_account_information_updated(listener)))
                        if len(on_account_information_updated_tasks) > 0:
                            await asyncio.wait(on_account_information_updated_tasks)
                    if 'updatedPositions' in data:
                        for position in data['updatedPositions']:
                            on_position_updated_tasks: List[asyncio.Task] = []

                            async def run_on_position_updated(listener):
                                try:
                                    await listener.on_position_updated(instance_index, position)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about update event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_position_updated_tasks.append(
                                        asyncio.create_task(run_on_position_updated(listener)))
                            if len(on_position_updated_tasks) > 0:
                                await asyncio.wait(on_position_updated_tasks)
                    if 'removedPositionIds' in data:
                        for positionId in data['removedPositionIds']:
                            on_position_removed_tasks: List[asyncio.Task] = []

                            async def run_on_position_removed(listener):
                                try:
                                    await listener.on_position_removed(instance_index, positionId)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about update event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_position_removed_tasks.append(
                                        asyncio.create_task(run_on_position_removed(listener)))
                            if len(on_position_removed_tasks) > 0:
                                await asyncio.wait(on_position_removed_tasks)
                    if 'updatedOrders' in data:
                        for order in data['updatedOrders']:
                            on_order_updated_tasks: List[asyncio.Task] = []

                            async def run_on_order_updated(listener):
                                try:
                                    await listener.on_order_updated(instance_index, order)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about update event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_order_updated_tasks.append(
                                        asyncio.create_task(run_on_order_updated(listener)))
                            if len(on_order_updated_tasks) > 0:
                                await asyncio.wait(on_order_updated_tasks)
                    if 'completedOrderIds' in data:
                        for orderId in data['completedOrderIds']:
                            on_order_completed_tasks: List[asyncio.Task] = []

                            async def run_on_order_completed(listener):
                                try:
                                    await listener.on_order_completed(instance_index, orderId)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about update event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_order_completed_tasks.append(
                                        asyncio.create_task(run_on_order_completed(listener)))
                            if len(on_order_completed_tasks) > 0:
                                await asyncio.wait(on_order_completed_tasks)
                    if 'historyOrders' in data:
                        for historyOrder in data['historyOrders']:
                            on_history_order_added_tasks: List[asyncio.Task] = []

                            async def run_on_history_order_added(listener):
                                try:
                                    await listener.on_history_order_added(instance_index, historyOrder)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about update event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_history_order_added_tasks.append(
                                        asyncio.create_task(run_on_history_order_added(listener)))
                            if len(on_history_order_added_tasks) > 0:
                                await asyncio.wait(on_history_order_added_tasks)
                    if 'deals' in data:
                        for deal in data['deals']:
                            on_deal_added_tasks: List[asyncio.Task] = []

                            async def run_on_deal_added(listener):
                                try:
                                    await listener.on_deal_added(instance_index, deal)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about update event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_deal_added_tasks.append(
                                        asyncio.create_task(run_on_deal_added(listener)))
                            if len(on_deal_added_tasks) > 0:
                                await asyncio.wait(on_deal_added_tasks)
                    if 'timestamps' in data:
                        data['timestamps']['clientProcessingFinished'] = datetime.now()
                        on_update_tasks: List[asyncio.Task] = []

                        async def run_on_update(listener):
                            try:
                                await listener.on_update(data['accountId'], data['timestamps'])
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify latency listener about update event', err)

                        for listener in self._latencyListeners:
                            on_update_tasks.append(asyncio.create_task(run_on_update(listener)))

                        if len(on_update_tasks) > 0:
                            await asyncio.wait(on_update_tasks)
                elif data['type'] == 'dealSynchronizationFinished':
                    if data['accountId'] in self._synchronizationListeners:
                        on_deal_synchronization_finished_tasks: List[asyncio.Task] = []

                        async def run_on_deal_synchronization_finished(listener):
                            self._synchronizationThrottler.remove_synchronization_id(data['synchronizationId'])
                            try:
                                await listener.on_deal_synchronization_finished(instance_index,
                                                                                data['synchronizationId'])
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about ' +
                                      'dealSynchronizationFinished event', err)

                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_deal_synchronization_finished_tasks.append(
                                        asyncio.create_task(run_on_deal_synchronization_finished(listener)))
                        if len(on_deal_synchronization_finished_tasks) > 0:
                            await asyncio.wait(on_deal_synchronization_finished_tasks)
                elif data['type'] == 'orderSynchronizationFinished':
                    if data['accountId'] in self._synchronizationListeners:
                        on_order_synchronization_finished_tasks: List[asyncio.Task] = []

                        async def run_on_order_synchronization_finished(listener):
                            try:
                                await listener.on_order_synchronization_finished(instance_index,
                                                                                 data['synchronizationId'])
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about ' +
                                      'orderSynchronizationFinished event', err)

                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_order_synchronization_finished_tasks.append(
                                asyncio.create_task(run_on_order_synchronization_finished(listener)))
                        if len(on_order_synchronization_finished_tasks) > 0:
                            await asyncio.wait(on_order_synchronization_finished_tasks)
                elif data['type'] == 'status':
                    if instance_id not in self._connectedHosts:
                        if instance_id not in self._resubscriptionTriggerTimes:
                            self._resubscriptionTriggerTimes[instance_id] = datetime.now()
                        elif self._resubscriptionTriggerTimes[instance_id].timestamp() + 2 * 60 < \
                                datetime.now().timestamp():
                            del self._resubscriptionTriggerTimes[instance_id]
                            print(f'[{datetime.now().isoformat()}] it seems like we are not connected to a ' +
                                  'running API server yet, retrying subscription for account ' + instance_id)

                            async def run_subscribe():
                                try:
                                    await self.subscribe(data['accountId'], data['instanceIndex'])
                                except Exception as error:
                                    print(f'[{datetime.now().isoformat()}] MetaApi websocket client failed to ' +
                                          'receive subscribe response for account id ' + instance_id, error)
                            asyncio.create_task(run_subscribe())
                    elif instance_id in self._connectedHosts and self._connectedHosts[instance_id] == data['host']:
                        if instance_id in self._resubscriptionTriggerTimes:
                            del self._resubscriptionTriggerTimes[instance_id]
                        on_broker_connection_status_changed_tasks: List[asyncio.Task] = []

                        async def run_on_broker_connection_status_changed(listener):
                            try:
                                await listener.on_broker_connection_status_changed(instance_index,
                                                                                   bool(data['connected']))
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about ' +
                                      'brokerConnectionStatusChanged event', err)

                        for listener in self._synchronizationListeners[data['accountId']]:
                            on_broker_connection_status_changed_tasks.append(
                                asyncio.create_task(run_on_broker_connection_status_changed(listener)))
                        if len(on_broker_connection_status_changed_tasks) > 0:
                            await asyncio.wait(on_broker_connection_status_changed_tasks)
                        if 'healthStatus' in data:
                            on_health_status_tasks: List[asyncio.Task] = []

                            async def run_on_health_status(listener):
                                try:
                                    await listener.on_health_status(instance_index, data['healthStatus'])
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about server-side ' +
                                          'healthStatus event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_health_status_tasks.append(
                                        asyncio.create_task(run_on_health_status(listener)))
                                if len(on_health_status_tasks) > 0:
                                    await asyncio.wait(on_health_status_tasks)
                elif data['type'] == 'specifications':
                    if 'specifications' in data:
                        for specification in data['specifications']:
                            on_symbol_specification_updated_tasks: List[asyncio.Task] = []

                            async def run_on_symbol_specification_updated(listener):
                                try:
                                    await listener.on_symbol_specification_updated(instance_index, specification)
                                except Exception as err:
                                    print(f'{data["accountId"]}: Failed to notify listener about specifications ' +
                                          'event', err)

                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_symbol_specification_updated_tasks.append(
                                        asyncio.create_task(run_on_symbol_specification_updated(listener)))
                                if len(on_symbol_specification_updated_tasks) > 0:
                                    await asyncio.wait(on_symbol_specification_updated_tasks)
                elif data['type'] == 'prices':
                    if 'prices' in data:
                        prices = data['prices']
                        on_symbol_prices_updated_tasks: List[asyncio.Task] = []

                        async def run_on_symbol_prices_updated(listener):
                            try:
                                equity = data['equity'] if 'equity' in data else None
                                margin = data['margin'] if 'margin' in data else None
                                free_margin = data['freeMargin'] if 'freeMargin' in data else None
                                margin_level = data['marginLevel'] if 'marginLevel' in data else None
                                await listener.on_symbol_prices_updated(instance_index, prices, equity, margin,
                                                                        free_margin, margin_level)
                            except Exception as err:
                                print(f'{data["accountId"]}: Failed to notify listener about prices event', err)

                        if data['accountId'] in self._synchronizationListeners:
                            for listener in self._synchronizationListeners[data['accountId']]:
                                on_symbol_prices_updated_tasks.append(
                                    asyncio.create_task(run_on_symbol_prices_updated(listener)))
                            if len(on_symbol_prices_updated_tasks) > 0:
                                await asyncio.wait(on_symbol_prices_updated_tasks)
                        for price in data['prices']:
                            on_symbol_price_updated_tasks: List[asyncio.Task] = []

                            async def run_on_symbol_price_updated(listener):
                                try:
                                    await listener.on_symbol_price_updated(instance_index, price)
                                except Exception as err:
                                    print('Failed to notify listener about price event', err)
                            if data['accountId'] in self._synchronizationListeners:
                                for listener in self._synchronizationListeners[data['accountId']]:
                                    on_symbol_price_updated_tasks.append(
                                        asyncio.create_task(run_on_symbol_price_updated(listener)))
                                if len(on_symbol_price_updated_tasks) > 0:
                                    await asyncio.wait(on_symbol_price_updated_tasks)

                        for price in data['prices']:
                            if 'timestamps' in price:
                                price['timestamps']['clientProcessingFinished'] = datetime.now()
                                on_symbol_price_tasks: List[asyncio.Task] = []

                                async def run_on_symbol_price(listener):
                                    try:
                                        await listener.on_symbol_price(data['accountId'], price['symbol'],
                                                                       price['timestamps'])
                                    except Exception as err:
                                        print(f'{data["accountId"]}: Failed to notify latency listener about ' +
                                              'update event', err)
                                for listener in self._latencyListeners:
                                    on_symbol_price_tasks.append(
                                        asyncio.create_task(run_on_symbol_price(listener)))
                                if len(on_symbol_price_tasks) > 0:
                                    await asyncio.wait(on_symbol_price_tasks)
        except Exception as err:
            print('Failed to process incoming synchronization packet', err)

    async def _fire_reconnected(self):
        for listener in self._reconnectListeners:
            try:
                await listener.on_reconnected()
            except Exception as err:
                print(f'[{datetime.now().isoformat()}] Failed to notify reconnect listener', err)
