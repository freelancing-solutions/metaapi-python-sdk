from .streamingMetaApiConnection import StreamingMetaApiConnection
from .streamingMetaApiConnectionInstance import StreamingMetaApiConnectionInstance
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from .models import MetatraderHistoryOrders, MetatraderDeals
from ..clients.metaApi.reconnectListener import ReconnectListener
from ..clients.metaApi.synchronizationListener import SynchronizationListener
from ..clients.metaApi.clientApi_client import ClientApiClient
from .metatraderAccount import MetatraderAccount
from .connectionHealthMonitor import ConnectionHealthMonitor
from .terminalState import TerminalState
from datetime import datetime
from .memoryHistoryStorage import MemoryHistoryStorage
from mock import MagicMock, AsyncMock
from ..clients.errorHandler import ValidationException
from typing import Coroutine
import pytest
import asyncio


class MockClient(MetaApiWebsocketClient):
    def get_account_information(self, account_id: str) -> asyncio.Future:
        pass

    def get_positions(self, account_id: str) -> asyncio.Future:
        pass

    def get_position(self, account_id: str, position_id: str) -> asyncio.Future:
        pass

    def get_orders(self, account_id: str) -> asyncio.Future:
        pass

    def get_order(self, account_id: str, order_id: str) -> asyncio.Future:
        pass

    def get_history_orders_by_ticket(self, account_id: str, ticket: str) -> MetatraderHistoryOrders:
        pass

    def get_history_orders_by_position(self, account_id: str, position_id: str) -> MetatraderHistoryOrders:
        pass

    def get_history_orders_by_time_range(self, account_id: str, start_time: datetime, end_time: datetime,
                                         offset=0, limit=1000) -> MetatraderHistoryOrders:
        pass

    def get_deals_by_ticket(self, account_id: str, ticket: str) -> MetatraderDeals:
        pass

    def get_deals_by_position(self, account_id: str, position_id: str) -> MetatraderDeals:
        pass

    def get_deals_by_time_range(self, account_id: str, start_time: datetime, end_time: datetime, offset: int = 0,
                                limit: int = 1000) -> MetatraderDeals:
        pass

    def remove_history(self, account_id: str, application: str = None) -> Coroutine:
        pass

    def trade(self, account_id: str, trade) -> asyncio.Future:
        pass

    def reconnect(self, account_id: str):
        pass

    def synchronize(self, account_id: str, instance_index: str, synchronization_id: str,
                    starting_history_order_time: datetime, starting_deal_time: datetime) -> Coroutine:
        pass

    def subscribe(self, account_id: str, instance_index: str = None):
        pass

    def subscribe_to_market_data(self, account_id: str, instance_index: str, symbol: str) -> Coroutine:
        pass

    def unsubscribe_from_market_data(self, account_id: str, instance_index: str, symbol: str) -> Coroutine:
        pass

    def add_synchronization_listener(self, account_id: str, listener):
        pass

    def add_reconnect_listener(self, listener: ReconnectListener, account_id: str):
        pass

    def remove_synchronization_listener(self, account_id: str, listener: SynchronizationListener):
        pass

    def get_symbol_specification(self, account_id: str, symbol: str) -> asyncio.Future:
        pass

    def get_symbol_price(self, account_id: str, symbol: str) -> asyncio.Future:
        pass

    async def wait_synchronized(self, account_id: str, instance_index: str, application_pattern: str,
                                timeout_in_seconds: float):
        pass


class MockAccount(MetatraderAccount):

    def __init__(self, data, metatrader_account_client,
                 meta_api_websocket_client, connection_registry, application):
        super(MockAccount, self).__init__(data, metatrader_account_client, meta_api_websocket_client,
                                          connection_registry, MagicMock(), MagicMock(), application)
        self._state = 'DEPLOYED'

    @property
    def id(self):
        return 'accountId'

    @property
    def synchronization_mode(self):
        return 'user'

    @property
    def state(self):
        return self._state

    @property
    def reliability(self) -> str:
        return 'regular'

    async def reload(self):
        pass

    @property
    def account_regions(self) -> dict:
        return {
            'vint-hill': 'accountId',
            'new-york': 'accountIdReplica'
        }


subscribed_symbols: list = []
history_storage: MemoryHistoryStorage = None
account: MockAccount = None
client: MockClient = None
connection: StreamingMetaApiConnection = None
client_api_client: ClientApiClient = None
api: StreamingMetaApiConnectionInstance = None
health_monitor: ConnectionHealthMonitor = None
terminal_state: TerminalState = None


@pytest.fixture(autouse=True)
async def run_around_tests():
    global subscribed_symbols
    subscribed_symbols = ['EURUSD', 'AUDUSD']
    global terminal_state
    terminal_state = TerminalState('accountId', MagicMock())
    global health_monitor
    health_monitor = ConnectionHealthMonitor(MagicMock())
    global account
    account = MockAccount(MagicMock(), MagicMock(), MagicMock(), MagicMock(), 'MetaApi')
    global client
    client = MockClient(MagicMock(), 'token')
    client.get_url_settings = AsyncMock()
    client.ensure_subscribe = AsyncMock()
    client.subscribe = AsyncMock()
    global history_storage
    history_storage = MagicMock()
    history_storage.initialize = AsyncMock()
    history_storage.last_history_order_time = AsyncMock(return_value=datetime.now())
    history_storage.last_deal_time = AsyncMock(return_value=datetime.now())
    history_storage.on_history_order_added = AsyncMock()
    history_storage.on_deal_added = AsyncMock()
    global client_api_client
    client_api_client = MagicMock()
    client_api_client.get_hashing_ignored_field_lists = AsyncMock(return_value={
        'g1': {
            'specification': [
                'description',
            ],
            'position': [
                'time',
            ],
            'order': [
                'time',
            ]
        },
        'g2': {
            'specification': [
                'pipSize'
            ],
            'position': [
                'comment',
            ],
            'order': [
                'comment',
            ]
        }
    })
    global connection
    connection = MagicMock()
    connection.subscribed_symbols = subscribed_symbols
    connection.terminal_state = terminal_state
    connection.connect = AsyncMock()
    connection.remove_application = AsyncMock()
    connection.close = AsyncMock()
    connection.application = None
    connection.account = account
    connection.history_storage = history_storage
    connection.health_monitor = health_monitor
    global api
    api = StreamingMetaApiConnectionInstance(client, connection)
    api.terminal_state.specification = MagicMock(return_value={'symbol': 'EURUSD'})
    yield
    api.health_monitor.stop()


class TestStreamingMetaApiConnection:

    @pytest.mark.asyncio
    async def test_remove_application(self):
        """Should remove application."""
        await api.connect()
        client.remove_application = AsyncMock()
        await api.remove_application()
        connection.remove_application.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_market_buy_order(self):
        """Should create market buy order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_market_buy_order('GBPUSD', 0.07, 0.9, 2.0, {'comment': 'comment',
                                                                              'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_BUY', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'stopLoss': 0.9, 'takeProfit': 2.0,
                                                      'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None,
                                        'regular')

    @pytest.mark.asyncio
    async def test_create_market_buy_order_with_relative_sl_tp(self):
        """Should create market buy order with relative SL/TP."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_market_buy_order('GBPUSD', 0.07, {'value': 0.1, 'units': 'RELATIVE_PRICE'},
                                                   {'value': 2000, 'units': 'RELATIVE_POINTS'},
                                                   {'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_BUY', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'stopLoss': 0.1,
                                                      'stopLossUnits': 'RELATIVE_PRICE', 'takeProfit': 2000,
                                                      'takeProfitUnits': 'RELATIVE_POINTS', 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_create_market_sell_order(self):
        """Should create market sell order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_market_sell_order('GBPUSD', 0.07, 0.9, 2.0, {'comment': 'comment',
                                                                               'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_SELL', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'stopLoss': 0.9, 'takeProfit': 2.0,
                                                      'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None,
                                        'regular')

    @pytest.mark.asyncio
    async def test_create_limit_buy_order(self):
        """Should create limit buy order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_limit_buy_order('GBPUSD', 0.07, 1.0, 0.9, 2.0, {'comment': 'comment',
                                                                                  'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_BUY_LIMIT', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'openPrice': 1.0, 'stopLoss': 0.9,
                                                      'takeProfit': 2.0, 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_create_limit_sell_order(self):
        """Should create limit sell order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_limit_sell_order('GBPUSD', 0.07, 1.0, 0.9, 2.0, {'comment': 'comment',
                                                                                   'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_SELL_LIMIT', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'openPrice': 1.0, 'stopLoss': 0.9,
                                                      'takeProfit': 2.0, 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_create_stop_buy_order(self):
        """Should create stop buy order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_stop_buy_order('GBPUSD', 0.07, 1.0, 0.9, 2.0, {'comment': 'comment',
                                                                                 'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_BUY_STOP', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'openPrice': 1.0, 'stopLoss': 0.9,
                                                      'takeProfit': 2.0, 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_create_stop_sell_order(self):
        """Should create stop sell order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_stop_sell_order('GBPUSD', 0.07, 1.0, 0.9, 2.0, {'comment': 'comment',
                                                                                  'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_SELL_STOP', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'openPrice': 1.0, 'stopLoss': 0.9,
                                                      'takeProfit': 2.0, 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_create_stop_limit_buy_order(self):
        """Should create stop limit buy order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_stop_limit_buy_order('GBPUSD', 0.07, 1.5, 1.4, 0.9, 2.0, {
            'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_BUY_STOP_LIMIT', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'openPrice': 1.5, 'stopLimitPrice': 1.4,
                                                      'stopLoss': 0.9, 'takeProfit': 2.0, 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_create_stop_limit_sell_order(self):
        """Should create stop limit sell order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.create_stop_limit_sell_order('GBPUSD', 0.07, 1.0, 1.1, 2.0, 0.9, {
            'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_TYPE_SELL_STOP_LIMIT', 'symbol': 'GBPUSD',
                                                      'volume': 0.07, 'openPrice': 1.0, 'stopLimitPrice': 1.1,
                                                      'stopLoss': 2.0, 'takeProfit': 0.9, 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_modify_position(self):
        """Should modify position."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.modify_position('46870472', 2.0, 0.9)
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITION_MODIFY', 'positionId': '46870472',
                                                      'stopLoss': 2.0, 'takeProfit': 0.9}, None, 'regular')

    @pytest.mark.asyncio
    async def test_close_position_partially(self):
        """Should close position partially."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.close_position_partially('46870472', 0.9)
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITION_PARTIAL', 'positionId': '46870472',
                                                      'volume': 0.9}, None, 'regular')

    @pytest.mark.asyncio
    async def test_close_position(self):
        """Should close position."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.close_position('46870472')
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITION_CLOSE_ID', 'positionId': '46870472'},
                                        None, 'regular')

    @pytest.mark.asyncio
    async def test_close_position_by_opposite(self):
        """Should close position by an opposite one."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'positionId': '46870472',
            'closeByPositionId': '46870482'
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.close_by('46870472', '46870482', {'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'})
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITION_CLOSE_BY', 'positionId': '46870472',
                                                      'closeByPositionId': '46870482', 'comment': 'comment',
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'}, None, 'regular')

    @pytest.mark.asyncio
    async def test_close_positions_by_symbol(self):
        """Should close positions by symbol."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.close_positions_by_symbol('EURUSD')
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITIONS_CLOSE_SYMBOL', 'symbol': 'EURUSD'},
                                        None, 'regular')

    @pytest.mark.asyncio
    async def test_modify_order(self):
        """Should modify order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.modify_order('46870472', 1.0, 2.0, 0.9)
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_MODIFY', 'orderId': '46870472',
                                                      'openPrice': 1.0, 'stopLoss': 2.0, 'takeProfit': 0.9}, None,
                                        'regular')

    @pytest.mark.asyncio
    async def test_cancel_order(self):
        """Should cancel order."""
        await api.connect()
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.cancel_order('46870472')
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_CANCEL', 'orderId': '46870472'}, None,
                                        'regular')

    @pytest.mark.asyncio
    async def test_calculate_margin(self):
        """Should calculate margin."""
        await api.connect()
        margin = {
            'margin': 110
        }
        order = {
            'symbol': 'EURUSD',
            'type': 'ORDER_TYPE_BUY',
            'volume': 0.1,
            'openPrice': 1.1
        }
        client.calculate_margin = AsyncMock(return_value=margin)
        actual = await api.calculate_margin(order)
        assert actual == margin
        client.calculate_margin.assert_called_with('accountId', None, 'regular', order)

    @pytest.mark.asyncio
    async def test_save_uptime_stats(self):
        """Should save uptime stats to the server."""
        await api.connect()
        client.save_uptime = AsyncMock()
        await api.save_uptime({'1h': 100})
        client.save_uptime.assert_called_with('accountId', {'1h': 100})

    @pytest.mark.asyncio
    async def test_connect_connection(self):
        """Should connect connection."""
        await api.connect()
        connection.connect.assert_called_with(api.instance_id)
        await api.connect()
        connection.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_if_connect_failed(self):
        """Should close if connect failed."""
        connection.connect = AsyncMock(side_effect=ValidationException('test'))
        try:
            await api.connect()
            raise Exception('ValidationException expected')
        except Exception as err:
            assert err.__class__.__name__ == 'ValidationException'
        connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_sync_listeners(self):
        """Should add synchronization listeners for account with user synchronization mode."""
        client.add_synchronization_listener = MagicMock()
        await api.connect()
        listener = {}
        api.add_synchronization_listener(listener)
        client.add_synchronization_listener.assert_called_with('accountId', listener)

    @pytest.mark.asyncio
    async def test_remove_sync_listeners(self):
        """Should remove synchronization listeners."""
        client.remove_synchronization_listener = MagicMock()
        await api.connect()
        listener = {}
        api.remove_synchronization_listener(listener)
        client.remove_synchronization_listener.assert_called_with('accountId', listener)

    @pytest.mark.asyncio
    async def test_remove_sync_listeners_on_close(self):
        """Should remove synchronization listeners on close."""
        client.remove_synchronization_listener = MagicMock()
        listener = {}
        await api.connect()
        api.add_synchronization_listener(listener)
        await api.close()
        client.remove_synchronization_listener.assert_called_with('accountId', listener)
        connection.close.assert_called_with(api.instance_id)

    @pytest.mark.asyncio
    async def test_wait_until_sync_complete(self):
        """Should wait util synchronization complete."""
        await api.connect()
        connection.wait_synchronized = AsyncMock()
        opts = {'applicationPattern': 'app.*', 'synchronizationId': 'synchronizationId',
                'timeoutInSeconds': 1, 'intervalInMilliseconds': 10}
        await api.wait_synchronized(opts)
        connection.wait_synchronized.assert_called_with(opts)

    @pytest.mark.asyncio
    async def test_subscribe_to_market_data(self):
        """Should subscribe to market data."""
        await api.connect()
        connection.subscribe_to_market_data = AsyncMock()
        await api.subscribe_to_market_data('EURUSD', None)
        connection.subscribe_to_market_data.assert_called_with('EURUSD', None, None, True)

    @pytest.mark.asyncio
    async def test_unsubscribe_from_market_data(self):
        """Should unsubscribe from market data."""
        await api.connect()
        connection.unsubscribe_from_market_data = AsyncMock()
        await api.unsubscribe_from_market_data('EURUSD', [{'type': 'quotes'}])
        connection.unsubscribe_from_market_data.assert_called_with('EURUSD', [{'type': 'quotes'}])

    @pytest.mark.asyncio
    async def test_return_subscribed_symbols(self):
        """Should return subscribed symbols."""
        assert api.subscribed_symbols == subscribed_symbols

    @pytest.mark.asyncio
    async def test_return_subscriptions(self):
        """Should return subscriptions."""
        expected = [{'type': 'books'}, {'type': 'candles', 'timeframe': '1m'}]
        connection.subscriptions = MagicMock(return_value=expected)
        subscriptions = api.subscriptions('EURUSD')
        connection.subscriptions.assert_called_with('EURUSD')
        assert subscriptions == [{'type': 'books'}, {'type': 'candles', 'timeframe': '1m'}]

    @pytest.mark.asyncio
    async def test_return_terminal_state(self):
        """Should return terminal state."""
        assert api.terminal_state == terminal_state

    @pytest.mark.asyncio
    async def test_return_history_storage(self):
        """Should return history storage."""
        assert api.history_storage == history_storage

    @pytest.mark.asyncio
    async def test_return_synchronized_flag(self):
        """Should return synchronized flag."""
        assert api.synchronized

    @pytest.mark.asyncio
    async def test_return_account(self):
        """Should return account."""
        assert api.account == account

    @pytest.mark.asyncio
    async def test_return_health_monitor(self):
        """Should return health monitor."""
        assert api.health_monitor == health_monitor

    @pytest.mark.asyncio
    async def test_queue_events(self):
        """Should queue events."""
        client.queue_event = MagicMock()

        def event_callable():
            pass

        api.queue_event('test', event_callable)
        client.queue_event.assert_called_with('accountId', 'test', event_callable)
