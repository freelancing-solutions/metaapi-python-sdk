from .metaApiConnection import MetaApiConnection
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from .models import MetatraderHistoryOrders, MetatraderDeals
from ..clients.metaApi.reconnectListener import ReconnectListener
from ..clients.metaApi.synchronizationListener import SynchronizationListener
from .metatraderAccount import MetatraderAccount
from datetime import datetime, timedelta
from mock import MagicMock, AsyncMock, patch
from .models import date
from typing import Coroutine
import pytest
import asyncio
from asyncio import sleep


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
                 meta_api_websocket_client, connection_registry):
        super(MockAccount, self).__init__(data, metatrader_account_client, meta_api_websocket_client,
                                          connection_registry, MagicMock(), MagicMock())
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

    async def reload(self):
        pass


class AutoMockAccount(MetatraderAccount):
    @property
    def id(self):
        return 'accountId'

    @property
    def synchronization_mode(self):
        return 'automatic'


account: MockAccount = None
auto_account: AutoMockAccount = None
client: MockClient = None
api: MetaApiConnection = None
empty_hash = 'd41d8cd98f00b204e9800998ecf8427e'


@pytest.fixture(autouse=True)
async def run_around_tests():
    global account
    account = MockAccount(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    global auto_account
    auto_account = AutoMockAccount(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    global client
    client = MockClient(MagicMock(), 'token')
    storage = MagicMock()
    storage.last_history_order_time = AsyncMock(return_value=datetime.now())
    storage.last_deal_time = AsyncMock(return_value=datetime.now())
    global api
    api = MetaApiConnection(client, account, storage, MagicMock())
    yield
    api.health_monitor.stop()


class TestMetaApiConnection:
    @pytest.mark.asyncio
    async def test_retrieve_account_information(self):
        """Should retrieve account information."""
        account_information = {
            'broker': 'True ECN Trading Ltd',
            'currency': 'USD',
            'server': 'ICMarketsSC-Demo',
            'balance': 7319.9,
            'equity': 7306.649913200001,
            'margin': 184.1,
            'freeMargin': 7120.22,
            'leverage': 100,
            'marginLevel': 3967.58283542
        }
        client.get_account_information = AsyncMock(return_value=account_information)
        actual = await api.get_account_information()
        assert actual == account_information
        client.get_account_information.assert_called_with('accountId')

    @pytest.mark.asyncio
    async def test_retrieve_positions(self):
        """Should retrieve positions."""
        positions = [{
            'id': '46214692',
            'type': 'POSITION_TYPE_BUY',
            'symbol': 'GBPUSD',
            'magic': 1000,
            'time': '2020-04-15T02:45:06.521Z',
            'updateTime': '2020-04-15T02:45:06.521Z',
            'openPrice': 1.26101,
            'currentPrice': 1.24883,
            'currentTickValue': 1,
            'volume': 0.07,
            'swap': 0,
            'profit': -85.25999999999966,
            'commission': -0.25,
            'clientId': 'TE_GBPUSD_7hyINWqAlE',
            'stopLoss': 1.17721,
            'unrealizedProfit': -85.25999999999901,
            'realizedProfit': -6.536993168992922e-13
        }]
        client.get_positions = AsyncMock(return_value=positions)
        actual = await api.get_positions()
        assert actual == positions
        client.get_positions.assert_called_with('accountId')

    @pytest.mark.asyncio
    async def test_retrieve_position_by_id(self):
        """Should retrieve position by id."""
        position = {
            'id': '46214692',
            'type': 'POSITION_TYPE_BUY',
            'symbol': 'GBPUSD',
            'magic': 1000,
            'time': '2020-04-15T02:45:06.521Z',
            'updateTime': '2020-04-15T02:45:06.521Z',
            'openPrice': 1.26101,
            'currentPrice': 1.24883,
            'currentTickValue': 1,
            'volume': 0.07,
            'swap': 0,
            'profit': -85.25999999999966,
            'commission': -0.25,
            'clientId': 'TE_GBPUSD_7hyINWqAlE',
            'stopLoss': 1.17721,
            'unrealizedProfit': -85.25999999999901,
            'realizedProfit': -6.536993168992922e-13
        }
        client.get_position = AsyncMock(return_value=position)
        actual = await api.get_position('46214692')
        assert actual == position
        client.get_position.assert_called_with('accountId', '46214692')

    @pytest.mark.asyncio
    async def test_retrieve_orders(self):
        """Should retrieve orders."""
        orders = [{
            'id': '46871284',
            'type': 'ORDER_TYPE_BUY_LIMIT',
            'state': 'ORDER_STATE_PLACED',
            'symbol': 'AUDNZD',
            'magic': 123456,
            'platform': 'mt5',
            'time': '2020-04-20T08:38:58.270Z',
            'openPrice': 1.03,
            'currentPrice': 1.05206,
            'volume': 0.01,
            'currentVolume': 0.01,
            'comment': 'COMMENT2'
        }]
        client.get_orders = AsyncMock(return_value=orders)
        actual = await api.get_orders()
        assert actual == orders
        client.get_orders.assert_called_with('accountId')

    @pytest.mark.asyncio
    async def test_retrieve_order_by_id(self):
        """Should retrieve order by id."""
        order = {
            'id': '46871284',
            'type': 'ORDER_TYPE_BUY_LIMIT',
            'state': 'ORDER_STATE_PLACED',
            'symbol': 'AUDNZD',
            'magic': 123456,
            'platform': 'mt5',
            'time': '2020-04-20T08:38:58.270Z',
            'openPrice': 1.03,
            'currentPrice': 1.05206,
            'volume': 0.01,
            'currentVolume': 0.01,
            'comment': 'COMMENT2'
        }
        client.get_order = AsyncMock(return_value=order)
        actual = await api.get_order('46871284')
        assert actual == order
        client.get_order.assert_called_with('accountId', '46871284')

    @pytest.mark.asyncio
    async def test_retrieve_history_orders_by_ticket(self):
        """Should retrieve history orders by ticket."""
        history_orders = {
            'historyOrders': [{
                'clientId': 'TE_GBPUSD_7hyINWqAlE',
                'currentPrice': 1.261,
                'currentVolume': 0,
                'doneTime': '2020-04-15T02:45:06.521Z',
                'id': '46214692',
                'magic': 1000,
                'platform': 'mt5',
                'positionId': '46214692',
                'state': 'ORDER_STATE_FILLED',
                'symbol': 'GBPUSD',
                'time': '2020-04-15T02:45:06.260Z',
                'type': 'ORDER_TYPE_BUY',
                'volume': 0.07
                }],
            'synchronizing': False
        }
        client.get_history_orders_by_ticket = AsyncMock(return_value=history_orders)
        actual = await api.get_history_orders_by_ticket('46214692')
        assert actual == history_orders
        client.get_history_orders_by_ticket.assert_called_with('accountId', '46214692')

    @pytest.mark.asyncio
    async def test_retrieve_history_orders_by_position(self):
        """Should retrieve history orders by position."""
        history_orders = {
            'historyOrders': [{
                'clientId': 'TE_GBPUSD_7hyINWqAlE',
                'currentPrice': 1.261,
                'currentVolume': 0,
                'doneTime': '2020-04-15T02:45:06.521Z',
                'id': '46214692',
                'magic': 1000,
                'platform': 'mt5',
                'positionId': '46214692',
                'state': 'ORDER_STATE_FILLED',
                'symbol': 'GBPUSD',
                'time': '2020-04-15T02:45:06.260Z',
                'type': 'ORDER_TYPE_BUY',
                'volume': 0.07
            }],
            'synchronizing': False
        }
        client.get_history_orders_by_position = AsyncMock(return_value=history_orders)
        actual = await api.get_history_orders_by_position('46214692')
        assert actual == history_orders
        client.get_history_orders_by_position.assert_called_with('accountId', '46214692')

    @pytest.mark.asyncio
    async def test_retrieve_history_orders_by_time_range(self):
        """Should retrieve history orders by time range."""
        history_orders = {
            'historyOrders': [{
                'clientId': 'TE_GBPUSD_7hyINWqAlE',
                'currentPrice': 1.261,
                'currentVolume': 0,
                'doneTime': '2020-04-15T02:45:06.521Z',
                'id': '46214692',
                'magic': 1000,
                'platform': 'mt5',
                'positionId': '46214692',
                'state': 'ORDER_STATE_FILLED',
                'symbol': 'GBPUSD',
                'time': '2020-04-15T02:45:06.260Z',
                'type': 'ORDER_TYPE_BUY',
                'volume': 0.07
            }],
            'synchronizing': False
        }
        client.get_history_orders_by_time_range = AsyncMock(return_value=history_orders)
        start_time = datetime.now() - timedelta(seconds=1)
        end_time = datetime.now()
        actual = await api.get_history_orders_by_time_range(start_time, end_time, 1, 100)
        assert actual == history_orders
        client.get_history_orders_by_time_range.assert_called_with('accountId', start_time, end_time, 1, 100)

    @pytest.mark.asyncio
    async def test_retrieve_history_deals_by_ticket(self):
        """Should retrieve history deals by ticket."""
        deals = {
            'deals': [{
                'clientId': 'TE_GBPUSD_7hyINWqAlE',
                'commission': -0.25,
                'entryType': 'DEAL_ENTRY_IN',
                'id': '33230099',
                'magic': 1000,
                'platform': 'mt5',
                'orderId': '46214692',
                'positionId': '46214692',
                'price': 1.26101,
                'profit': 0,
                'swap': 0,
                'symbol': 'GBPUSD',
                'time': '2020-04-15T02:45:06.521Z',
                'type': 'DEAL_TYPE_BUY',
                'volume': 0.07
            }],
            'synchronizing': False
        }
        client.get_deals_by_ticket = AsyncMock(return_value=deals)
        actual = await api.get_deals_by_ticket('46214692')
        assert actual == deals
        client.get_deals_by_ticket.assert_called_with('accountId', '46214692')

    @pytest.mark.asyncio
    async def test_retrieve_history_deals_by_position(self):
        """Should retrieve history deals by position."""
        deals = {
            'deals': [{
                'clientId': 'TE_GBPUSD_7hyINWqAlE',
                'commission': -0.25,
                'entryType': 'DEAL_ENTRY_IN',
                'id': '33230099',
                'magic': 1000,
                'platform': 'mt5',
                'orderId': '46214692',
                'positionId': '46214692',
                'price': 1.26101,
                'profit': 0,
                'swap': 0,
                'symbol': 'GBPUSD',
                'time': '2020-04-15T02:45:06.521Z',
                'type': 'DEAL_TYPE_BUY',
                'volume': 0.07
            }],
            'synchronizing': False
        }
        client.get_deals_by_position = AsyncMock(return_value=deals)
        actual = await api.get_deals_by_position('46214692')
        assert actual == deals
        client.get_deals_by_position.assert_called_with('accountId', '46214692')

    @pytest.mark.asyncio
    async def test_retrieve_history_deals_by_time_range(self):
        """Should retrieve history deals by time range."""
        deals = {
            'deals': [{
                'clientId': 'TE_GBPUSD_7hyINWqAlE',
                'commission': -0.25,
                'entryType': 'DEAL_ENTRY_IN',
                'id': '33230099',
                'magic': 1000,
                'platform': 'mt5',
                'orderId': '46214692',
                'positionId': '46214692',
                'price': 1.26101,
                'profit': 0,
                'swap': 0,
                'symbol': 'GBPUSD',
                'time': '2020-04-15T02:45:06.521Z',
                'type': 'DEAL_TYPE_BUY',
                'volume': 0.07
            }],
            'synchronizing': False
        }
        client.get_deals_by_time_range = AsyncMock(return_value=deals)
        start_time = datetime.now() - timedelta(seconds=1)
        end_time = datetime.now()
        actual = await api.get_deals_by_time_range(start_time, end_time, 1, 100)
        assert actual == deals
        client.get_deals_by_time_range.assert_called_with('accountId', start_time, end_time, 1, 100)

    @pytest.mark.asyncio
    async def test_remove_history(self):
        """Should remove history."""
        client.remove_history = AsyncMock()
        api.history_storage.clear = AsyncMock()
        await api.remove_history('app')
        api.history_storage.clear.assert_called()
        client.remove_history.assert_called_with('accountId', 'app')

    @pytest.mark.asyncio
    async def test_create_market_buy_order(self):
        """Should create market buy order."""
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
                                                      'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_market_buy_order_with_relative_sl_tp(self):
        """Should create market buy order with relative SL/TP."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_market_sell_order(self):
        """Should create market sell order."""
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
                                                      'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_limit_buy_order(self):
        """Should create limit buy order."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_limit_sell_order(self):
        """Should create limit sell order."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_stop_buy_order(self):
        """Should create stop buy order."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_stop_sell_order(self):
        """Should create stop sell order."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_stop_limit_buy_order(self):
        """Should create stop limit buy order."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_create_stop_limit_sell_order(self):
        """Should create stop limit sell order."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_modify_position(self):
        """Should modify position."""
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.modify_position('46870472', 2.0, 0.9)
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITION_MODIFY', 'positionId': '46870472',
                                                      'stopLoss': 2.0, 'takeProfit': 0.9})

    @pytest.mark.asyncio
    async def test_close_position_partially(self):
        """Should close position partially."""
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.close_position_partially('46870472', 0.9)
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITION_PARTIAL', 'positionId': '46870472',
                                                      'volume': 0.9})

    @pytest.mark.asyncio
    async def test_close_position(self):
        """Should close position."""
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.close_position('46870472')
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITION_CLOSE_ID', 'positionId': '46870472'})

    @pytest.mark.asyncio
    async def test_close_position_by_opposite(self):
        """Should close position by an opposite one."""
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
                                                      'clientId': 'TE_GBPUSD_7hyINWqAlE'})

    @pytest.mark.asyncio
    async def test_close_positions_by_symbol(self):
        """Should close positions by symbol."""
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.close_positions_by_symbol('EURUSD')
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'POSITIONS_CLOSE_SYMBOL', 'symbol': 'EURUSD'})

    @pytest.mark.asyncio
    async def test_modify_order(self):
        """Should modify order."""
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.modify_order('46870472', 1.0, 2.0, 0.9)
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_MODIFY', 'orderId': '46870472',
                                                      'openPrice': 1.0, 'stopLoss': 2.0, 'takeProfit': 0.9})

    @pytest.mark.asyncio
    async def test_cancel_order(self):
        """Should cancel order."""
        trade_result = {
            'error': 10009,
            'description': 'TRADE_RETCODE_DONE',
            'orderId': 46870472
        }
        client.trade = AsyncMock(return_value=trade_result)
        actual = await api.cancel_order('46870472')
        assert actual == trade_result
        client.trade.assert_called_with('accountId', {'actionType': 'ORDER_CANCEL', 'orderId': '46870472'})

    @pytest.mark.asyncio
    async def test_reconnect_terminal(self):
        """Should reconnect terminal."""
        client.reconnect = AsyncMock()
        await api.reconnect()
        client.reconnect.assert_called_with('accountId')

    @pytest.mark.asyncio
    async def test_subscribe_to_terminal(self):
        """Should subscribe to terminal."""
        client.ensure_subscribe = AsyncMock()
        await api.subscribe()
        client.ensure_subscribe.assert_called_with('accountId')

    @pytest.mark.asyncio
    async def test_not_subscribe_if_closed(self):
        """Should not subscribe if connection is closed."""
        client.ensure_subscribe = AsyncMock()
        client.unsubscribe = AsyncMock()
        await api.close()
        await api.subscribe()
        client.ensure_subscribe.assert_not_called()

    @pytest.mark.asyncio
    async def test_synchronize_state_with_terminal(self):
        """Should synchronize state with terminal."""
        client.synchronize = AsyncMock()
        with patch('lib.metaApi.metaApiConnection.random_id', return_value='synchronizationId'):
            api = MetaApiConnection(client, account, None, MagicMock())
            await api.history_storage.on_history_order_added('1:ps-mpa-1',
                                                             {'doneTime': date('2020-01-01T00:00:00.000Z')})
            await api.history_storage.on_deal_added('1:ps-mpa-1', {'time': date('2020-01-02T00:00:00.000Z')})
            await api.synchronize('1:ps-mpa-1')
            client.synchronize.assert_called_with('accountId', 1, 'ps-mpa-1', 'synchronizationId',
                                                  date('2020-01-01T00:00:00.000Z'), date('2020-01-02T00:00:00.000Z'),
                                                  empty_hash, empty_hash, empty_hash)

    @pytest.mark.asyncio
    async def test_synchronize_state_with_terminal_from_time(self):
        """Should synchronize state with terminal from specified time."""
        client.synchronize = AsyncMock()
        with patch('lib.metaApi.metaApiConnection.random_id', return_value='synchronizationId'):
            api = MetaApiConnection(client, account, None, MagicMock(), date('2020-10-07T00:00:00.000Z'))
            await api.history_storage.on_history_order_added('1:ps-mpa-1',
                                                             {'doneTime': date('2020-01-01T00:00:00.000Z')})
            await api.history_storage.on_deal_added('1:ps-mpa-1', {'time': date('2020-01-02T00:00:00.000Z')})
            await api.synchronize('1:ps-mpa-1')
            client.synchronize.assert_called_with('accountId', 1, 'ps-mpa-1', 'synchronizationId',
                                                  date('2020-10-07T00:00:00.000Z'), date('2020-10-07T00:00:00.000Z'),
                                                  empty_hash, empty_hash, empty_hash)

    @pytest.mark.asyncio
    async def test_subscribe_to_market_data(self):
        """Should subscribe to market data."""
        client.subscribe_to_market_data = AsyncMock()
        promise = asyncio.create_task(api.subscribe_to_market_data('EURUSD', None, 1))
        await api.terminal_state.on_symbol_prices_updated('1:ps-mpa-1', [{'time': datetime.fromtimestamp(1000000),
                                                          'symbol': 'EURUSD', 'bid': 1, 'ask': 1.1}])
        await promise
        assert 'EURUSD' in api.subscribed_symbols
        client.subscribe_to_market_data.assert_called_with('accountId', 1, 'EURUSD', [{'type': 'quotes'}])
        assert api.subscriptions('EURUSD') == [{'type': 'quotes'}]
        await api.subscribe_to_market_data('EURUSD', [{'type': 'books'}, {'type': 'candles', 'timeframe': '1m'}], 1)
        assert api.subscriptions('EURUSD') == [{'type': 'quotes'}, {'type': 'books'},
                                               {'type': 'candles', 'timeframe': '1m'}]
        await api.subscribe_to_market_data('EURUSD', [{'type': 'quotes'}, {'type': 'candles', 'timeframe': '5m'}], 1)
        assert api.subscriptions('EURUSD') == [{'type': 'quotes'}, {'type': 'books'},
                                               {'type': 'candles', 'timeframe': '1m'},
                                               {'type': 'candles', 'timeframe': '5m'}]

    @pytest.mark.asyncio
    async def test_unsubscribe_from_market_data(self):
        """Should unsubscribe from market data."""
        client.subscribe_to_market_data = AsyncMock()
        client.unsubscribe_from_market_data = AsyncMock()
        await api.terminal_state.on_symbol_prices_updated('1:ps-mpa-1', [{'time': datetime.fromtimestamp(1000000),
                                                                          'symbol': 'EURUSD', 'bid': 1, 'ask': 1.1}])
        await api.subscribe_to_market_data('EURUSD', [{'type': 'quotes'}], 1)
        assert 'EURUSD' in api.subscribed_symbols
        await api.unsubscribe_from_market_data('EURUSD', [{'type': 'quotes'}], 1)
        assert 'EURUSD' not in api.subscribed_symbols
        client.unsubscribe_from_market_data.assert_called_with('accountId', 1, 'EURUSD', [{'type': 'quotes'}])
        await api.subscribe_to_market_data('EURUSD', [{'type': 'quotes'}, {'type': 'books'},
                                                      {'type': 'candles', 'timeframe': '1m'},
                                                      {'type': 'candles', 'timeframe': '5m'}], 1)
        assert api.subscriptions('EURUSD') == [{'type': 'quotes'}, {'type': 'books'},
                                               {'type': 'candles', 'timeframe': '1m'},
                                               {'type': 'candles', 'timeframe': '5m'}]
        await api.unsubscribe_from_market_data('EURUSD', [{'type': 'quotes'},
                                                          {'type': 'candles', 'timeframe': '5m'}], 1)
        assert api.subscriptions('EURUSD') == [{'type': 'books'}, {'type': 'candles', 'timeframe': '1m'}]

    @pytest.mark.asyncio
    async def test_unsubscribe_during_subscription_downgrade(self):
        """Should unsubscribe during market data subscription downgrade."""
        api.subscribe_to_market_data = AsyncMock()
        api.unsubscribe_from_market_data = AsyncMock()
        await api.on_subscription_downgraded('1:ps-mpa-1', 'EURUSD', None, [{'type': 'ticks'}, {'type': 'books'}])
        api.unsubscribe_from_market_data.assert_called_with('EURUSD', [{'type': 'ticks'}, {'type': 'books'}])
        api.subscribe_to_market_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_market_data_subscription_on_downgrade(self):
        """Should update market data subscription on downgrade."""
        api.subscribe_to_market_data = AsyncMock()
        api.unsubscribe_from_market_data = AsyncMock()
        await api.on_subscription_downgraded('1:ps-mpa-1', 'EURUSD',
                                             [{'type': 'quotes', 'intervalInMilliseconds': 30000}])
        api.subscribe_to_market_data.assert_called_with('EURUSD', [{'type': 'quotes', 'intervalInMilliseconds': 30000}])
        api.unsubscribe_from_market_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_retrieve_symbols(self):
        """Should retrieve symbols."""
        symbols = ['EURUSD']
        client.get_symbols = AsyncMock(return_value=symbols)
        actual = await api.get_symbols()
        assert actual == symbols
        client.get_symbols.assert_called_with('accountId')

    @pytest.mark.asyncio
    async def test_retrieve_symbol_specification(self):
        """Should retrieve symbol specification."""
        specification = {
            'symbol': 'AUDNZD',
            'tickSize': 0.00001,
            'minVolume': 0.01,
            'maxVolume': 100,
            'volumeStep': 0.01
        }
        client.get_symbol_specification = AsyncMock(return_value=specification)
        actual = await api.get_symbol_specification('AUDNZD')
        assert actual == specification
        client.get_symbol_specification.assert_called_with('accountId', 'AUDNZD')

    @pytest.mark.asyncio
    async def test_retrieve_symbol_price(self):
        """Should retrieve symbol price."""
        price = {
            'symbol': 'AUDNZD',
            'bid': 1.05297,
            'ask': 1.05309,
            'profitTickValue': 0.59731,
            'lossTickValue': 0.59736
        }
        client.get_symbol_price = AsyncMock(return_value=price)
        actual = await api.get_symbol_price('AUDNZD')
        assert actual == price

        client.get_symbol_price.assert_called_with('accountId', 'AUDNZD')

    @pytest.mark.asyncio
    async def test_retrieve_current_candle(self):
        """Should retrieve current candle."""
        candle = {
            'symbol': 'AUDNZD',
            'timeframe': '15m',
            'time': '2020-04-07T03:45:00.000Z',
            'brokerTime': '2020-04-07 06:45:00.000',
            'open': 1.03297,
            'high': 1.06309,
            'low': 1.02705,
            'close': 1.043,
            'tickVolume': 1435,
            'spread': 17,
            'volume': 345
        }
        client.get_candle = AsyncMock(return_value=candle)
        actual = await api.get_candle('AUDNZD', '15m')
        candle['time'] = date(candle['time'])
        assert actual == candle
        client.get_candle.assert_called_with('accountId', 'AUDNZD', '15m')

    @pytest.mark.asyncio
    async def test_retrieve_latest_tick(self):
        """Should retrieve latest tick."""
        tick = {
            'symbol': 'AUDNZD',
            'time': '2020-04-07T03:45:00.000Z',
            'brokerTime': '2020-04-07 06:45:00.000',
            'bid': 1.05297,
            'ask': 1.05309,
            'last': 0.5298,
            'volume': 0.13,
            'side': 'buy'
        }
        client.get_tick = AsyncMock(return_value=tick)
        actual = await api.get_tick('AUDNZD')
        tick['time'] = date(tick['time'])
        assert actual == tick
        client.get_tick.assert_called_with('accountId', 'AUDNZD')

    @pytest.mark.asyncio
    async def test_retrieve_latest_order_book(self):
        """Should retrieve latest order book."""
        book = {
            'symbol': 'AUDNZD',
            'time': '2020-04-07T03:45:00.000Z',
            'brokerTime': '2020-04-07 06:45:00.000',
            'book': [
                {
                    'type': 'BOOK_TYPE_SELL',
                    'price': 1.05309,
                    'volume': 5.67
                },
                {
                    'type': 'BOOK_TYPE_BUY',
                    'price': 1.05297,
                    'volume': 3.45
                }
            ]
        }
        client.get_book = AsyncMock(return_value=book)
        actual = await api.get_book('AUDNZD')
        book['time'] = date(book['time'])
        assert actual == book
        client.get_book.assert_called_with('accountId', 'AUDNZD')

    @pytest.mark.asyncio
    async def test_save_uptime_stats(self):
        """Should save uptime stats to the server."""
        client.save_uptime = AsyncMock()
        await api.save_uptime({'1h': 100})
        client.save_uptime.assert_called_with('accountId', {'1h': 100})

    @pytest.mark.asyncio
    async def test_initialize(self):
        """Should initialize listeners, terminal state and history storage for accounts with user sync mode."""
        client.add_synchronization_listener = MagicMock()
        api = MetaApiConnection(client, account, MagicMock(), MagicMock())
        assert api.terminal_state
        assert api.history_storage
        client.add_synchronization_listener.assert_any_call('accountId', api)
        client.add_synchronization_listener.assert_any_call('accountId', api.terminal_state)
        client.add_synchronization_listener.assert_any_call('accountId', api.history_storage)

    @pytest.mark.asyncio
    async def test_add_sync_listeners(self):
        """Should add synchronization listeners for account with user synchronization mode."""
        client.add_synchronization_listener = MagicMock()
        api = MetaApiConnection(client, account, MagicMock(), MagicMock())
        listener = {}
        api.add_synchronization_listener(listener)
        client.add_synchronization_listener.assert_called_with('accountId', listener)

    @pytest.mark.asyncio
    async def test_remove_sync_listeners(self):
        """Should remove synchronization listeners for account with user synchronization mode."""
        client.remove_synchronization_listener = MagicMock()
        api = MetaApiConnection(client, account, MagicMock(), MagicMock())
        listener = {}
        api.remove_synchronization_listener(listener)
        client.remove_synchronization_listener.assert_called_with('accountId', listener)

    @pytest.mark.asyncio
    async def test_sync_on_connection(self):
        """Should synchronize on connection."""
        with patch('lib.metaApi.metaApiConnection.random_id', return_value='synchronizationId'):
            client.synchronize = AsyncMock()
            api = MetaApiConnection(client, account, None, MagicMock())
            await api.history_storage.on_history_order_added('1:ps-mpa-1',
                                                             {'doneTime': date('2020-01-01T00:00:00.000Z')})
            await api.history_storage.on_deal_added('1:ps-mpa-1', {'time': date('2020-01-02T00:00:00.000Z')})
            await api.on_connected('1:ps-mpa-1', 1)
            await asyncio.sleep(0.05)
            client.synchronize.assert_called_with('accountId', 1, 'ps-mpa-1', 'synchronizationId',
                                                  date('2020-01-01T00:00:00.000Z'), date('2020-01-02T00:00:00.000Z'),
                                                  empty_hash, empty_hash, empty_hash)

    @pytest.mark.asyncio
    async def test_maintain_sync(self):
        """Should maintain synchronization if connection has failed."""
        with patch('lib.metaApi.metaApiConnection.random_id', return_value='synchronizationId'):
            client.synchronize = AsyncMock(side_effect=[Exception('test error'), None])
            api = MetaApiConnection(client, account, None, MagicMock())
            await api.history_storage.on_history_order_added('1:ps-mpa-1',
                                                             {'doneTime': date('2020-01-01T00:00:00.000Z')})
            await api.history_storage.on_deal_added('1:ps-mpa-1', {'time': date('2020-01-02T00:00:00.000Z')})
            await api.on_connected('1:ps-mpa-1', 1)
            await asyncio.sleep(0.05)
            client.synchronize.assert_called_with('accountId', 1,  'ps-mpa-1', 'synchronizationId',
                                                  date('2020-01-01T00:00:00.000Z'), date('2020-01-02T00:00:00.000Z'),
                                                  empty_hash, empty_hash, empty_hash)

    @pytest.mark.asyncio
    async def test_not_sync_if_connection_closed(self):
        """Should not synchronize if connection is closed."""
        with patch('lib.metaApi.metaApiConnection.random_id', return_value='synchronizationId'):
            client.synchronize = AsyncMock()
            client.unsubscribe = AsyncMock()
            api = MetaApiConnection(client, account, None, MagicMock())
            await api.history_storage.on_history_order_added('1:ps-mpa-1',
                                                             {'doneTime': date('2020-01-01T00:00:00.000Z')})
            await api.history_storage.on_deal_added('1:ps-mpa-1', {'time': date('2020-01-02T00:00:00.000Z')})
            await api.close()
            await api.on_connected('1:ps-mpa-1', 1)
            client.synchronize.assert_not_called()

    @pytest.mark.asyncio
    async def test_restore_market_data_subs_on_sync(self):
        """Should restore market data subscriptions on synchronization."""
        call_count = 0

        def get_price(symbol):
            nonlocal call_count
            call_count += 1
            if call_count == 6:
                return None
            return {'symbol': symbol}

        client.subscribe_to_market_data = AsyncMock()
        api.terminal_state.price = get_price
        await api.subscribe_to_market_data('EURUSD')
        await api.subscribe_to_market_data('AUDNZD')
        client.subscribe_to_market_data = AsyncMock()
        await api.on_account_information_updated('1:ps-mpa-1', {})
        await asyncio.sleep(0.05)
        assert client.subscribe_to_market_data.call_count == 1
        client.subscribe_to_market_data.assert_called_with('accountId', 1, 'AUDNZD', [{'type': 'quotes'}])

    @pytest.mark.asyncio
    async def test_unsubscribe_from_events_on_close(self):
        """Should unsubscribe from events on close."""
        client.add_synchronization_listener = MagicMock()
        client.remove_synchronization_listener = MagicMock()
        client.unsubscribe = AsyncMock()
        api = MetaApiConnection(client, account, MagicMock(), MagicMock())
        await api.close()
        client.unsubscribe.assert_any_call('accountId')
        client.remove_synchronization_listener.assert_any_call('accountId', api)
        client.remove_synchronization_listener.assert_any_call('accountId', api.terminal_state)
        client.remove_synchronization_listener.assert_any_call('accountId', api.history_storage)

    @pytest.mark.timeout(60)
    @pytest.mark.asyncio
    async def test_wait_sync_complete_user_mode(self):
        """Should wait until synchronization complete."""
        assert not (await api.is_synchronized('1:ps-mpa-1'))
        api._historyStorage.update_disk_storage = AsyncMock()
        try:
            await api.wait_synchronized({'applicationPattern': 'app.*', 'synchronizationId': 'synchronizationId',
                                         'timeoutInSeconds': 1, 'intervalInMilliseconds': 10})
            raise Exception('TimeoutError is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
        await api.on_history_orders_synchronized('1:ps-mpa-1', 'synchronizationId')
        await api.on_deals_synchronized('1:ps-mpa-1', 'synchronizationId')
        promise = api.wait_synchronized({'applicationPattern': 'app.*', 'synchronizationId': 'synchronizationId',
                                         'timeoutInSeconds': 1, 'intervalInMilliseconds': 10})
        start_time = datetime.now()
        await promise
        assert pytest.approx(10, 10) == (datetime.now() - start_time).seconds * 1000
        assert (await api.is_synchronized('1:ps-mpa-1', 'synchronizationId'))

    @pytest.mark.asyncio
    async def test_time_out_waiting_for_sync(self):
        """Should time out waiting for synchronization complete."""
        try:
            await api.wait_synchronized({'applicationPattern': 'app.*', 'synchronizationId': 'synchronizationId',
                                         'timeoutInSeconds': 1, 'intervalInMilliseconds': 10})
            raise Exception('TimeoutError is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
        assert not (await api.is_synchronized('1:ps-mpa-1', 'synchronizationId'))

    @pytest.mark.asyncio
    async def test_load_history_storage_from_disk(self):
        """Should load data to history storage from disk."""
        api._historyStorage.initialize = AsyncMock()
        await api.initialize()
        api._historyStorage.initialize.assert_called()

    @pytest.mark.asyncio
    async def test_disconnect(self):
        """Should set synchronized false on disconnect."""
        client.synchronize = AsyncMock()
        await api.on_connected('1:ps-mpa-1', 2)
        await asyncio.sleep(0.05)
        assert api.synchronized
        client.subscribe = AsyncMock()
        account.reload = AsyncMock()
        await api.on_disconnected('1:ps-mpa-1')
        assert not api.synchronized

    @pytest.mark.asyncio
    async def test_on_stream_closed(self):
        """Should delete state if stream closed."""
        client.synchronize = AsyncMock()
        await api.on_connected('1:ps-mpa-1', 2)
        await asyncio.sleep(0.05)
        assert api.synchronized
        await api.on_stream_closed('1:ps-mpa-1')
        assert not api.synchronized

    @pytest.mark.asyncio
    async def test_create_refresh_market_data_subscriptions_job(self):
        """Should create refresh subscriptions job."""
        with patch('lib.metaApi.metaApiConnection.asyncio.sleep', new=lambda x: sleep(x / 10)):
            with patch('lib.metaApi.metaApiConnection.uniform', new=MagicMock(return_value=1)):
                client.refresh_market_data_subscriptions = AsyncMock()
                client.subscribe_to_market_data = AsyncMock()
                client.add_synchronization_listener = MagicMock()
                client.remove_synchronization_listener = MagicMock()
                client.unsubscribe = AsyncMock()
                api.terminal_state.wait_for_price = AsyncMock()
                await api.on_synchronization_started('1:ps-mpa-1')
                await sleep(0.05)
                client.refresh_market_data_subscriptions.assert_called_with('accountId', 1, [])
                await api.subscribe_to_market_data('EURUSD', [{'type': 'quotes'}], 1)
                await sleep(0.11)
                client.refresh_market_data_subscriptions.assert_called_with(
                    'accountId', 1, [{'symbol': 'EURUSD', 'subscriptions': [{'type': 'quotes'}]}])
                assert client.refresh_market_data_subscriptions.call_count == 2
                await api.on_synchronization_started('1:ps-mpa-1')
                await sleep(0.05)
                assert client.refresh_market_data_subscriptions.call_count == 3
                await api.close()
                await sleep(0.11)
                assert client.refresh_market_data_subscriptions.call_count == 3
