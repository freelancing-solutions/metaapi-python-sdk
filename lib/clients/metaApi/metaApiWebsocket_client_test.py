from .metaApiWebsocket_client import MetaApiWebsocketClient
from socketio import AsyncServer
from aiohttp import web
from ...metaApi.models import date, format_date
import pytest
import asyncio
import copy
import re
from urllib.parse import parse_qs
from mock import MagicMock, AsyncMock
from copy import deepcopy
from datetime import datetime
sio = None
client: MetaApiWebsocketClient = None


class FakeServer:

    def __init__(self):
        self.app = web.Application()
        self.runner = None

    async def start(self):
        port = 8080
        global sio
        sio = AsyncServer(async_mode='aiohttp')

        @sio.event
        async def connect(sid, environ):
            qs = parse_qs(environ['QUERY_STRING'])
            if ('auth-token' not in qs) or (qs['auth-token'] != ['token']):
                environ.emit({'error': 'UnauthorizedError', 'message': 'Authorization token invalid'})
                environ.close()

        sio.attach(self.app, socketio_path='ws')
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, 'localhost', port)
        await site.start()

    async def stop(self):
        await self.runner.cleanup()


@pytest.fixture(autouse=True)
async def run_around_tests():
    FinalMock.__await__ = lambda x: async_magic_close().__await__()
    fake_server = FakeServer()
    await fake_server.start()
    global client
    client = MetaApiWebsocketClient('token', {'application': 'application', 'domain': 'project-stock.agiliumlabs.cloud',
                                              'requestTimeout': 3, 'retryOpts': {'retries': 3,
                                                                                 'minDelayInSeconds': 0.1,
                                                                                 'maxDelayInSeconds': 0.5}})
    client.set_url('http://localhost:8080')
    await client.connect()
    client._resolved = True
    yield
    await client.close()
    await fake_server.stop()
    tasks = [task for task in asyncio.all_tasks() if task is not
             asyncio.tasks.current_task()]
    list(map(lambda task: task.cancel(), tasks))


# This method closes the client once the required socket event has been called
async def async_magic_close():
    await client.close()


class FinalMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super(MagicMock, self).__init__(*args, **kwargs)


FinalMock.__await__ = lambda x: async_magic_close().__await__()


class TestMetaApiWebsocketClient:
    @pytest.mark.asyncio
    async def test_retrieve_account(self):
        """Should retrieve MetaTrader account information from API."""

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

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getAccountInformation' and data['accountId'] == 'accountId' \
                    and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'accountInformation': account_information})

        actual = await client.get_account_information('accountId')
        assert actual == account_information

    @pytest.mark.asyncio
    async def test_retrieve_positions(self):
        """Should retrieve MetaTrader positions from API."""

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

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getPositions' and data['accountId'] == 'accountId' \
                    and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'positions': positions})
            else:
                raise Exception('Wrong request')

        actual = await client.get_positions('accountId')
        positions[0]['time'] = date(positions[0]['time'])
        positions[0]['updateTime'] = date(positions[0]['updateTime'])
        assert actual == positions

    @pytest.mark.asyncio
    async def test_retrieve_position(self):
        """Should retrieve MetaTrader position from API."""

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

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getPosition' and data['accountId'] == 'accountId' and data['positionId'] == '46214692' \
                    and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'position': position})

        actual = await client.get_position('accountId', '46214692')
        position['time'] = date(position['time'])
        position['updateTime'] = date(position['updateTime'])
        assert actual == position

    @pytest.mark.asyncio
    async def test_retrieve_orders(self):
        """Should retrieve MetaTrader orders from API."""

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

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getOrders' and data['accountId'] == 'accountId' \
                    and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'orders': orders})

        actual = await client.get_orders('accountId')
        orders[0]['time'] = date(orders[0]['time'])
        assert actual == orders

    @pytest.mark.asyncio
    async def test_retrieve_order(self):
        """Should retrieve MetaTrader order from API by id."""

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

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getOrder' and data['accountId'] == 'accountId' and data['orderId'] == '46871284' \
                    and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'order': order})

        actual = await client.get_order('accountId', '46871284')
        order['time'] = date(order['time'])
        assert actual == order

    @pytest.mark.asyncio
    async def test_retrieve_history_orders_by_ticket(self):
        """Should retrieve MetaTrader history orders from API by ticket."""

        history_orders = [{
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
        }]

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getHistoryOrdersByTicket' and data['accountId'] == 'accountId' and \
                    data['ticket'] == '46214692' and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'historyOrders': history_orders,
                                            'synchronizing': False})

        actual = await client.get_history_orders_by_ticket('accountId', '46214692')
        history_orders[0]['time'] = date(history_orders[0]['time'])
        history_orders[0]['doneTime'] = date(history_orders[0]['doneTime'])
        assert actual == {'historyOrders': history_orders, 'synchronizing': False}

    @pytest.mark.asyncio
    async def test_retrieve_history_orders_by_position(self):
        """Should retrieve MetaTrader history orders from API by position."""

        history_orders = [{
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
        }]

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getHistoryOrdersByPosition' and data['accountId'] == 'accountId' and \
                    data['positionId'] == '46214692' and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'historyOrders': history_orders,
                                            'synchronizing': False})

        actual = await client.get_history_orders_by_position('accountId', '46214692')
        history_orders[0]['time'] = date(history_orders[0]['time'])
        history_orders[0]['doneTime'] = date(history_orders[0]['doneTime'])
        assert actual == {'historyOrders': history_orders, 'synchronizing': False}

    @pytest.mark.asyncio
    async def test_retrieve_history_orders_by_time_range(self):
        """Should retrieve MetaTrader history orders from API by time range."""

        history_orders = [{
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
        }]

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getHistoryOrdersByTimeRange' and data['accountId'] == 'accountId' and \
                    data['startTime'] == '2020-04-15T02:45:00.000Z' and data['application'] == 'RPC' and \
                    data['endTime'] == '2020-04-15T02:46:00.000Z' and data['offset'] == 1 and data['limit'] == 100:
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'historyOrders': history_orders,
                                            'synchronizing': False})

        actual = await client.get_history_orders_by_time_range('accountId', date('2020-04-15T02:45:00.000Z'),
                                                               date('2020-04-15T02:46:00.000Z'), 1, 100)
        history_orders[0]['time'] = date(history_orders[0]['time'])
        history_orders[0]['doneTime'] = date(history_orders[0]['doneTime'])
        assert actual == {'historyOrders': history_orders, 'synchronizing': False}

    @pytest.mark.asyncio
    async def test_retrieve_deals_by_ticket(self):
        """Should retrieve MetaTrader deals from API by ticket."""

        deals = [{
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
        }]

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getDealsByTicket' and data['accountId'] == 'accountId' and \
                    data['ticket'] == '46214692' and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'deals': deals,
                                            'synchronizing': False})

        actual = await client.get_deals_by_ticket('accountId', '46214692')
        deals[0]['time'] = date(deals[0]['time'])
        assert actual == {'deals': deals, 'synchronizing': False}

    @pytest.mark.asyncio
    async def test_retrieve_deals_by_position(self):
        """Should retrieve MetaTrader deals from API by position."""

        deals = [{
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
        }]

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getDealsByPosition' and data['accountId'] == 'accountId' and \
                    data['positionId'] == '46214692' and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'deals': deals,
                                            'synchronizing': False})

        actual = await client.get_deals_by_position('accountId', '46214692')
        deals[0]['time'] = date(deals[0]['time'])
        assert actual == {'deals': deals, 'synchronizing': False}

    @pytest.mark.asyncio
    async def test_retrieve_deals_by_time_range(self):
        """Should retrieve MetaTrader deals from API by time range."""

        deals = [{
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
        }]

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getDealsByTimeRange' and data['accountId'] == 'accountId' and \
                    data['startTime'] == '2020-04-15T02:45:00.000Z' and data['application'] == 'RPC' and \
                    data['endTime'] == '2020-04-15T02:46:00.000Z' and data['offset'] == 1 and data['limit'] == 100:
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'deals': deals,
                                            'synchronizing': False})

        actual = await client.get_deals_by_time_range('accountId', date('2020-04-15T02:45:00.000Z'),
                                                      date('2020-04-15T02:46:00.000Z'), 1, 100)
        deals[0]['time'] = date(deals[0]['time'])
        assert actual == {'deals': deals, 'synchronizing': False}

    @pytest.mark.asyncio
    async def test_remove_history(self):
        """Should remove history from API."""

        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'removeHistory' and data['accountId'] == 'accountId' \
                    and data['application'] == 'app':
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.remove_history('accountId', 'app')
        assert request_received

    @pytest.mark.asyncio
    async def test_remove_application(self):
        """Should remove application from API."""

        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'removeApplication' and data['accountId'] == 'accountId' \
                    and data['application'] == 'application':
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.remove_application('accountId')
        assert request_received

    @pytest.mark.asyncio
    async def test_execute_trade(self):
        """Should execute a trade via new API version."""

        trade = {
            'actionType': 'ORDER_TYPE_SELL',
            'symbol': 'AUDNZD',
            'volume': 0.07
        }
        response = {
            'numericCode': 10009,
            'stringCode': 'TRADE_RETCODE_DONE',
            'message': 'Request completed',
            'orderId': '46870472'
        }

        @sio.on('request')
        async def on_request(sid, data):
            assert data['trade'] == trade
            if data['type'] == 'trade' and data['accountId'] == 'accountId' and data['application'] == 'application':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'response': response})

        actual = await client.trade('accountId', trade)
        assert actual == response

    @pytest.mark.asyncio
    async def test_fail_trade_on_old_api(self):
        """Should execute a trade via API and receive trade error from old API version."""

        trade = {
            'actionType': 'ORDER_TYPE_SELL',
            'symbol': 'AUDNZD',
            'volume': 0.07
        }
        response = {
            'error': 10006,
            'description': 'TRADE_RETCODE_REJECT',
            'message': 'Request rejected',
            'orderId': '46870472'
        }

        @sio.on('request')
        async def on_request(sid, data):
            assert data['trade'] == trade
            if data['type'] == 'trade' and data['accountId'] == 'accountId' and data['application'] == 'application':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'response': response})

        try:
            await client.trade('accountId', trade)
            Exception('TradeException expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TradeException'
            assert err.__str__() == 'Request rejected'
            assert err.stringCode == 'TRADE_RETCODE_REJECT'
            assert err.numericCode == 10006

    @pytest.mark.asyncio
    async def test_connect_to_terminal(self):
        """Should connect to MetaTrader terminal."""
        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'subscribe' and data['accountId'] == 'accountId' \
                    and data['application'] == 'application' and data['instanceIndex'] == 1:
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.subscribe('accountId', 1)
        await asyncio.sleep(0.05)
        assert request_received

    @pytest.mark.asyncio
    async def test_return_error_if_failed(self):
        """Should return error if connect to MetaTrader terminal failed."""
        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'subscribe' and data['accountId'] == 'accountId' \
                    and data['application'] == 'application':
                nonlocal request_received
                request_received = True
            await sio.emit('processingError', {'id': 1, 'error': 'NotAuthenticatedError', 'message': 'Error message',
                                               'requestId': data['requestId']})

        success = True
        try:
            await client.subscribe('accountId')
            await asyncio.sleep(0.05)
            success = False
        except Exception as err:
            assert err.__class__.__name__ == 'NotConnectedException'
        assert success
        assert request_received

    @pytest.mark.asyncio
    async def test_reconnect_to_terminal(self):
        """Should reconnect to MetaTrader terminal."""

        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'reconnect' and data['accountId'] == 'accountId' \
                    and data['application'] == 'application':
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.reconnect('accountId')
        assert request_received

    @pytest.mark.asyncio
    async def test_retrieve_symbol_specification(self):
        """Should retrieve symbol specification from API."""

        specification = {
            'symbol': 'AUDNZD',
            'tickSize': 0.00001,
            'minVolume': 0.01,
            'maxVolume': 100,
            'volumeStep': 0.01
        }

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getSymbolSpecification' and data['accountId'] == 'accountId' and \
                    data['symbol'] == 'AUDNZD' and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'specification': specification})

        actual = await client.get_symbol_specification('accountId', 'AUDNZD')
        assert actual == specification

    @pytest.mark.asyncio
    async def test_retrieve_symbol_price(self):
        """Should retrieve symbol price from API."""

        price = {
            'symbol': 'AUDNZD',
            'bid': 1.05297,
            'ask': 1.05309,
            'profitTickValue': 0.59731,
            'lossTickValue': 0.59736
        }

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getSymbolPrice' and data['accountId'] == 'accountId' and \
                    data['symbol'] == 'AUDNZD' and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'price': price})

        actual = await client.get_symbol_price('accountId', 'AUDNZD')
        assert actual == price

    @pytest.mark.asyncio
    async def test_send_uptime_stats(self):
        """Should send uptime stats to the server."""

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'saveUptime' and data['accountId'] == 'accountId' and \
                    data['uptime'] == {'1h': 100} and data['application'] == 'application':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})
        await client.save_uptime('accountId', {'1h': 100})

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """Should unsubscribe from account data."""

        response = {'type': 'response', 'accountId': 'accountId'}

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'unsubscribe' and data['accountId'] == 'accountId':
                await sio.emit('response', {'requestId': data['requestId'], **response})

        actual = await client.unsubscribe('accountId')
        assert actual['type'] == response['type']
        assert actual['accountId'] == response['accountId']

    @pytest.mark.asyncio
    async def test_handle_validation_exception(self):
        """Should handle ValidationError."""

        trade = {
            'actionType': 'ORDER_TYPE_SELL',
            'symbol': 'AUDNZD'
        }

        @sio.on('request')
        async def on_request(sid, data):
            await sio.emit('processingError', {'id': 1, 'error': 'ValidationError', 'message': 'Validation failed',
                           'details': [{'parameter': 'volume', 'message': 'Required value.'}],
                                               'requestId': data['requestId']})

        try:
            await client.trade('accountId', trade)
            Exception('ValidationError expected')
        except Exception as err:
            assert err.__class__.__name__ == 'ValidationException'

    @pytest.mark.asyncio
    async def test_handle_not_found_exception(self):
        """Should handle NotFoundError."""

        @sio.on('request')
        async def on_request(sid, data):
            await sio.emit('processingError',  {'id': 1, 'error': 'NotFoundError',
                                                'message': 'Position id 1234 not found',
                                                'requestId': data['requestId']})

        try:
            await client.get_position('accountId', '1234')
            Exception('NotFoundException expected')
        except Exception as err:
            assert err.__class__.__name__ == 'NotFoundException'

    @pytest.mark.asyncio
    async def test_handle_not_synchronized_exception(self):
        """Should handle NotSynchronizedError."""

        @sio.on('request')
        async def on_request(sid, data):
            await sio.emit('processingError', {'id': 1, 'error': 'NotSynchronizedError', 'message': 'Error message',
                                               'requestId': data['requestId']})

        try:
            await client.get_position('accountId', '1234')
            Exception('NotSynchronizedError expected')
        except Exception as err:
            assert err.__class__.__name__ == 'NotSynchronizedException'

    @pytest.mark.asyncio
    async def test_handle_not_connected_exception(self):
        """Should handle NotSynchronizedError."""

        @sio.on('request')
        async def on_request(sid, data):
            await sio.emit('processingError', {'id': 1, 'error': 'NotAuthenticatedError', 'message': 'Error message',
                                               'requestId': data['requestId']})

        try:
            await client.get_position('accountId', '1234')
            Exception('NotConnectedError expected')
        except Exception as err:
            assert err.__class__.__name__ == 'NotConnectedException'

    @pytest.mark.asyncio
    async def test_handle_other_exceptions(self):
        """Should handle other errors."""

        @sio.on('request')
        async def on_request(sid, data):
            await sio.emit('processingError', {'id': 1, 'error': 'Error', 'message': 'Error message',
                                               'requestId': data['requestId']})

        try:
            await client.get_position('accountId', '1234')
            Exception('InternalError expected')
        except Exception as err:
            assert err.__class__.__name__ == 'InternalException'

    @pytest.mark.asyncio
    async def test_process_auth_sync_event(self):
        """Should process authenticated synchronization event."""
        listener = MagicMock()
        listener.on_connected = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'authenticated', 'accountId': 'accountId', 'instanceIndex': 1,
                                           'replicas': 2})
        await client._socket.wait()
        listener.on_connected.assert_called_with(1, 2)

    @pytest.mark.asyncio
    async def test_process_auth_sync_event_with_session_id(self):
        """Should process authenticated synchronization event with session id."""
        listener = MagicMock()
        listener.on_connected = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'authenticated', 'accountId': 'accountId', 'instanceIndex': 2,
                                           'replicas': 4, 'sessionId': 'wrong'})
        await sio.emit('synchronization', {'type': 'authenticated', 'accountId': 'accountId', 'instanceIndex': 1,
                                           'replicas': 2, 'sessionId': client._sessionId})
        await client._socket.wait()
        assert listener.on_connected.call_count == 1
        listener.on_connected.assert_called_with(1, 2)

    @pytest.mark.asyncio
    async def test_process_broker_connection_status_event(self):
        """Should process broker connection status event."""
        listener = MagicMock()
        listener.on_broker_connection_status_changed = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'authenticated', 'accountId': 'accountId', 'host': 'ps-mpa-1',
                                           'instanceIndex': 1})
        await sio.emit('synchronization', {'type': 'status', 'accountId': 'accountId', 'host': 'ps-mpa-1',
                                           'connected': True, 'instanceIndex': 1})
        await client._socket.wait()
        listener.on_broker_connection_status_changed.assert_called_with(1, True)

    @pytest.mark.asyncio
    async def test_process_server_health_status(self):
        """Should process server-side health status event."""
        listener = MagicMock()
        listener.on_broker_connection_status_changed = AsyncMock()
        listener.on_health_status = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'authenticated', 'accountId': 'accountId', 'host': 'ps-mpa-1',
                                           'instanceIndex': 1})
        await sio.emit('synchronization', {'type': 'status', 'accountId': 'accountId', 'host': 'ps-mpa-1',
                                           'connected': True, 'healthStatus': {'restApiHealthy': True},
                                           'instanceIndex': 1})
        await client._socket.wait()
        listener.on_health_status.assert_called_with(1, {'restApiHealthy': True})

    @pytest.mark.asyncio
    async def test_process_disconnected_synchronization_event(self):
        """Should process disconnected synchronization event."""

        listener = MagicMock()
        listener.on_disconnected = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'authenticated', 'accountId': 'accountId', 'host': 'ps-mpa-1',
                                           'instanceIndex': 1})
        await sio.emit('synchronization', {'type': 'disconnected', 'accountId': 'accountId', 'host': 'ps-mpa-1',
                                           'instanceIndex': 1})
        await client._socket.wait()
        listener.on_disconnected.assert_called_with(1)

    @pytest.mark.asyncio
    async def test_synchronize_with_metatrader_terminal(self):
        """Should synchronize with MetaTrader terminal."""

        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'synchronize' and data['accountId'] == 'accountId' and \
                    data['startingHistoryOrderTime'] == '2020-01-01T00:00:00.000Z' and \
                    data['startingDealTime'] == '2020-01-02T00:00:00.000Z' and \
                    data['requestId'] == 'synchronizationId' and data['application'] == 'application' and \
                    data['instanceIndex'] == 1:
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.synchronize('accountId', 1, 'synchronizationId', date('2020-01-01T00:00:00.000Z'),
                                 date('2020-01-02T00:00:00.000Z'))
        assert request_received

    @pytest.mark.asyncio
    async def test_process_sync_started(self):
        """Should process synchronization started event."""
        listener = MagicMock()
        listener.on_synchronization_started = FinalMock()

        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'synchronizationStarted', 'accountId': 'accountId',
                                           'instanceIndex': 1})
        await client._socket.wait()
        listener.on_synchronization_started.assert_called_with(1)

    @pytest.mark.asyncio
    async def test_synchronize_account_information(self):
        """Should synchronize account information."""

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
        listener = MagicMock()
        listener.on_account_information_updated = FinalMock()

        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'accountInformation', 'accountId': 'accountId',
                                           'accountInformation': account_information, 'instanceIndex': 1})
        await client._socket.wait()
        listener.on_account_information_updated.assert_called_with(1, account_information)

    @pytest.mark.asyncio
    async def test_synchronize_positions(self):
        """Should synchronize positions."""

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
        listener = MagicMock()
        listener.on_positions_replaced = FinalMock()

        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'positions', 'accountId': 'accountId', 'positions': positions,
                                           'instanceIndex': 1})
        await client._socket.wait()
        positions[0]['time'] = date(positions[0]['time'])
        positions[0]['updateTime'] = date(positions[0]['updateTime'])
        listener.on_positions_replaced.assert_called_with(1, positions)

    @pytest.mark.asyncio
    async def test_synchronize_orders(self):
        """Should synchronize orders."""

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
        listener = MagicMock()
        listener.on_orders_replaced = FinalMock()

        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'orders', 'accountId': 'accountId', 'orders': orders,
                                           'instanceIndex': 1})
        await client._socket.wait()
        orders[0]['time'] = date(orders[0]['time'])
        listener.on_orders_replaced.assert_called_with(1, orders)

    @pytest.mark.asyncio
    async def test_synchronize_history_orders(self):
        """Should synchronize history orders."""

        history_orders = [{
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
        }]
        listener = MagicMock()
        listener.on_history_order_added = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'historyOrders', 'accountId': 'accountId',
                                           'historyOrders': history_orders, 'instanceIndex': 1})
        await client._socket.wait()
        history_orders[0]['time'] = date(history_orders[0]['time'])
        history_orders[0]['doneTime'] = date(history_orders[0]['doneTime'])
        listener.on_history_order_added.assert_called_with(1, history_orders[0])

    @pytest.mark.asyncio
    async def test_synchronize_deals(self):
        """Should synchronize deals."""

        deals = [{
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
        }]
        listener = MagicMock()
        listener.on_deal_added = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'deals', 'accountId': 'accountId', 'deals': deals,
                                           'instanceIndex': 1})
        await client._socket.wait()
        deals[0]['time'] = date(deals[0]['time'])
        listener.on_deal_added.assert_called_with(1, deals[0])

    @pytest.mark.asyncio
    async def test_process_synchronization_updates(self):
        """Should process synchronization updates."""

        update = {
            'accountInformation': {
                'broker': 'True ECN Trading Ltd',
                'currency': 'USD',
                'server': 'ICMarketsSC-Demo',
                'balance': 7319.9,
                'equity': 7306.649913200001,
                'margin': 184.1,
                'freeMargin': 7120.22,
                'leverage': 100,
                'marginLevel': 3967.58283542
            },
            'updatedPositions': [{
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
            }],
            'removedPositionIds': ['1234'],
            'updatedOrders': [{
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
            }],
            'completedOrderIds': ['2345'],
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
            }]
        }
        listener = MagicMock()
        listener.on_account_information_updated = AsyncMock()
        listener.on_position_updated = AsyncMock()
        listener.on_position_removed = AsyncMock()
        listener.on_order_updated = AsyncMock()
        listener.on_order_completed = AsyncMock()
        listener.on_history_order_added = AsyncMock()
        listener.on_deal_added = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        emit = copy.deepcopy(update)
        emit['type'] = 'update'
        emit['accountId'] = 'accountId'
        emit['instanceIndex'] = 1
        await sio.emit('synchronization', emit)
        await client._socket.wait()
        update['updatedPositions'][0]['time'] = date(update['updatedPositions'][0]['time'])
        update['updatedPositions'][0]['updateTime'] = date(update['updatedPositions'][0]['updateTime'])
        update['updatedOrders'][0]['time'] = date(update['updatedOrders'][0]['time'])
        update['historyOrders'][0]['time'] = date(update['historyOrders'][0]['time'])
        update['historyOrders'][0]['doneTime'] = date(update['historyOrders'][0]['doneTime'])
        update['deals'][0]['time'] = date(update['deals'][0]['time'])
        listener.on_account_information_updated.assert_called_with(1, update['accountInformation'])
        listener.on_position_updated.assert_called_with(1, update['updatedPositions'][0])
        listener.on_position_removed.assert_called_with(1, update['removedPositionIds'][0])
        listener.on_order_updated.assert_called_with(1, update['updatedOrders'][0])
        listener.on_order_completed.assert_called_with(1, update['completedOrderIds'][0])
        listener.on_history_order_added.assert_called_with(1, update['historyOrders'][0])
        listener.on_deal_added.assert_called_with(1, update['deals'][0])

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Should retry request on failure."""
        request_counter = 0
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

        @sio.on('request')
        async def on_request(sid, data):
            nonlocal request_counter
            if request_counter > 1 and data['type'] == 'getOrder' and data['accountId'] == 'accountId' and \
                    data['orderId'] == '46871284' and data['application'] == 'RPC':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'order': order})
            request_counter += 1

        actual = await client.get_order('accountId', '46871284')
        order['time'] = date(order['time'])
        assert actual == order

    @pytest.mark.asyncio
    async def test_not_retry_on_failure(self):
        """Should not retry request on validation error."""
        request_counter = 0

        @sio.on('request')
        async def on_request(sid, data):
            nonlocal request_counter
            if request_counter > 0 and data['type'] == 'subscribeToMarketData' and data['accountId'] == 'accountId' \
                    and data['symbol'] == 'EURUSD' and data['application'] == 'application' and \
                    data['instanceIndex'] == 1:
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})
            else:
                await sio.emit('processingError', {'id': 1, 'error': 'ValidationError', 'message': 'Validation failed',
                                                   'details': [{'parameter': 'volume', 'message': 'Required value.'}],
                                                   'requestId': data['requestId']})
            request_counter += 1
            try:
                await client.subscribe_to_market_data('accountId', 1, 'EURUSD')
                Exception('ValidationException expected')
            except Exception as err:
                assert err.__class__.__name__ == 'ValidationException'
                await client.close()
            assert request_counter == 1

    @pytest.mark.asyncio
    async def test_not_retry_trade(self):
        """Should not retry trade requests on fail."""
        request_counter = 0
        trade = {
            'actionType': 'ORDER_TYPE_SELL',
            'symbol': 'AUDNZD',
            'volume': 0.07
        }

        @sio.on('request')
        async def on_request(sid, data):
            nonlocal request_counter
            if request_counter > 0:
                pytest.fail()
            request_counter += 1

        try:
            await client.trade('accountId', trade)
            Exception('TimeoutException expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            await client.close()

    @pytest.mark.asyncio
    async def test_timeout_on_no_response(self):
        """Should return timeout error if no server response received."""

        trade = {
            'actionType': 'ORDER_TYPE_SELL',
            'symbol': 'AUDNZD',
            'volume': 0.07
        }

        @sio.on('request')
        async def on_request(sid, data):
            pass

        try:
            await client.trade('accountId', trade)
            Exception('TimeoutException expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            await client.close()

    @pytest.mark.asyncio
    async def test_subscribe_to_market_data_with_mt_terminal(self):
        """Should subscribe to market data with MetaTrader terminal."""

        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'subscribeToMarketData' and data['accountId'] == 'accountId' and \
                    data['symbol'] == 'EURUSD' and data['application'] == 'application' and data['instanceIndex'] == 1:
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.subscribe_to_market_data('accountId', 1, 'EURUSD')
        assert request_received

    @pytest.mark.asyncio
    async def test_unsubscribe_from_market_data_with_mt_terminal(self):
        """Should unsubscribe from market data with MetaTrader terminal."""

        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'unsubscribeFromMarketData' and data['accountId'] == 'accountId' and \
                    data['symbol'] == 'EURUSD' and data['application'] == 'application' and data['instanceIndex'] == 1:
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.unsubscribe_from_market_data('accountId', 1, 'EURUSD')
        assert request_received

    @pytest.mark.asyncio
    async def test_synchronize_symbol_specifications(self):
        """Should synchronize symbol specifications."""

        specifications = [{
            'symbol': 'EURUSD',
            'tickSize': 0.00001,
            'minVolume': 0.01,
            'maxVolume': 200,
            'volumeStep': 0.01
        }]
        listener = MagicMock()
        listener.on_symbol_specification_updated = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'specifications', 'accountId': 'accountId',
                                           'specifications': specifications, 'instanceIndex': 1})
        await client._socket.wait()
        listener.on_symbol_specification_updated.assert_called_with(1, specifications[0])

    @pytest.mark.asyncio
    async def test_synchronize_symbol_prices(self):
        """Should synchronize symbol prices."""

        prices = [{
            'symbol': 'AUDNZD',
            'bid': 1.05916,
            'ask': 1.05927,
            'profitTickValue': 0.602,
            'lossTickValue': 0.60203
        }]
        listener = MagicMock()
        listener.on_symbol_prices_updated = AsyncMock()
        listener.on_symbol_price_updated = FinalMock()
        client.add_synchronization_listener('accountId', listener)
        await sio.emit('synchronization', {'type': 'prices', 'accountId': 'accountId', 'prices': prices,
                                           'equity': 100, 'margin': 200, 'freeMargin': 400, 'marginLevel': 40000,
                                           'instanceIndex': 1})
        await client._socket.wait()
        listener.on_symbol_prices_updated.assert_called_with(1, prices, 100, 200, 400, 40000)
        listener.on_symbol_price_updated.assert_called_with(1, prices[0])

    @pytest.mark.asyncio
    async def test_wait_for_server_side_sync(self):
        """Should wait for server-side terminal state synchronization."""

        request_received = False

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'waitSynchronized' and data['accountId'] == 'accountId' and \
                    data['applicationPattern'] == 'app.*' and data['timeoutInSeconds'] == 10 \
                    and data['application'] == 'application' and data['instanceIndex'] == 1:
                nonlocal request_received
                request_received = True
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId']})

        await client.wait_synchronized('accountId', 1, 'app.*', 10)
        assert request_received

    @pytest.mark.asyncio
    async def test_invoke_latency_listener(self):
        """Should invoke latency listener on response."""

        account_id = None
        request_type = None
        actual_timestamps = None

        def on_response(aid, type, ts):
            nonlocal account_id
            account_id = aid
            nonlocal request_type
            request_type = type
            nonlocal actual_timestamps
            actual_timestamps = ts

        listener = MagicMock()
        listener.on_response = on_response
        client.add_latency_listener(listener)
        price = {}
        timestamps = None

        @sio.on('request')
        async def on_request(sid, data):
            if data['type'] == 'getSymbolPrice' and data['accountId'] == 'accountId' and data['symbol'] == 'AUDNZD' \
                    and data['application'] == 'RPC' and 'clientProcessingStarted' in data['timestamps']:
                nonlocal timestamps
                timestamps = deepcopy(data['timestamps'])
                timestamps['serverProcessingStarted'] = format_date(datetime.now())
                timestamps['serverProcessingFinished'] = format_date(datetime.now())
                timestamps['clientProcessingStarted'] = format_date(date(timestamps['clientProcessingStarted']))
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'price': price, 'timestamps': timestamps})

        await client.get_symbol_price('accountId', 'AUDNZD')
        await asyncio.sleep(0.05)
        assert account_id == 'accountId'
        assert request_type == 'getSymbolPrice'
        assert actual_timestamps['clientProcessingStarted'] == date(timestamps['clientProcessingStarted'])
        assert actual_timestamps['serverProcessingStarted'] == date(timestamps['serverProcessingStarted'])
        assert actual_timestamps['serverProcessingFinished'] == date(timestamps['serverProcessingFinished'])
        assert 'clientProcessingFinished' in actual_timestamps

    @pytest.mark.asyncio
    async def test_measure_price_latencies(self):
        """Should measure price streaming latencies."""
        prices = [{
            'symbol': 'AUDNZD',
            'timestamps': {
                'eventGenerated': format_date(datetime.now()),
                'serverProcessingStarted': format_date(datetime.now()),
                'serverProcessingFinished': format_date(datetime.now())
            }
        }]
        account_id = None
        symbol = None
        actual_timestamps = None
        listener = MagicMock()

        async def on_symbol_price(aid, sym, ts):
            nonlocal account_id
            account_id = aid
            nonlocal symbol
            symbol = sym
            nonlocal actual_timestamps
            actual_timestamps = ts
            await client.close()

        listener.on_symbol_price = on_symbol_price
        client.add_latency_listener(listener)
        await sio.emit('synchronization', {'type': 'prices', 'accountId': 'accountId', 'prices': prices,
                                           'equity': 100, 'margin': 200, 'freeMargin': 400, 'marginLevel': 40000})
        await client._socket.wait()
        assert account_id == 'accountId'
        assert symbol == 'AUDNZD'
        assert actual_timestamps['serverProcessingFinished'] == \
            date(prices[0]['timestamps']['serverProcessingFinished'])
        assert actual_timestamps['serverProcessingStarted'] == \
               date(prices[0]['timestamps']['serverProcessingStarted'])
        assert actual_timestamps['eventGenerated'] == \
               date(prices[0]['timestamps']['eventGenerated'])
        assert 'clientProcessingFinished' in actual_timestamps

    @pytest.mark.asyncio
    async def test_measure_update_latencies(self):
        """Should measure update latencies."""
        update = {
            'timestamps': {
                'eventGenerated': format_date(datetime.now()),
                'serverProcessingStarted': format_date(datetime.now()),
                'serverProcessingFinished': format_date(datetime.now())
            }
        }
        account_id = None
        actual_timestamps = None
        listener = MagicMock()

        async def on_update(aid, ts):
            nonlocal account_id
            account_id = aid
            nonlocal actual_timestamps
            actual_timestamps = ts
            await client.close()

        listener.on_update = on_update
        client.add_latency_listener(listener)
        await sio.emit('synchronization', {'type': 'update', 'accountId': 'accountId', **update})
        await client._socket.wait()
        assert account_id == 'accountId'
        assert actual_timestamps['serverProcessingFinished'] == \
               date(update['timestamps']['serverProcessingFinished'])
        assert actual_timestamps['serverProcessingStarted'] == \
               date(update['timestamps']['serverProcessingStarted'])
        assert actual_timestamps['eventGenerated'] == \
               date(update['timestamps']['eventGenerated'])
        assert 'clientProcessingFinished' in actual_timestamps

    @pytest.mark.asyncio
    async def test_process_trade_latency(self):
        """Should proces trade latency."""
        trade = {}
        response = {
            'numericCode': 10009,
            'stringCode': 'TRADE_RETCODE_DONE',
            'message': 'Request completed',
            'orderId': '46870472'
        }
        timestamps = {
            'clientExecutionStarted': format_date(datetime.now()),
            'serverExecutionStarted': format_date(datetime.now()),
            'serverExecutionFinished': format_date(datetime.now()),
            'tradeExecuted': format_date(datetime.now())
        }
        account_id = None
        actual_timestamps = None
        listener = MagicMock()

        def on_trade(aid, ts):
            nonlocal account_id
            account_id = aid
            nonlocal actual_timestamps
            actual_timestamps = ts

        listener.on_trade = on_trade
        client.add_latency_listener(listener)

        @sio.on('request')
        async def on_request(sid, data):
            assert data['trade'] == trade
            if data['type'] == 'trade' and data['accountId'] == 'accountId' and data['application'] == 'application':
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'response': response,
                                            'timestamps': timestamps})

        await client.trade('accountId', trade)
        assert account_id == 'accountId'
        assert actual_timestamps['clientExecutionStarted'] == \
               date(timestamps['clientExecutionStarted'])
        assert actual_timestamps['serverExecutionStarted'] == \
               date(timestamps['serverExecutionStarted'])
        assert actual_timestamps['serverExecutionFinished'] == \
               date(timestamps['serverExecutionFinished'])
        assert actual_timestamps['tradeExecuted'] == \
               date(timestamps['tradeExecuted'])
        assert 'clientProcessingFinished' in actual_timestamps

    @pytest.mark.asyncio
    async def test_reconnect(self):
        """Should reconnect to server on disconnect."""

        trade = {
            'actionType': 'ORDER_TYPE_SELL',
            'symbol': 'AUDNZD',
            'volume': 0.07
        }
        response = {
            'numericCode': 10009,
            'stringCode': 'TRADE_RETCODE_DONE',
            'message': 'Request completed',
            'orderId': '46870472'
        }
        listener = MagicMock()
        listener.on_reconnected = MagicMock()
        client.add_reconnect_listener(listener)
        request_counter = 0

        @sio.on('request')
        async def on_request(sid, data):
            print(data)
            if data['type'] == 'trade':
                nonlocal request_counter
                request_counter += 1
                await sio.emit('response', {'type': 'response', 'accountId': data['accountId'],
                                            'requestId': data['requestId'], 'response': response})
            await sio.disconnect(sid)

        await client.trade('accountId', trade)
        await asyncio.sleep(0.1)
        listener.on_reconnected.assert_called_once()
        await client.trade('accountId', trade)
        assert request_counter == 2
        await client.close()
