import asyncio

from .equityChartListener import EquityChartListener
from .equityChartStreamManager import EquityChartStreamManager
from ..domain_client import DomainClient
from .... import MetaApi, SynchronizationListener
from ....metaApi.streamingMetaApiConnectionInstance import StreamingMetaApiConnectionInstance
from .equityTracking_client import EquityTrackingClient
from mock import MagicMock, AsyncMock, patch
from asyncio import sleep, create_task
import pytest

token = 'header.payload.sign'
account_information = {
    'broker': 'True ECN Trading Ltd',
    'currency': 'USD',
    'server': 'ICMarketsSC-Demo',
    'balance': 11000,
    'equity': 7306.649913200001,
    'margin': 184.1,
    'freeMargin': 7120.22,
    'leverage': 100,
    'marginLevel': 3967.58283542
}
domain_client = DomainClient(MagicMock(), token, 'risk-management-api-v1')
meta_api: MetaApi = MagicMock()
equity_tracking_client: EquityTrackingClient = MagicMock()
equity_chart_stream_manager = EquityChartStreamManager(domain_client, equity_tracking_client, meta_api)
call_stub = MagicMock()
connected_stub = MagicMock()
finish_stub = MagicMock()
disconnected_stub = MagicMock()
error_stub = MagicMock()
get_equity_chart_stub = AsyncMock()
listener = EquityChartListener()
account = MagicMock()
sync_listener: SynchronizationListener = None
results = {}
connection: StreamingMetaApiConnectionInstance = MagicMock()


@pytest.fixture(autouse=True)
async def run_around_tests():
    global domain_client
    domain_client = DomainClient(MagicMock(), token, 'risk-management-api-v1')
    global meta_api
    meta_api = MagicMock()
    global equity_tracking_client
    equity_tracking_client = MagicMock()
    global equity_chart_stream_manager
    equity_chart_stream_manager = EquityChartStreamManager(domain_client, equity_tracking_client, meta_api)
    global call_stub
    call_stub = MagicMock()
    global finish_stub
    finish_stub = MagicMock()
    global connected_stub
    connected_stub = MagicMock()
    global disconnected_stub
    disconnected_stub = MagicMock()
    global error_stub
    error_stub = MagicMock()

    def add_sync_listener(lis):
        global sync_listener
        sync_listener = lis

    global connection
    connection = MagicMock()
    connection.connect = AsyncMock()
    connection.wait_synchronized = AsyncMock()
    connection.close = AsyncMock()
    connection.add_synchronization_listener = MagicMock(side_effect=add_sync_listener)
    connection.terminal_state.account_information = account_information

    class Listener(EquityChartListener):
        async def on_equity_record_updated(self, equity_chart_event):
            call_stub(equity_chart_event)

        async def on_equity_record_completed(self):
            finish_stub()

        async def on_connected(self):
            connected_stub()

        async def on_disconnected(self):
            disconnected_stub()

        async def on_error(self, error: Exception):
            error_stub(error)

    global listener
    listener = Listener()

    global account
    account = MagicMock()
    account.wait_deployed = AsyncMock()
    account.get_streaming_connection = MagicMock(return_value=connection)
    meta_api.metatrader_account_api.get_account = AsyncMock(return_value=account)
    global results
    results = [
        {
            'minBalance': 10000,
            'averageBalance': 10100,
            'maxBalance': 10200,
            'minEquity': 11000,
            'averageEquity': 11100,
            'maxEquity': 11200,
            'balanceSum': 50000,
            'duration': 2000000,
            'brokerTime': '2020-05-10 12:50:50.000',
            'endBrokerTime': '2020-05-10 12:59:59.999',
            'equitySum': 6000000000,
            'startBrokerTime': '2020-05-10 12:00:00.000',
            'lastBalance': 10100,
            'lastEquity': 11100
        },
        {
            'minBalance': 10000,
            'averageBalance': 10100,
            'maxBalance': 10200,
            'minEquity': 11000,
            'averageEquity': 11100,
            'maxEquity': 11200,
            'balanceSum': 50000,
            'duration': 2000000,
            'brokerTime': '2020-05-10 13:50:50.000',
            'endBrokerTime': '2020-05-10 13:59:59.999',
            'equitySum': 6000000000,
            'startBrokerTime': '2020-05-10 13:00:00.000',
            'lastBalance': 10100,
            'lastEquity': 11100
        }
    ]
    global get_equity_chart_stub
    get_equity_chart_stub = AsyncMock(return_value=results)
    equity_tracking_client.get_equity_chart = get_equity_chart_stub


class TestEquityChartStreamManager:

    @pytest.mark.asyncio
    async def test_add_listener_and_request_events(self):
        """Should add listener and request events."""
        listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
        await sleep(0.05)
        listener_id = await listener_id
        equity_chart_stream_manager.remove_equity_chart_listener(listener_id)
        call_stub.assert_called_with(results)

    @pytest.mark.asyncio
    async def test_process_price_events(self):
        """Should process price events."""
        listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
        await sleep(0.05)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1', {'equity': 10600, 'balance': 9000})
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
              'symbol': 'EURUSD',
              'bid': 1.02273,
              'ask': 1.02274,
              'brokerTime': '2020-05-10 13:55:00.000',
              'profitTickValue': 1,
              'lossTickValue': 1,
              'accountCurrencyExchangeRate': 1,
              'equity': 10500
        })
        await sleep(0.05)
        equity_tracking_client.get_equity_chart = AsyncMock(return_value=[results[1], {
              'minBalance': 11000,
              'averageBalance': 11500,
              'maxBalance': 12000,
              'minEquity': 12000,
              'averageEquity': 12100,
              'maxEquity': 12200,
              'balanceSum': 50000,
              'duration': 2000000,
              'brokerTime': '2020-05-10 14:01:00.000',
              'endBrokerTime': '2020-05-10 14:59:59.999',
              'equitySum': 6000000000,
              'startBrokerTime': '2020-05-10 14:00:00.000',
              'lastBalance': 12000,
              'lastEquity': 12000
        }])
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
              'symbol': 'EURUSD',
              'bid': 1.02273,
              'ask': 1.02274,
              'brokerTime': '2020-05-10 14:01:00.000',
              'profitTickValue': 1,
              'lossTickValue': 1,
              'accountCurrencyExchangeRate': 1,
              'equity': 10500
        })
        await sleep(0.05)
        assert finish_stub.call_count == 1
        call_stub.assert_any_call([{
              'averageBalance': 10100,
              'averageEquity': 11100,
              'balanceSum': 50000,
              'brokerTime': '2020-05-10 13:50:50.000',
              'duration': 2000000,
              'endBrokerTime': '2020-05-10 13:59:59.999',
              'equitySum': 6000000000,
              'maxBalance': 10200,
              'maxEquity': 11200,
              'minBalance': 10000,
              'minEquity': 11000,
              'startBrokerTime': '2020-05-10 13:00:00.000',
              'lastBalance': 10100,
              'lastEquity': 11100
            }, {
              'minBalance': 11000,
              'averageBalance': 11500,
              'maxBalance': 12000,
              'minEquity': 12000,
              'averageEquity': 12100,
              'maxEquity': 12200,
              'balanceSum': 50000,
              'duration': 2000000,
              'brokerTime': '2020-05-10 14:01:00.000',
              'endBrokerTime': '2020-05-10 14:59:59.999',
              'equitySum': 6000000000,
              'startBrokerTime': '2020-05-10 14:00:00.000',
              'lastBalance': 12000,
              'lastEquity': 12000
        }])
        listener_id = await listener_id
        equity_chart_stream_manager.remove_equity_chart_listener(listener_id)

    @pytest.mark.asyncio
    async def test_not_process_price_events(self):
        """Should not process price events if new period not received yet."""
        listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
        await sleep(0.05)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1', {'equity': 10600, 'balance': 9000})
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-10 13:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await sleep(0.05)
        get_equity_chart_stub.return_value = [results[1]]
        create_task(sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-10 14:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        }))
        await sleep(0.05)
        call_stub.assert_any_call(results)
        call_stub.assert_any_call([{
            'startBrokerTime': '2020-05-10 13:00:00.000',
            'endBrokerTime': '2020-05-10 13:59:59.999',
            'averageBalance': 1000.0222222222222,
            'minBalance': 9000,
            'maxBalance': 11000,
            'averageEquity': 3478,
            'minEquity': 10500,
            'maxEquity': 11200,
            'lastBalance': 11000,
            'lastEquity': 10500
        }])
        assert call_stub.call_count == 2
        create_task(sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-10 14:02:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        }))
        await sleep(0.05)
        create_task(sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-10 14:03:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        }))
        await sleep(0.05)
        assert call_stub.call_count == 2
        listener_id = await listener_id
        equity_chart_stream_manager.remove_equity_chart_listener(listener_id)

    @pytest.mark.asyncio
    async def test_retry_on_sync_error(self):
        """Should retry on synchronization error."""
        with patch('lib.riskManagement.clients.equityTracking.equityChartStreamManager.asyncio.sleep',
                   new=lambda x: sleep(x / 20)):
            account.wait_deployed = AsyncMock(side_effect=[TimeoutError(), TimeoutError(), True])
            listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
            await sleep(0.25)
            listener_id = await listener_id
            equity_chart_stream_manager.remove_equity_chart_listener(listener_id)
            call_stub.assert_any_call(results)
            assert error_stub.call_count == 2

    @pytest.mark.asyncio
    async def test_retry_on_get_equity_chart_error(self):
        """Should retry on get equity chart error."""
        with patch('lib.riskManagement.clients.equityTracking.equityChartStreamManager.asyncio.sleep',
                   new=lambda x: sleep(x / 70)):
            get_equity_chart_stub.side_effect = [TimeoutError(), TimeoutError(), results]
            listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
            await sleep(0.5)
            listener_id = await listener_id
            equity_chart_stream_manager.remove_equity_chart_listener(listener_id)
            call_stub.assert_any_call(results)
            assert error_stub.call_count == 2

    @pytest.mark.asyncio
    async def test_initialize_two_listeners(self):
        """Should initialize two listeners."""
        listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
        await sleep(0.05)
        call_stub.assert_any_call(results)

        call_stub2 = MagicMock()
        finish_stub2 = MagicMock()

        class Listener2(EquityChartListener):
            async def on_equity_record_updated(self, equity_chart_event):
                call_stub2(equity_chart_event)

            async def on_equity_record_completed(self):
                finish_stub2()

        listener2 = Listener2()
        listener_id2 = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener2, 'accountId'))
        await sleep(0.05)
        call_stub2.assert_any_call(results)
        listener_id = await listener_id
        equity_chart_stream_manager.remove_equity_chart_listener(listener_id)
        await sleep(0.05)
        connection.close.assert_not_called()
        listener_id2 = await listener_id2
        equity_chart_stream_manager.remove_equity_chart_listener(listener_id2)
        await sleep(0.05)
        connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_until_sync_for_second_listener(self):
        """Should wait until synchronization for second listener."""
        connection.health_monitor.health_status = {'synchronized': False}
        listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
        await sleep(0.05)
        call_stub.assert_any_call(results)

        call_stub2 = MagicMock()
        finish_stub2 = MagicMock()
        connected_stub2 = MagicMock()
        disconnected_stub2 = MagicMock()

        class Listener2(EquityChartListener):
            async def on_equity_record_updated(self, equity_chart_event):
                call_stub2(equity_chart_event)

            async def on_equity_record_completed(self):
                finish_stub2()

            async def on_connected(self):
                connected_stub2()

            async def on_disconnected(self):
                disconnected_stub2()

        listener2 = Listener2()
        listener_promise = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener2, 'accountId'))
        await sleep(0.05)
        call_stub2.assert_not_called()
        await sync_listener.on_deals_synchronized('new-york:0:ps-mpa-1', 'syncId')
        listener_id2 = await listener_promise
        await sleep(0.05)
        call_stub2.assert_any_call(results)
        listener_id = await listener_id
        equity_chart_stream_manager.remove_equity_chart_listener(listener_id)
        await sleep(0.05)
        connection.close.assert_not_called()
        equity_chart_stream_manager.remove_equity_chart_listener(listener_id2)
        await sleep(0.05)
        connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_track_connection_stat(self):
        """Should track connection stat."""
        listener_id = create_task(equity_chart_stream_manager.add_equity_chart_listener(listener, 'accountId'))
        await sleep(0.05)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1', {'equity': 10600, 'balance': 9000})
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'syncId')
        await sleep(0.05)
        connected_stub.assert_called_once()
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'syncId')
        await sleep(0.05)
        connected_stub.assert_called_once()
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await sleep(0.05)
        disconnected_stub.assert_not_called()
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await sleep(0.05)
        disconnected_stub.assert_not_called()
        connection.health_monitor.health_status = {'synchronized': False}
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await sleep(0.05)
        disconnected_stub.assert_called_once()
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await sleep(0.05)
        assert disconnected_stub.call_count == 2
