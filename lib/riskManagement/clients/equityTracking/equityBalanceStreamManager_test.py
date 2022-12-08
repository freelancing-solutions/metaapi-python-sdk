from .equityBalanceListener import EquityBalanceListener
from .equityBalanceStreamManager import EquityBalanceStreamManager
from ..domain_client import DomainClient
from .... import MetaApi, SynchronizationListener
from ....metaApi.streamingMetaApiConnectionInstance import StreamingMetaApiConnectionInstance
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
equity_balance_stream_manager = EquityBalanceStreamManager(domain_client, meta_api)
call_stub = MagicMock()
connected_stub = MagicMock()
disconnected_stub = MagicMock()
error_stub = MagicMock()
listener = EquityBalanceListener()
account = MagicMock()
sync_listener: SynchronizationListener = None
results = {'equity': 10600, 'balance': 9000}
connection: StreamingMetaApiConnectionInstance = MagicMock()


@pytest.fixture(autouse=True)
async def run_around_tests():
    global domain_client
    domain_client = DomainClient(MagicMock(), token, 'risk-management-api-v1')
    global meta_api
    meta_api = MagicMock()
    global equity_balance_stream_manager
    equity_balance_stream_manager = EquityBalanceStreamManager(domain_client, meta_api)
    global call_stub
    call_stub = MagicMock()
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

    class Listener(EquityBalanceListener):
        async def on_equity_or_balance_updated(self, equity_balance_data):
            call_stub(equity_balance_data)

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
    results = {'equity': 10600, 'balance': 9000}


class TestEquityBalanceStreamManager:

    @pytest.mark.asyncio
    async def test_process_price_events(self):
        """Should process price events."""
        listener_id = create_task(equity_balance_stream_manager.add_equity_balance_listener(listener, 'accountId'))
        await sleep(0.1)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-10 13:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10200
        })
        call_stub.assert_not_called()
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1', {'equity': 10600, 'balance': 9000})
        call_stub.assert_any_call(results)
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
        call_stub.assert_any_call({'equity': 10500, 'balance': 9000})
        equity_balance_stream_manager.remove_equity_balance_listener(await listener_id)

    @pytest.mark.asyncio
    async def test_retry_on_sync_error(self):
        """Should retry on synchronization error."""
        with patch('lib.riskManagement.clients.equityTracking.equityBalanceStreamManager.asyncio.sleep',
                   new=lambda x: sleep(x / 20)):
            account.wait_deployed = AsyncMock(side_effect=[TimeoutError(), TimeoutError(), True])
            listener_id = create_task(equity_balance_stream_manager.add_equity_balance_listener(listener, 'accountId'))
            await sleep(0.25)
            await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1',
                                                               {'equity': 10600, 'balance': 9000})
            call_stub.assert_any_call(results)
            assert error_stub.call_count == 2
            equity_balance_stream_manager.remove_equity_balance_listener(await listener_id)

    @pytest.mark.asyncio
    async def test_initialize_two_listeners(self):
        """Should initialize two listeners."""
        listener_id = await equity_balance_stream_manager.add_equity_balance_listener(listener, 'accountId')
        await sleep(0.05)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1',
                                                           {'equity': 10600, 'balance': 9000})
        call_stub.assert_any_call(results)

        call_stub2 = MagicMock()

        class Listener2(EquityBalanceListener):
            async def on_equity_or_balance_updated(self, equity_balance_data):
                call_stub2(equity_balance_data)

        listener2 = Listener2()
        listener_id2 = await equity_balance_stream_manager.add_equity_balance_listener(listener2, 'accountId')
        await sleep(0.05)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1',
                                                           {'equity': 10500, 'balance': 9000})
        call_stub2.assert_any_call({'equity': 10500, 'balance': 9000})
        equity_balance_stream_manager.remove_equity_balance_listener(listener_id)
        await sleep(0.05)
        connection.close.assert_not_called()
        equity_balance_stream_manager.remove_equity_balance_listener(listener_id2)
        await sleep(0.05)
        assert connection.close.call_count == 1

    @pytest.mark.asyncio
    async def test_wait_until_sync_for_second_listener(self):
        """Should wait until synchronization for second listener."""
        connection.health_monitor.health_status = {'synchronized': False}
        listener_id = await equity_balance_stream_manager.add_equity_balance_listener(listener, 'accountId')
        await sleep(0.05)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1',
                                                           {'equity': 10500, 'balance': 9000})
        call_stub.assert_any_call({'equity': 10500, 'balance': 9000})
        call_stub2 = MagicMock()
        connection_stub2 = MagicMock()
        disconnected_stub2 = MagicMock()

        class Listener2(EquityBalanceListener):
            async def on_equity_or_balance_updated(self, equity_balance_data):
                call_stub2(equity_balance_data)

            async def on_connected(self):
                connection_stub2()

            async def on_disconnected(self):
                disconnected_stub2()

        listener2 = Listener2()
        listener_promise = \
            create_task(equity_balance_stream_manager.add_equity_balance_listener(listener2, 'accountId'))
        await sleep(0.05)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1', {'equity': 10400, 'balance': 9000})
        call_stub2.assert_any_call({'equity': 10400, 'balance': 9000})
        await sync_listener.on_deals_synchronized('new-york:0:ps-mpa-1', 'id')
        listener_id2 = await listener_promise
        equity_balance_stream_manager.remove_equity_balance_listener(listener_id)
        await sleep(0.05)
        connection.close.assert_not_called()
        equity_balance_stream_manager.remove_equity_balance_listener(listener_id2)
        await sleep(0.05)
        assert connection.close.call_count == 1

    @pytest.mark.asyncio
    async def test_track_connection_state(self):
        """Should track connection state."""
        create_task(equity_balance_stream_manager.add_equity_balance_listener(listener, 'accountId'))
        await sleep(0.1)
        await sync_listener.on_account_information_updated('vint-hill:1:ps-mpa-1', {'equity': 10600, 'balance': 9000})
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'id')
        await sleep(0.05)
        assert connected_stub.call_count == 1
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'id')
        await sleep(0.05)
        assert connected_stub.call_count == 1
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        disconnected_stub.assert_not_called()
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        disconnected_stub.assert_not_called()
        connection.health_monitor.health_status = {'synchronized': False}
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await sleep(0.05)
        assert disconnected_stub.call_count == 1
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'id')
        await sleep(0.05)
        assert connected_stub.call_count == 2
