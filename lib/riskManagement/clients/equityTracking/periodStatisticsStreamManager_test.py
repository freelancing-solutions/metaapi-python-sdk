import asyncio
from .periodStatisticsListener import PeriodStatisticsListener
from .periodStatisticsStreamManager import PeriodStatisticsStreamManager
from ..domain_client import DomainClient
from .... import MetaApi, SynchronizationListener
from ....metaApi.streamingMetaApiConnectionInstance import StreamingMetaApiConnectionInstance
from .equityTracking_client import EquityTrackingClient
from mock import MagicMock, AsyncMock, patch
from asyncio import sleep, create_task
from ...models import date
from ....clients.errorHandler import NotFoundException
from ....metaApi.memoryHistoryStorage import MemoryHistoryStorage
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
period_statistics_stream_manager = PeriodStatisticsStreamManager(domain_client, equity_tracking_client, meta_api)
get_account_stub = MagicMock()
updated_stub = MagicMock()
connected_stub = MagicMock()
completed_stub = MagicMock()
tracker_completed_stub = MagicMock()
disconnected_stub = MagicMock()
error_stub = MagicMock()
get_period_statistics_stub = AsyncMock()
get_tracker_stub = AsyncMock()
listener = PeriodStatisticsListener()
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
    global period_statistics_stream_manager
    period_statistics_stream_manager = PeriodStatisticsStreamManager(domain_client, equity_tracking_client, meta_api)
    global updated_stub
    updated_stub = MagicMock()
    global completed_stub
    completed_stub = MagicMock()
    global tracker_completed_stub
    tracker_completed_stub = MagicMock()
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
    connection.history_storage = MemoryHistoryStorage()

    class Listener(PeriodStatisticsListener):
        async def on_period_statistics_updated(self, period_statistics_event):
            print('UPDATE STUB', period_statistics_event)
            updated_stub(period_statistics_event)

        async def on_period_statistics_completed(self):
            completed_stub()

        async def on_tracker_completed(self):
            tracker_completed_stub()

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
    global get_account_stub
    get_account_stub = AsyncMock(return_value=account)
    meta_api.metatrader_account_api.get_account = get_account_stub
    global results
    results = [
        {
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 200,
            'maxAbsoluteProfit': 500,
            'maxDrawdownTime': '2020-05-11 14:00:00.000',
            'maxProfitTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.05,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        },
        {
            'endBrokerTime': '2020-05-11 11:59:59.999',
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 200,
            'maxAbsoluteProfit': 500,
            'maxDrawdownTime': '2020-05-10 14:00:00.000',
            'maxProfitTime': '2020-05-10 14:00:00.000',
            'maxRelativeDrawdown': 0.05,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-10 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }
    ]
    global get_period_statistics_stub
    get_period_statistics_stub = AsyncMock(return_value=results)
    equity_tracking_client.get_tracking_statistics = get_period_statistics_stub
    global get_tracker_stub
    get_tracker_stub = AsyncMock(return_value={
        'name': 'trackerName2',
        '_id': 'tracker2',
        'startBrokerTime': '2020-05-11 12:00:00.000',
        'endBrokerTime': '2020-05-12 11:57:59.999'
    })
    equity_tracking_client.get_tracker = get_tracker_stub


class TestPeriodStatisticsStreamManager:

    @pytest.mark.asyncio
    async def test_add_listener_and_request_events(self):
        """Should add listener and request events."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await sleep(0.05)
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)
        updated_stub.assert_called_with(results)

    @pytest.mark.asyncio
    async def test_process_price_events(self):
        """Should process price events."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_called_once()
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await sleep(0.05)
        updated_stub.assert_called_once()
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9000
        })
        await sleep(0.05)
        updated_stub.assert_any_call([{
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'period': 'day',
            'initialBalance': 10000,
            'maxDrawdownTime': '2020-05-12 11:55:00.000',
            'maxAbsoluteDrawdown': 1000,
            'maxRelativeDrawdown': 0.1,
            'maxProfitTime': '2020-05-11 14:00:00.000',
            'maxAbsoluteProfit': 500,
            'maxRelativeProfit': 0.1,
            'thresholdExceeded': False,
            'exceededThresholdType': None,
            'tradeDayCount': 0
        }])
        assert updated_stub.call_count == 2
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_process_price_events_within_tracker_limits(self):
        """Should process price events within tracker limits."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker2'))
        await sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_called_once()
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-09 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9000
        })
        await sleep(0.05)
        updated_stub.assert_called_once()
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9000
        })
        await sleep(0.05)
        assert updated_stub.call_count == 2
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:58:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9000
        })
        await sleep(0.05)
        assert updated_stub.call_count == 2
        tracker_completed_stub.assert_called_once()
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_process_balance_deals(self):
        """Should process balance deals."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker2'))
        await sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_called_once()
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9000
        })
        await sleep(0.05)
        assert updated_stub.call_count == 2
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9000
        })
        await sleep(0.05)
        assert updated_stub.call_count == 2
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'exceededThresholdType': None,
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 1000,
            'maxAbsoluteProfit': 500,
            'maxDrawdownTime': '2020-05-12 11:55:00.000',
            'maxProfitTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.1,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        await sync_listener.on_deal_added('vint-hill:1:ps-mpa-1', {
            'clientId': 'TE_GBPUSD_7hyINWqAlE',
            'commission': -0.25,
            'entryType': 'DEAL_ENTRY_IN',
            'id': '33230099',
            'magic': 1000,
            'platform': 'mt5',
            'orderId': '46214692',
            'positionId': '46214692',
            'price': 1.26101,
            'profit': 500,
            'swap': 0,
            'symbol': 'GBPUSD',
            'time': date('2020-04-15T02:45:06.521Z'),
            'type': 'DEAL_TYPE_BALANCE',
            'volume': 0.07
        })
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:05.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9000
        })
        await sleep(0.05)
        assert updated_stub.call_count == 3
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'exceededThresholdType': None,
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 1500,
            'maxAbsoluteProfit': 500,
            'maxDrawdownTime': '2020-05-12 11:55:05.000',
            'maxProfitTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.15,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        get_period_statistics_stub.return_value = [{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False
        }, results[0]]
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await sleep(0.05)
        assert updated_stub.call_count == 4
        completed_stub.assert_called_once()
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:02:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 11000
        })
        await sleep(0.05)
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'exceededThresholdType': None,
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 1500,
            'maxAbsoluteProfit': 1000,
            'maxDrawdownTime': '2020-05-12 11:55:05.000',
            'maxProfitTime': '2020-05-12 12:02:00.000',
            'maxRelativeDrawdown': 0.15,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_process_price_events_if_period_completed(self):
        """Should process price events if period completed."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await sleep(0.05)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await sleep(0.05)
        get_period_statistics_stub.return_value = [{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }, results[0]]
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await sleep(0.05)
        updated_stub.assert_any_call(results)
        completed_stub.assert_called_once()
        updated_stub.assert_any_call([results[0], {
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_retry_on_sync_error(self):
        """Should retry on synchronization error."""
        with patch('lib.riskManagement.clients.equityTracking.periodStatisticsStreamManager.asyncio.sleep',
                   new=lambda x: sleep(x / 50)):
            account.wait_deployed = AsyncMock(side_effect=[TimeoutError(), TimeoutError(), True])
            listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
                listener, 'accountId', 'tracker1'))
            await sleep(0.1)
            listener_id = await listener_id
            period_statistics_stream_manager.remove_period_statistics_listener(listener_id)
            updated_stub.assert_any_call(results)
            assert error_stub.call_count == 2

    @pytest.mark.asyncio
    async def test_retry_on_get_period_statistics_error(self):
        """Should retry on get period statistics error."""
        with patch('lib.riskManagement.clients.equityTracking.periodStatisticsStreamManager.asyncio.sleep',
                   new=lambda x: sleep(x / 70)):
            get_period_statistics_stub.side_effect = [TimeoutError(), TimeoutError(), results]
            listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
                listener, 'accountId', 'tracker1'))
            await asyncio.sleep(0.5)
            listener_id = await listener_id
            period_statistics_stream_manager.remove_period_statistics_listener(listener_id)
            updated_stub.assert_any_call(results)
            assert error_stub.call_count == 2

    @pytest.mark.asyncio
    async def test_return_error_if_account_not_found(self):
        """Should return error if account not found."""
        get_account_stub.side_effect = NotFoundException('test')
        try:
            await period_statistics_stream_manager.add_period_statistics_listener(listener, 'accountId', 'tracker1')
            pytest.fail()
        except Exception as err:
            assert err.__class__.__name__ == 'NotFoundException'

    @pytest.mark.asyncio
    async def test_return_error_if_failed_to_return_trackers(self):
        """Should return error if failed to return trackers."""
        get_tracker_stub.side_effect = TimeoutError()
        try:
            await period_statistics_stream_manager.add_period_statistics_listener(listener, 'accountId', 'tracker1')
            pytest.fail()
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutError'

    @pytest.mark.asyncio
    async def test_return_error_if_tracker_not_found(self):
        """Should return error if tracker not found."""
        get_tracker_stub.side_effect = NotFoundException('test')
        try:
            await period_statistics_stream_manager.add_period_statistics_listener(listener, 'accountId', 'tracker1')
            pytest.fail()
        except Exception as err:
            assert err.__class__.__name__ == 'NotFoundException'

    @pytest.mark.asyncio
    async def test_record_if_absolute_drawdown_threshold_exceeded(self):
        """Should record if absolute drawdown threshold exceeded."""
        get_tracker_stub.return_value = {'name': 'trackerName1', '_id': 'tracker1', 'absoluteDrawdownThreshold': 500}
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await asyncio.sleep(0.05)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9400
        })
        await asyncio.sleep(0.05)
        get_period_statistics_stub.return_value = [{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }, results[0]]
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await asyncio.sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 600,
            'maxAbsoluteProfit': 500,
            'maxDrawdownTime': '2020-05-12 11:55:00.000',
            'maxProfitTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.06,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'exceededThresholdType': 'drawdown',
            'thresholdExceeded': True,
            'tradeDayCount': 0
        }])
        updated_stub.assert_any_call([results[0], {
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_record_if_relative_drawdown_threshold_exceeded(self):
        """Should record if relative drawdown threshold exceeded."""
        get_tracker_stub.return_value = {'name': 'trackerName1', '_id': 'tracker1', 'relativeDrawdownThreshold': 0.05}
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await asyncio.sleep(0.05)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9400
        })
        await asyncio.sleep(0.05)
        get_period_statistics_stub.return_value = [{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }, results[0]]
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await asyncio.sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 600,
            'maxAbsoluteProfit': 500,
            'maxDrawdownTime': '2020-05-12 11:55:00.000',
            'maxProfitTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.06,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'exceededThresholdType': 'drawdown',
            'thresholdExceeded': True,
            'tradeDayCount': 0
        }])
        updated_stub.assert_any_call([results[0], {
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_record_if_absolute_profit_threshold_exceeded(self):
        """Should record if absolute profit threshold exceeded."""
        get_tracker_stub.return_value = {'name': 'trackerName1', '_id': 'tracker1', 'absoluteProfitThreshold': 500}
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await asyncio.sleep(0.05)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10600
        })
        await asyncio.sleep(0.05)
        get_period_statistics_stub.return_value = [{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }, results[0]]
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await asyncio.sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 200,
            'maxAbsoluteProfit': 600,
            'maxProfitTime': '2020-05-12 11:55:00.000',
            'maxDrawdownTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.05,
            'maxRelativeProfit': 0.06,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'exceededThresholdType': 'profit',
            'thresholdExceeded': True,
            'tradeDayCount': 0
        }])
        updated_stub.assert_any_call([results[0], {
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_record_if_relative_profit_threshold_exceeded(self):
        """Should record if relative profit threshold exceeded."""
        get_tracker_stub.return_value = {'name': 'trackerName1', '_id': 'tracker1', 'relativeProfitThreshold':  0.05}
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await asyncio.sleep(0.05)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10600
        })
        await asyncio.sleep(0.05)
        get_period_statistics_stub.return_value = [{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }, results[0]]
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await asyncio.sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 200,
            'maxAbsoluteProfit': 600,
            'maxProfitTime': '2020-05-12 11:55:00.000',
            'maxDrawdownTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.05,
            'maxRelativeProfit': 0.06,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'exceededThresholdType': 'profit',
            'thresholdExceeded': True,
            'tradeDayCount': 0
        }])
        updated_stub.assert_any_call([results[0], {
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_not_rewrite_record_exceeded_event(self):
        """Should not rewrite record exceeded event."""
        get_tracker_stub.return_value = {'name': 'trackerName1', '_id': 'tracker1', 'absoluteDrawdownThreshold': 500,
                                         'absoluteProfitThreshold': 500}
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        await asyncio.sleep(0.05)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 9400
        })
        await asyncio.sleep(0.05)
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 11:55:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10600
        })
        await asyncio.sleep(0.05)
        get_period_statistics_stub.return_value = [{
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }, results[0]]
        await sync_listener.on_symbol_price_updated('vint-hill:1:ps-mpa-1', {
            'symbol': 'EURUSD',
            'bid': 1.02273,
            'ask': 1.02274,
            'brokerTime': '2020-05-12 12:01:00.000',
            'profitTickValue': 1,
            'lossTickValue': 1,
            'accountCurrencyExchangeRate': 1,
            'equity': 10500
        })
        await asyncio.sleep(0.05)
        updated_stub.assert_any_call(results)
        updated_stub.assert_any_call([{
            'endBrokerTime': '2020-05-12 11:59:59.999',
            'initialBalance': 10000,
            'maxAbsoluteDrawdown': 600,
            'maxAbsoluteProfit': 500,
            'maxDrawdownTime': '2020-05-12 11:55:00.000',
            'maxProfitTime': '2020-05-11 14:00:00.000',
            'maxRelativeDrawdown': 0.06,
            'maxRelativeProfit': 0.1,
            'period': 'day',
            'startBrokerTime': '2020-05-11 12:00:00.000',
            'exceededThresholdType': 'drawdown',
            'thresholdExceeded': True,
            'tradeDayCount': 0
        }])
        updated_stub.assert_any_call([results[0], {
            'endBrokerTime': '2020-05-13 11:59:59.999',
            'initialBalance': 10000,
            'period': 'day',
            'startBrokerTime': '2020-05-12 12:00:00.000',
            'thresholdExceeded': False,
            'tradeDayCount': 0
        }])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_track_connection_state(self):
        """Should track connection state."""
        create_task(period_statistics_stream_manager.add_period_statistics_listener(listener, 'accountId', 'tracker1'))
        await asyncio.sleep(0.05)
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'syncId')
        await asyncio.sleep(0.05)
        assert connected_stub.call_count == 1
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'syncId')
        await asyncio.sleep(0.05)
        assert connected_stub.call_count == 1
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await asyncio.sleep(0.05)
        disconnected_stub.assert_not_called()
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await asyncio.sleep(0.05)
        disconnected_stub.assert_not_called()
        connection.health_monitor.health_status = {'synchronized': False}
        await sync_listener.on_disconnected('vint-hill:1:ps-mpa-1')
        await asyncio.sleep(0.05)
        assert disconnected_stub.call_count == 1
        await sync_listener.on_deals_synchronized('vint-hill:1:ps-mpa-1', 'syncId')
        await asyncio.sleep(0.05)
        assert connected_stub.call_count == 2

    @pytest.mark.asyncio
    async def test_send_an_update_event_if_a_new_deal_arrived(self):
        """Should send an update event if a new deal arrived."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        deal_balance = {
            'id': '200745237',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BALANCE',
            'time': date('2022-05-11T13:00:00.000Z'),
            'brokerTime': '2020-05-11 16:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        deal = {
            'id': '200745237',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BUY',
            'time': date('2022-05-11T13:00:00.000Z'),
            'brokerTime': '2020-05-11 16:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        deal2 = {
            'id': '200745238',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BUY',
            'time': date('2022-05-11T14:00:00.000Z'),
            'brokerTime': '2020-05-11 17:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        deal3 = {
            'id': '200745239',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BUY',
            'time': date('2022-05-12T02:00:00.000Z'),
            'brokerTime': '2020-05-12 05:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        await asyncio.sleep(0.05)
        await asyncio.gather(sync_listener.on_deal_added('vint-hill:1:ps-mpa-1', deal_balance),
                             connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal_balance))
        await asyncio.sleep(0.05)
        assert updated_stub.call_count == 1
        await asyncio.gather(sync_listener.on_deal_added('vint-hill:1:ps-mpa-1', deal),
                             connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal))
        assert updated_stub.call_count == 2
        updated_stub.assert_any_call([
            {
                'startBrokerTime': '2020-05-11 12:00:00.000',
                'endBrokerTime': '2020-05-12 11:59:59.999',
                'initialBalance': 10000,
                'maxAbsoluteDrawdown': 200,
                'maxAbsoluteProfit': 500,
                'maxDrawdownTime': '2020-05-11 14:00:00.000',
                'maxProfitTime': '2020-05-11 14:00:00.000',
                'maxRelativeDrawdown': 0.05,
                'maxRelativeProfit': 0.1,
                'period': 'day',
                'exceededThresholdType': None,
                'thresholdExceeded': False,
                'tradeDayCount': 1
            }
        ])
        await asyncio.sleep(0.05)
        await asyncio.gather(sync_listener.on_deal_added('vint-hill:1:ps-mpa-1', deal2),
                             connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal2))
        assert updated_stub.call_count == 2
        await asyncio.gather(sync_listener.on_deal_added('vint-hill:1:ps-mpa-1', deal3),
                             connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal3))
        assert updated_stub.call_count == 3
        updated_stub.assert_any_call([
            {
                'startBrokerTime': '2020-05-11 12:00:00.000',
                'endBrokerTime': '2020-05-12 11:59:59.999',
                'initialBalance': 10000,
                'maxAbsoluteDrawdown': 200,
                'maxAbsoluteProfit': 500,
                'maxDrawdownTime': '2020-05-11 14:00:00.000',
                'maxProfitTime': '2020-05-11 14:00:00.000',
                'maxRelativeDrawdown': 0.05,
                'maxRelativeProfit': 0.1,
                'period': 'day',
                'exceededThresholdType': None,
                'thresholdExceeded': False,
                'tradeDayCount': 2
            }
        ])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_account_for_deals_already_in_the_database(self):
        """Should account for deals already in the database."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        deal = {
            'id': '200745237',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BUY',
            'time': date('2022-05-11T13:00:00.000Z'),
            'brokerTime': '2020-05-11 16:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        deal2 = {
            'id': '200745239',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BUY',
            'time': date('2022-05-12T02:00:00.000Z'),
            'brokerTime': '2020-05-12 05:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        await asyncio.sleep(0.05)
        create_task(connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal))
        assert updated_stub.call_count == 1
        await asyncio.gather(sync_listener.on_deal_added('vint-hill:1:ps-mpa-1', deal2),
                             connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal2))
        assert updated_stub.call_count == 2
        updated_stub.assert_any_call([
            {
                'startBrokerTime': '2020-05-11 12:00:00.000',
                'endBrokerTime': '2020-05-12 11:59:59.999',
                'initialBalance': 10000,
                'maxAbsoluteDrawdown': 200,
                'maxAbsoluteProfit': 500,
                'maxDrawdownTime': '2020-05-11 14:00:00.000',
                'maxProfitTime': '2020-05-11 14:00:00.000',
                'maxRelativeDrawdown': 0.05,
                'maxRelativeProfit': 0.1,
                'period': 'day',
                'exceededThresholdType': None,
                'thresholdExceeded': False,
                'tradeDayCount': 2
            }
        ])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)

    @pytest.mark.asyncio
    async def test_filter_deals_according_to_timezone_offset(self):
        """Should filter deals according to timezone offset."""
        listener_id = create_task(period_statistics_stream_manager.add_period_statistics_listener(
            listener, 'accountId', 'tracker1'))
        deal = {
            'id': '200745237',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BUY',
            'time': date('2022-05-11T10:00:00.000Z'),
            'brokerTime': '2020-05-11 11:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        deal2 = {
            'id': '200745239',
            'platform': 'mt5',
            'type': 'DEAL_TYPE_BUY',
            'time': date('2022-05-12T02:00:00.000Z'),
            'brokerTime': '2020-05-12 03:00:00.000',
            'commission': -3.5,
            'swap': 0,
            'profit': 0,
            'symbol': 'EURUSD',
            'magic': 0,
            'orderId': '281184743',
            'positionId': '281184743',
            'volume': 1,
            'price': 0.97062,
            'entryType': 'DEAL_ENTRY_IN',
            'reason': 'DEAL_REASON_EXPERT',
            'accountCurrencyExchangeRate': 1,
            'updateSequenceNumber': 1665435250622040
        }
        await asyncio.sleep(0.05)
        create_task(connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal))
        await asyncio.gather(sync_listener.on_deal_added('vint-hill:1:ps-mpa-1', deal2),
                             connection.history_storage.on_deal_added('vint-hill:1:ps-mpa-1', deal2))
        assert updated_stub.call_count == 2
        updated_stub.assert_any_call([
            {
                'startBrokerTime': '2020-05-11 12:00:00.000',
                'endBrokerTime': '2020-05-12 11:59:59.999',
                'initialBalance': 10000,
                'maxAbsoluteDrawdown': 200,
                'maxAbsoluteProfit': 500,
                'maxDrawdownTime': '2020-05-11 14:00:00.000',
                'maxProfitTime': '2020-05-11 14:00:00.000',
                'maxRelativeDrawdown': 0.05,
                'maxRelativeProfit': 0.1,
                'period': 'day',
                'exceededThresholdType': None,
                'thresholdExceeded': False,
                'tradeDayCount': 1
            }
        ])
        listener_id = await listener_id
        period_statistics_stream_manager.remove_period_statistics_listener(listener_id)
