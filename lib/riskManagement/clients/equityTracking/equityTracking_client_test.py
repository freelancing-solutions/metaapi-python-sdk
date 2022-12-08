from .equityTracking_client import EquityTrackingClient
from .trackerEventListener import TrackerEventListener
from .periodStatisticsListener import PeriodStatisticsListener
from .equityChartListener import EquityChartListener
import pytest
from mock import MagicMock, AsyncMock
from ...models import date
domain_client = MagicMock()
meta_api = MagicMock()
request_api_stub = MagicMock()
equity_tracking_client = EquityTrackingClient(domain_client, meta_api)
token = 'header.payload.sign'


class Listener(TrackerEventListener):
    async def on_tracker_event(self, tracker_event):
        pass


listener: TrackerEventListener = None


@pytest.fixture(autouse=True)
async def run_around_tests():
    global domain_client
    domain_client = MagicMock()
    domain_client.request_api = AsyncMock()
    domain_client.token = token
    global request_api_stub
    request_api_stub = AsyncMock()
    domain_client.request_api = request_api_stub
    global meta_api
    meta_api = MagicMock()
    global equity_tracking_client
    equity_tracking_client = EquityTrackingClient(domain_client, meta_api)
    global listener
    listener = Listener()


class TestEquityTrackingClient:
    @pytest.mark.asyncio
    async def test_create_tracker(self):
        """Should create a tracker."""
        expected = {'id': 'trackerId'}
        tracker = {'name': 'trackerName'}
        domain_client.request_api = AsyncMock(return_value=expected)
        actual = await equity_tracking_client.create_tracker('accountId', tracker)
        assert actual == expected
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/trackers',
            'method': 'POST',
            'body': tracker
        })

    @pytest.mark.asyncio
    async def test_retrieve_trackers(self):
        """Should retrieve trackers."""
        expected = [{'name': 'trackerName'}]
        domain_client.request_api = AsyncMock(return_value=expected)
        actual = await equity_tracking_client.get_trackers('accountId')
        assert actual == expected
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/trackers',
            'method': 'GET'
        })

    @pytest.mark.asyncio
    async def test_retrieve_tracker_by_id(self):
        """Should retrieve tracker by id."""
        expected = {'id': 'trackerId', 'name': 'trackerName'}
        domain_client.request_api = AsyncMock(return_value=expected)
        actual = await equity_tracking_client.get_tracker('accountId', 'trackerId')
        assert actual == expected
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/trackers/trackerId',
            'method': 'GET'
        })

    @pytest.mark.asyncio
    async def test_retrieve_tracker_by_name(self):
        """Should retrieve tracker by name."""
        expected = {'name': 'trackerName'}
        domain_client.request_api = AsyncMock(return_value=expected)
        actual = await equity_tracking_client.get_tracker_by_name('accountId', 'name')
        assert actual == expected
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/trackers/name/name',
            'method': 'GET'
        })

    @pytest.mark.asyncio
    async def test_update_tracker(self):
        """Should update tracker."""
        update = {'name': 'newTrackerName'}
        await equity_tracking_client.update_tracker('accountId', 'trackerId', update)
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/trackers/trackerId',
            'method': 'PUT',
            'body': update
        })

    @pytest.mark.asyncio
    async def test_delete_tracker(self):
        """Should delete tracker."""
        await equity_tracking_client.delete_tracker('accountId', 'trackerId')
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/trackers/trackerId',
            'method': 'DELETE'
        })

    @pytest.mark.asyncio
    async def test_retrieve_tracker_events(self):
        """Should retrieve tracker events."""
        expected = [{
            'sequenceNumber': 1,
            'accountId': 'accountId',
            'trackerId': 'trackerId',
            'period': 'day',
            'startBrokerTime': '2022-04-08 00:00:00.000',
            'endBrokerTime': '2022-04-08 23:59:59.999',
            'brokerTime': '2022-04-08 09:36:00.000',
            'absoluteDrawdown': 250,
            'relativeDrawdown': 0.25
        }]
        domain_client.request_api = AsyncMock(return_value=expected)

        actual = await equity_tracking_client.get_tracker_events(
            '2022-04-08 09:36:00.000', '2022-04-08 10:36:00.000', 'accountId', 'trackerId', 100)
        assert actual == expected
        domain_client.request_api.assert_called_with({
            'url': '/users/current/tracker-events/by-broker-time',
            'params': {
                'startBrokerTime': '2022-04-08 09:36:00.000',
                'endBrokerTime': '2022-04-08 10:36:00.000',
                'accountId': 'accountId',
                'trackerId': 'trackerId',
                'limit': 100
            },
            'method': 'GET'
        })

    @pytest.mark.asyncio
    async def test_retrieve_tracking_statistics(self):
        """Should retrieve tracking statistics."""
        expected = [{
            'period': 'day',
            'startBrokerTime': '2022-04-08 00:00:00.000',
            'endBrokerTime': '2022-04-08 23:59:59.999',
            'initialBalance': 1000,
            'maxDrawdownTime': '2022-04-08 09:36:00.000',
            'maxAbsoluteDrawdown': 250,
            'maxRelativeDrawdown': 0.25,
            'thresholdExceeded': True
        }]
        domain_client.request_api = AsyncMock(return_value=expected)

        actual = await equity_tracking_client.get_tracking_statistics('accountId', 'trackerId',
                                                                      '2022-04-08 09:36:00.000', 100)
        assert actual == expected
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/trackers/trackerId/statistics',
            'params': {'startTime': '2022-04-08 09:36:00.000', 'limit': 100, 'realTime': False},
            'method': 'GET'
        })

    @pytest.mark.asyncio
    async def test_retrieve_equity_chart(self):
        """Should retrieve equity chart."""
        expected = [{
            'startBrokerTime': '2022-04-08 00:00:00.000',
            'endBrokerTime': '2022-04-08 23:59:59.999',
            'averageBalance': 1050,
            'minBalance': 100,
            'maxBalance': 2000,
            'averageEquity': 1075,
            'minEquity': 50,
            'maxEquity': 2100
        }]
        domain_client.request_api = AsyncMock(return_value=expected)
        actual = await equity_tracking_client.get_equity_chart('accountId', '2022-04-08 09:36:00.000',
                                                               '2022-04-08 10:36:00.000')
        assert actual == expected
        domain_client.request_api.assert_called_with({
            'url': '/users/current/accounts/accountId/equity-chart',
            'params': {
                'realTime': False,
                'startTime': '2022-04-08 09:36:00.000',
                'endTime': '2022-04-08 10:36:00.000'
            },
            'method': 'GET'
        })

    @pytest.mark.asyncio
    async def test_retrieve_equity_chart_and_fill_the_missing_records(self):
        """Should retrieve equity chart and fill the missing records."""
        expected = [{
            'startBrokerTime': '2022-04-08 00:00:00.000',
            'endBrokerTime': '2022-04-08 00:59:59.999',
            'averageBalance': 1050,
            'minBalance': 100,
            'maxBalance': 2000,
            'averageEquity': 1075,
            'minEquity': 50,
            'maxEquity': 2100
        }, {
            'startBrokerTime': '2022-04-08 03:00:00.000',
            'endBrokerTime': '2022-04-08 03:59:59.999',
            'averageBalance': 1050,
            'minBalance': 100,
            'maxBalance': 2000,
            'averageEquity': 1075,
            'minEquity': 50,
            'maxEquity': 2100,
            'lastBalance': 500,
            'lastEquity': 600
        }, {
            'startBrokerTime': '2022-04-08 06:00:00.000',
            'endBrokerTime': '2022-04-08 06:59:59.999',
            'averageBalance': 1100,
            'minBalance': 200,
            'maxBalance': 1900,
            'averageEquity': 1100,
            'minEquity': 100,
            'maxEquity': 2000,
            'lastBalance': 500,
            'lastEquity': 500
        }]
        request_api_stub.return_value = expected

        actual = await equity_tracking_client.get_equity_chart('accountId', '2022-04-08 09:36:00.000',
                                                               '2022-04-08 10:36:00.000', False, True)
        assert actual == [{
          'averageBalance': 1050,
          'averageEquity': 1075,
          'endBrokerTime': '2022-04-08 00:59:59.999',
          'maxBalance': 2000,
          'maxEquity': 2100,
          'minBalance': 100,
          'minEquity': 50,
          'startBrokerTime': '2022-04-08 00:00:00.000'
        }, {
          'averageBalance': 1050,
          'averageEquity': 1075,
          'endBrokerTime': '2022-04-08 03:59:59.999',
          'lastBalance': 500,
          'lastEquity': 600,
          'maxBalance': 2000,
          'maxEquity': 2100,
          'minBalance': 100,
          'minEquity': 50,
          'startBrokerTime': '2022-04-08 03:00:00.000'
        }, {
          'averageBalance': 500,
          'averageEquity': 600,
          'brokerTime': '2022-04-08 04:59:59.999',
          'endBrokerTime': '2022-04-08 04:59:59.999',
          'lastBalance': 500,
          'lastEquity': 600,
          'maxBalance': 500,
          'maxEquity': 600,
          'minBalance': 500,
          'minEquity': 600,
          'startBrokerTime': '2022-04-08 04:00:00.000'
        }, {
          'averageBalance': 500,
          'averageEquity': 600,
          'brokerTime': '2022-04-08 05:59:59.999',
          'endBrokerTime': '2022-04-08 05:59:59.999',
          'lastBalance': 500,
          'lastEquity': 600,
          'maxBalance': 500,
          'maxEquity': 600,
          'minBalance': 500,
          'minEquity': 600,
          'startBrokerTime': '2022-04-08 05:00:00.000'
        }, {
          'averageBalance': 1100,
          'averageEquity': 1100,
          'endBrokerTime': '2022-04-08 06:59:59.999',
          'lastBalance': 500,
          'lastEquity': 500,
          'maxBalance': 1900,
          'maxEquity': 2000,
          'minBalance': 200,
          'minEquity': 100,
          'startBrokerTime': '2022-04-08 06:00:00.000'
        }]
        request_api_stub.assert_any_call({
            'url': '/users/current/accounts/accountId/equity-chart',
            'params': {
                'startTime': '2022-04-08 09:36:00.000',
                'endTime': '2022-04-08 10:36:00.000',
                'realTime': False
            },
            'method': 'GET'
        })
        request_api_stub.assert_called_once()

    @pytest.fixture()
    def tracker_event_listener_fixture(self):

        class Listener(TrackerEventListener):
            async def on_tracker_event(self, tracker_event):
                pass

        global listener
        listener = Listener()

    @pytest.mark.asyncio
    async def test_add_tracker_event_listener(self, tracker_event_listener_fixture):
        """Should add tracker event listener."""

        call_stub = MagicMock()
        equity_tracking_client._trackerEventListenerManager.add_tracker_event_listener = call_stub
        equity_tracking_client.add_tracker_event_listener(listener, 'accountId', 'trackerId', 1)
        call_stub.assert_called_with(listener, 'accountId', 'trackerId', 1)

    @pytest.mark.asyncio
    async def test_remove_tracker_event_listener(self, tracker_event_listener_fixture):
        """Should remove tracker event listener."""

        call_stub = MagicMock()
        equity_tracking_client._trackerEventListenerManager.remove_tracker_event_listener = call_stub
        equity_tracking_client.remove_tracker_event_listener('id')
        call_stub.assert_any_call('id')

    @pytest.fixture()
    def period_statistics_listener_fixture(self):
        class Listener(PeriodStatisticsListener):
            async def on_period_statistics_updated(self, period_statistics_event):
                pass

        global listener
        listener = Listener()

    @pytest.mark.asyncio
    async def test_add_period_statistics_event_listener(self, period_statistics_listener_fixture):
        """Should add period statistics event listener."""

        call_stub = AsyncMock()
        equity_tracking_client._periodStatisticsStreamManager.add_period_statistics_listener = call_stub
        await equity_tracking_client.add_period_statistics_listener(listener, 'accountId', 'trackerId')
        call_stub.assert_called_with(listener, 'accountId', 'trackerId')

    @pytest.mark.asyncio
    async def test_remove_period_statistics_event_listener(self, period_statistics_listener_fixture):
        """Should remove period statistics event listener."""

        call_stub = MagicMock()
        equity_tracking_client._periodStatisticsStreamManager.remove_period_statistics_listener = call_stub
        equity_tracking_client.remove_period_statistics_listener('id')
        call_stub.assert_any_call('id')

    @pytest.fixture()
    def equirt_chart_listener_fixture(self):
        class Listener(EquityChartListener):
            async def on_equity_record_updated(self, equity_chart_event):
                pass

        global listener
        listener = Listener()

    @pytest.mark.asyncio
    async def test_add_equity_chart_event_listener(self, equirt_chart_listener_fixture):
        """Should add equity chart event listener."""

        call_stub = AsyncMock()
        equity_tracking_client._equityChartStreamManager.add_equity_chart_listener = call_stub
        await equity_tracking_client.add_equity_chart_listener(listener, 'accountId', date('2022-04-08 09:36:00.000'))
        call_stub.assert_called_with(listener, 'accountId', date('2022-04-08 09:36:00.000'))

    @pytest.mark.asyncio
    async def test_remove_equity_chart_event_listener(self, equirt_chart_listener_fixture):
        """Should remove equity chart event listener."""

        call_stub = MagicMock()
        equity_tracking_client._equityChartStreamManager.remove_equity_chart_listener = call_stub
        equity_tracking_client.remove_equity_chart_listener('id')
        call_stub.assert_called_with('id')
