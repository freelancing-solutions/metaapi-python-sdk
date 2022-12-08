import pytest
import asyncio
from ..domain_client import DomainClient
from mock import MagicMock, AsyncMock
from .trackerEventListener import TrackerEventListener
from .trackerEventListenerManager import TrackerEventListenerManager
from asyncio import sleep


token = 'header.payload.sign'
domain_client: DomainClient = None
tracker_event_listener_manager: TrackerEventListenerManager = None
get_event_stub = MagicMock()
call_stub = MagicMock()
error_stub = MagicMock()
listener: TrackerEventListener = MagicMock()
expected = [{
    'sequenceNumber': 2,
    'accountId': 'accountId',
    'trackerId': 'trackerId',
    'period': 'day',
    'startBrokerTime': '2022-04-08 00:00:00.000',
    'endBrokerTime': '2022-04-08 23:59:59.999',
    'brokerTime': '2022-04-08 09:36:00.000',
    'absoluteDrawdown': 250,
    'relativeDrawdown': 0.25
}, {
    'sequenceNumber': 3,
    'accountId': 'accountId',
    'trackerId': 'trackerId',
    'period': 'day',
    'startBrokerTime': '2022-04-08 00:00:00.000',
    'endBrokerTime': '2022-04-08 23:59:59.999',
    'brokerTime': '2022-04-08 09:36:00.000',
    'absoluteDrawdown': 250,
    'relativeDrawdown': 0.25
}]

expected2 = [{
    'sequenceNumber': 4,
    'accountId': 'accountId',
    'trackerId': 'trackerId',
    'period': 'day',
    'startBrokerTime': '2022-04-08 00:00:00.000',
    'endBrokerTime': '2022-04-08 23:59:59.999',
    'brokerTime': '2022-04-08 09:36:00.000',
    'absoluteDrawdown': 250,
    'relativeDrawdown': 0.25
}, {
    'sequenceNumber': 5,
    'accountId': 'accountId',
    'trackerId': 'trackerId',
    'period': 'day',
    'startBrokerTime': '2022-04-08 00:00:00.000',
    'endBrokerTime': '2022-04-08 23:59:59.999',
    'brokerTime': '2022-04-08 09:36:00.000',
    'absoluteDrawdown': 250,
    'relativeDrawdown': 0.25
}]


@pytest.fixture(autouse=True)
async def run_around_tests():
    global domain_client
    domain_client = DomainClient(MagicMock(), token, 'risk-management-api-v1')
    global tracker_event_listener_manager
    tracker_event_listener_manager = TrackerEventListenerManager(domain_client)
    global get_event_stub

    async def get_event_func(arg1, arg2):
        opt1 = {
            'url': '/users/current/tracker-events/stream',
            'method': 'GET',
            'qs': {
              'previousSequenceNumber': 1,
              'accountId': 'accountId',
              'trackerId': 'trackerId',
              'limit': 1000
            }
        }
        opt2 = {
            'url': '/users/current/tracker-events/stream',
            'method': 'GET',
            'qs': {
                'previousSequenceNumber': 3,
                'accountId': 'accountId',
                'trackerId': 'trackerId',
                'limit': 1000
            }
        }
        if arg1 == opt1:
            await asyncio.sleep(1)
            return expected
        elif arg1 == opt2:
            await asyncio.sleep(1)
            return expected2
        else:
            await asyncio.sleep(1)
            return []

    get_event_stub = AsyncMock(side_effect=get_event_func)
    domain_client.request_api = get_event_stub
    global call_stub
    call_stub = MagicMock()
    global error_stub
    error_stub = MagicMock()

    class Listener(TrackerEventListener):
        async def on_tracker_event(self, tracker_event):
            call_stub(tracker_event)

        async def on_error(self, error: Exception):
            error_stub(error)

    global listener
    listener = Listener()


class TestTrackerEventListenerManager:
    @pytest.mark.asyncio
    async def test_add_tracker_event_listener(self):
        """Should add tracker event listener."""
        id = tracker_event_listener_manager.add_tracker_event_listener(listener, 'accountId', 'trackerId', 1)
        await sleep(2.2)
        assert call_stub.call_count == 4
        assert call_stub.call_args_list[0].args[0] == expected[0]
        assert call_stub.call_args_list[1].args[0] == expected[1]
        assert call_stub.call_args_list[2].args[0] == expected2[0]
        assert call_stub.call_args_list[3].args[0] == expected2[1]
        tracker_event_listener_manager.remove_tracker_event_listener(id)

    @pytest.mark.asyncio
    async def test_remove_tracker_event_listener(self):
        """Should remove tracker event listener."""
        id = tracker_event_listener_manager.add_tracker_event_listener(listener, 'accountId', 'trackerId', 1)
        await sleep(0.8)
        tracker_event_listener_manager.remove_tracker_event_listener(id)
        await sleep(2.2)
        assert call_stub.call_count == 2
        assert call_stub.call_args_list[0].args[0] == expected[0]
        assert call_stub.call_args_list[1].args[0] == expected[1]

    @pytest.mark.asyncio
    async def test_wait_if_error_returned(self):
        """Should wait if error returned."""
        error = Exception('test')
        error2 = Exception('test2')
        call_count = 0

        async def get_event_func(arg1, arg2):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise error
            elif call_count == 2:
                raise error2
            opt1 = {
                'url': '/users/current/tracker-events/stream',
                'method': 'GET',
                'qs': {
                    'previousSequenceNumber': 1,
                    'accountId': 'accountId',
                    'trackerId': 'trackerId',
                    'limit': 1000
                }
            }
            if arg1 == opt1:
                await asyncio.sleep(0.5)
                return expected
            else:
                await asyncio.sleep(0.5)
                return []

        get_event_stub.side_effect = get_event_func
        id = tracker_event_listener_manager.add_tracker_event_listener(listener, 'accountId', 'trackerId', 1)
        await sleep(0.6)
        assert get_event_stub.call_count == 1
        call_stub.assert_not_called()
        assert error_stub.call_count == 1
        error_stub.assert_any_call(error)
        await sleep(0.6)
        assert get_event_stub.call_count == 2
        call_stub.assert_not_called()
        assert error_stub.call_count == 2
        error_stub.assert_any_call(error2)
        await sleep(2)
        assert get_event_stub.call_count == 3
        call_stub.assert_not_called()
        await sleep(0.8)
        call_stub.assert_any_call(expected[0])
        call_stub.assert_any_call(expected[1])
        tracker_event_listener_manager.remove_tracker_event_listener(id)
