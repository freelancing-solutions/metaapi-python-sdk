from mock import AsyncMock, patch
import pytest
from asyncio import sleep
import asyncio
from freezegun import freeze_time
from .metaApiWebsocket_client import MetaApiWebsocketClient
from .synchronizationThrottler import SynchronizationThrottler


class MockClient(MetaApiWebsocketClient):
    async def _rpc_request(self, account_id: str, request: dict, timeout_in_seconds: float = None):
        await sleep(0.1)
        pass


start_time = '2020-10-05 10:00:00.000'
throttler: SynchronizationThrottler = None
client = None


@pytest.fixture(autouse=True)
async def run_around_tests():
    with patch('lib.clients.metaApi.synchronizationThrottler.asyncio.sleep', new=lambda x: sleep(x / 20)):
        global client
        client = MockClient('token')
        client._rpc_request = AsyncMock()
        global throttler
        throttler = SynchronizationThrottler(client, 2)
        throttler.start()
        yield
        throttler.stop()


class TestSynchronizationThrottler:
    @pytest.mark.asyncio
    async def test_sync_without_queue(self):
        """Should immediately send request if free slots exist."""
        with freeze_time() as frozen_datetime:
            frozen_datetime.move_to('2020-10-10 01:00:01.000')
            await throttler.schedule_synchronize('accountId', {'requestId': 'test'})
            assert throttler._synchronizationIds == {'test': 1602291601.0}
            throttler.remove_synchronization_id('test')
            client._rpc_request.assert_called_with('accountId', {'requestId': 'test'})
            assert throttler._synchronizationIds == {}

    @pytest.mark.asyncio
    async def test_not_remove_if_different_instance_index(self):
        """Should not remove sync if different instance index."""
        with freeze_time() as frozen_datetime:
            frozen_datetime.move_to('2020-10-10 01:00:01.000')
            await throttler.schedule_synchronize('accountId', {'requestId': 'test', 'instanceIndex': 0})
            await throttler.schedule_synchronize('accountId', {'requestId': 'test1', 'instanceIndex': 1})
            assert throttler._synchronizationIds == {'test': 1602291601.0, 'test1': 1602291601.0}
            throttler.remove_synchronization_id('test')
            assert throttler._synchronizationIds == {'test1': 1602291601.0}
            client._rpc_request.assert_any_call('accountId', {'requestId': 'test', 'instanceIndex': 0})
            client._rpc_request.assert_any_call('accountId', {'requestId': 'test1', 'instanceIndex': 1})

    @pytest.mark.asyncio
    async def test_sync_with_queue(self):
        """Should wait for other sync requests to finish if slots are full."""
        await throttler.schedule_synchronize('accountId1', {'requestId': 'test1'})
        await throttler.schedule_synchronize('accountId2', {'requestId': 'test2'})
        client._rpc_request.assert_any_call('accountId1', {'requestId': 'test1'})
        client._rpc_request.assert_any_call('accountId2', {'requestId': 'test2'})
        asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test3'}))
        await sleep(0.1)
        assert client._rpc_request.call_count == 2
        throttler.remove_synchronization_id('test1')
        await sleep(0.1)
        assert client._rpc_request.call_count == 3

    @pytest.mark.asyncio
    async def test_not_take_slots_if_same_account(self):
        """Should not take extra slots if sync ids belong to the same account."""
        asyncio.create_task(throttler.schedule_synchronize('accountId', {'requestId': 'test', 'instanceIndex': 0}))
        asyncio.create_task(throttler.schedule_synchronize('accountId', {'requestId': 'test1', 'instanceIndex': 1}))
        asyncio.create_task(throttler.schedule_synchronize('accountId2', {'requestId': 'test2'}))
        asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test3'}))
        await sleep(0.2)
        assert client._rpc_request.call_count == 3
        client._rpc_request.assert_any_call('accountId', {'requestId': 'test', 'instanceIndex': 0})
        client._rpc_request.assert_any_call('accountId', {'requestId': 'test1', 'instanceIndex': 1})
        client._rpc_request.assert_any_call('accountId2', {'requestId': 'test2'})

    @pytest.mark.asyncio
    async def test_clear_expired_slots(self):
        """Should clear expired synchronization slots if no packets for 10 seconds."""
        with freeze_time(start_time) as frozen_datetime:
            await throttler.schedule_synchronize('accountId1', {'requestId': 'test1'})
            await throttler.schedule_synchronize('accountId2', {'requestId': 'test2'})
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test3'}))
            await sleep(0.2)
            assert client._rpc_request.call_count == 2
            frozen_datetime.tick(20)
            await sleep(0.2)
            assert client._rpc_request.call_count == 3

    @pytest.mark.asyncio
    async def test_renew_sync(self):
        """Should renew sync on update."""
        with freeze_time(start_time) as frozen_datetime:
            await throttler.schedule_synchronize('accountId1', {'requestId': 'test1'})
            await throttler.schedule_synchronize('accountId2', {'requestId': 'test2'})
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test3'}))
            await sleep(0.2)
            assert client._rpc_request.call_count == 2
            frozen_datetime.tick(11)
            await sleep(0.2)
            assert client._rpc_request.call_count == 3
            frozen_datetime.tick(11)
            throttler.update_synchronization_id('test1')
            asyncio.create_task(throttler.schedule_synchronize('accountId4', {'requestId': 'test4'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId5', {'requestId': 'test5'}))
            await sleep(0.2)
            assert client._rpc_request.call_count == 4

    @pytest.mark.asyncio
    async def test_replace_previous_syncs(self):
        """Should replace previous syncs."""
        asyncio.create_task(throttler.schedule_synchronize('accountId1', {'requestId': 'test1'}))
        asyncio.create_task(throttler.schedule_synchronize('accountId1', {'requestId': 'test2'}))
        asyncio.create_task(throttler.schedule_synchronize('accountId1', {'requestId': 'test3'}))
        asyncio.create_task(throttler.schedule_synchronize('accountId2', {'requestId': 'test4'}))
        asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test5'}))
        asyncio.create_task(throttler.schedule_synchronize('accountId1', {'requestId': 'test6', 'instanceIndex': 0}))
        await sleep(0.2)
        assert client._rpc_request.call_count == 4

    @pytest.mark.asyncio
    async def test_clear_on_disconnect(self):
        """Should clear existing sync ids on disconnect."""
        await throttler.schedule_synchronize('accountId1', {'requestId': 'test1'})
        await throttler.schedule_synchronize('accountId2', {'requestId': 'test2'})
        asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test3'}))
        await sleep(0.2)
        assert client._rpc_request.call_count == 2
        throttler.on_disconnect()
        await sleep(0.2)
        assert client._rpc_request.call_count == 3

    @pytest.mark.asyncio
    async def test_remove_from_queue(self):
        """Should remove synchronizations from queue."""
        with freeze_time(start_time) as frozen_datetime:
            await throttler.schedule_synchronize('accountId1', {'requestId': 'test1'})
            await throttler.schedule_synchronize('accountId2', {'requestId': 'test2'})
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test3'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test4',
                                                                              'instanceIndex': 0}))
            asyncio.create_task(throttler.schedule_synchronize('accountId4', {'requestId': 'test5'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test6'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId4', {'requestId': 'test7'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test8'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId5', {'requestId': 'test9'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test10',
                                                                              'instanceIndex': 0}))
            await sleep(0.2)
            frozen_datetime.tick(21)
            await sleep(0.2)
            frozen_datetime.tick(21)
            await sleep(0.2)
            frozen_datetime.tick(21)
            await sleep(0.2)
            frozen_datetime.tick(21)
            await sleep(0.2)
            assert client._rpc_request.call_count == 6
            client._rpc_request.assert_any_call('accountId1', {'requestId': 'test1'})
            client._rpc_request.assert_any_call('accountId2', {'requestId': 'test2'})
            client._rpc_request.assert_any_call('accountId3', {'requestId': 'test8'})
            client._rpc_request.assert_any_call('accountId3', {'requestId': 'test10', 'instanceIndex': 0})
            client._rpc_request.assert_any_call('accountId4', {'requestId': 'test7'})
            client._rpc_request.assert_any_call('accountId5', {'requestId': 'test9'})

    @pytest.mark.asyncio
    async def test_remove_expired_from_queue(self):
        """Should remove expired synchronizations from queue."""
        with freeze_time(start_time) as frozen_datetime:
            await throttler.schedule_synchronize('accountId1', {'requestId': 'test1'})
            await throttler.schedule_synchronize('accountId2', {'requestId': 'test2'})
            asyncio.create_task(throttler.schedule_synchronize('accountId3', {'requestId': 'test3'}))
            asyncio.create_task(throttler.schedule_synchronize('accountId4', {'requestId': 'test4'}))
            await sleep(0.1)
            frozen_datetime.tick(160)
            asyncio.create_task(throttler.schedule_synchronize('accountId5', {'requestId': 'test5'}))
            frozen_datetime.tick(160)
            throttler.update_synchronization_id('test1')
            throttler.update_synchronization_id('test2')
            await sleep(0.1)
            frozen_datetime.tick(21)
            await sleep(0.1)
            frozen_datetime.tick(21)
            await sleep(0.1)
            assert client._rpc_request.call_count == 3
            client._rpc_request.assert_any_call('accountId1', {'requestId': 'test1'})
            client._rpc_request.assert_any_call('accountId2', {'requestId': 'test2'})
            client._rpc_request.assert_any_call('accountId5', {'requestId': 'test5'})
