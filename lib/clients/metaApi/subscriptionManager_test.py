from .subscriptionManager import SubscriptionManager
from .metaApiWebsocket_client import MetaApiWebsocketClient
from ..timeoutException import TimeoutException
from ..errorHandler import TooManyRequestsException
from mock import MagicMock, AsyncMock, patch
from datetime import datetime, timedelta
from ...metaApi.models import format_date
import pytest
import asyncio
from asyncio import sleep


class MockClient(MetaApiWebsocketClient):
    def subscribe(self, account_id: str, instance_index: int = None):
        pass


client: MockClient = None
manager: SubscriptionManager = None


@pytest.fixture(autouse=True)
async def run_around_tests():
    global client
    client = MockClient('token')
    client._socket = MagicMock()
    client._socket.connected = True
    global manager
    manager = SubscriptionManager(client)
    yield
    client._synchronizationThrottler.stop()


class TestSubscriptionManager:

    @pytest.mark.asyncio
    async def test_subscribe_to_terminal(self):
        """Should subscribe to terminal."""
        client.subscribe = AsyncMock()

        async def delay_connect():
            await sleep(0.1)
            await manager.cancel_subscribe('accountId:0')

        asyncio.create_task(delay_connect())
        await manager.subscribe('accountId')
        client.subscribe.assert_called_with('accountId', None)

    @pytest.mark.asyncio
    async def test_retry_subscribe(self):
        """Should retry subscribe if no response received."""
        with patch('lib.clients.metaApi.subscriptionManager.asyncio.sleep', new=lambda x: sleep(x / 10)):
            response = {'type': 'response', 'accountId': 'accountId', 'requestId': 'requestId'}
            client.subscribe = AsyncMock(side_effect=[TimeoutException('timeout'), response, response])

            async def delay_connect():
                await sleep(0.36)
                await manager.cancel_subscribe('accountId:0')

            asyncio.create_task(delay_connect())
            await manager.subscribe('accountId')
            client.subscribe.assert_called_with('accountId', None)
            assert client.subscribe.call_count == 2

    @pytest.mark.asyncio
    async def test_wait_on_too_many_requests_error(self):
        """Should wait for recommended time if too many requests error received."""
        with patch('lib.clients.metaApi.subscriptionManager.asyncio.sleep', new=lambda x: sleep(x / 10)):
            response = {'type': 'response', 'accountId': 'accountId', 'requestId': 'requestId'}
            client.subscribe = AsyncMock(side_effect=[TooManyRequestsException('timeout', {
                'periodInMinutes': 60, 'maxRequestsForPeriod': 10000,
                'recommendedRetryTime': format_date(datetime.now() + timedelta(seconds=5))}), response, response])

            asyncio.create_task(manager.subscribe('accountId'))
            await sleep(0.36)
            assert client.subscribe.call_count == 1
            await sleep(0.2)
            manager.cancel_subscribe('accountId:0')
            client.subscribe.assert_called_with('accountId', None)
            assert client.subscribe.call_count == 2

    @pytest.mark.asyncio
    async def test_cancel_on_reconnect(self):
        """Should cancel all subscriptions on reconnect."""
        with patch('lib.clients.metaApi.subscriptionManager.asyncio.sleep', new=lambda x: sleep(x / 10)):
            client.connect = AsyncMock()
            client.subscribe = AsyncMock()
            asyncio.create_task(manager.subscribe('accountId'))
            asyncio.create_task(manager.subscribe('accountId2'))
            await sleep(0.1)
            manager.on_reconnected()
            await sleep(0.5)
            assert client.subscribe.call_count == 2

    @pytest.mark.asyncio
    async def test_no_multiple_subscribes(self):
        """Should not send multiple subscribe requests at the same time."""
        with patch('lib.clients.metaApi.subscriptionManager.asyncio.sleep', new=lambda x: sleep(x / 10)):
            client.subscribe = AsyncMock()
            asyncio.create_task(manager.subscribe('accountId'))
            asyncio.create_task(manager.subscribe('accountId'))
            await sleep(0.1)
            manager.cancel_subscribe('accountId:0')
            await sleep(0.25)
            client.subscribe.assert_called_with('accountId', None)
            assert client.subscribe.call_count == 1

    @pytest.mark.asyncio
    async def test_resubscribe_on_timeout(self):
        client.subscribe = AsyncMock()
        client._socket.connected = True

        async def delay_connect():
            await sleep(0.1)
            await manager.cancel_subscribe('accountId:0')

        asyncio.create_task(delay_connect())
        manager.on_timeout('accountId')
        await sleep(0.05)
        client.subscribe.assert_called_with('accountId', None)

    @pytest.mark.asyncio
    async def test_not_subscribe_if_disconnected(self):
        """Should not retry subscribe to terminal if connection is closed."""
        client.subscribe = AsyncMock()
        client._socket.connected = False

        async def delay_connect():
            await sleep(0.1)
            await manager.cancel_subscribe('accountId:0')

        asyncio.create_task(delay_connect())
        manager.on_timeout('accountId')
        await sleep(0.05)
        client.subscribe.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_account(self):
        """Should cancel all subscriptions for an account."""
        with patch('lib.clients.metaApi.subscriptionManager.asyncio.sleep', new=lambda x: sleep(x / 10)):
            client.subscribe = AsyncMock()
            asyncio.create_task(manager.subscribe('accountId', 0))
            asyncio.create_task(manager.subscribe('accountId', 1))
            await sleep(0.1)
            manager.cancel_account('accountId')
            await sleep(0.5)
            assert client.subscribe.call_count == 2

    @pytest.mark.asyncio
    async def test_should_destroy_subscribe_process_on_cancel(self):
        subscribe = AsyncMock()

        async def delay_subscribe(account_id, instance_index):
            await subscribe()
            await asyncio.sleep(0.4)
            return

        client.subscribe = delay_subscribe
        asyncio.create_task(manager.subscribe('accountId'))
        await asyncio.sleep(0.05)
        manager.cancel_subscribe('accountId:0')
        await asyncio.sleep(0.05)
        asyncio.create_task(manager.subscribe('accountId'))
        await asyncio.sleep(0.05)
        assert subscribe.call_count == 2
