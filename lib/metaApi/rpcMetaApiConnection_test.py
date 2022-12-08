from .rpcMetaApiConnection import RpcMetaApiConnection
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from .models import MetatraderHistoryOrders, MetatraderDeals
from ..clients.metaApi.reconnectListener import ReconnectListener
from ..clients.metaApi.synchronizationListener import SynchronizationListener
from .metatraderAccount import MetatraderAccount
from ..clients.timeoutException import TimeoutException
from datetime import datetime
from .connectionRegistryModel import ConnectionRegistryModel
from mock import MagicMock, AsyncMock
from typing import Coroutine
import pytest
import asyncio

account_regions = {
    'vint-hill': 'accountId',
    'new-york': 'accountIdReplica'
}


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

    def subscribe(self, account_id: str, instance_index: str = None):
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
                                timeout_in_seconds: float, application: str = None):
        pass

    def add_account_region(self, account_id: str, region: str):
        pass

    def remove_account_region(self, account_id: str):
        pass


class MockAccount(MetatraderAccount):

    def __init__(self, data, metatrader_account_client,
                 meta_api_websocket_client, connection_registry):
        super(MockAccount, self).__init__(data, metatrader_account_client, meta_api_websocket_client,
                                          connection_registry, MagicMock(), MagicMock(), 'MetaApi')
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

    @property
    def account_regions(self) -> dict:
        return account_regions

    async def reload(self):
        pass


account: MockAccount = None
client: MockClient = None
api: RpcMetaApiConnection = None
connection_registry: ConnectionRegistryModel = None


@pytest.fixture(autouse=True)
async def run_around_tests():
    global account
    account = MockAccount(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    global client
    client = MockClient(MagicMock(), 'token')
    client.ensure_subscribe = AsyncMock()
    global connection_registry
    connection_registry = MagicMock()
    global api
    api = RpcMetaApiConnection(client, account, connection_registry)
    yield


class TestRpcMetaApiConnection:
    @pytest.mark.asyncio
    async def test_connect(self):
        """Should connect rpc connection."""
        client.add_account_cache = MagicMock()
        client.ensure_subscribe = AsyncMock()
        await api.connect('instanceId')
        client.add_account_cache.assert_called_with('accountId', account_regions)
        client.ensure_subscribe.assert_any_call('accountId', 0)
        client.ensure_subscribe.assert_any_call('accountId', 1)
        client.ensure_subscribe.assert_any_call('accountIdReplica', 0)
        client.ensure_subscribe.assert_any_call('accountIdReplica', 1)

    @pytest.mark.asyncio
    async def test_close(self):
        """Should close connection only if all instances closed."""
        client.remove_account_cache = MagicMock()
        client.remove_reconnect_listener = MagicMock()
        connection_registry.remove_rpc = AsyncMock()
        await api.connect('accountId')
        await api.connect('accountId')
        await api.connect('accountId2')
        await api.connect('accountId3')
        await api.close('accountId')
        client.remove_account_cache.assert_not_called()
        await api.close('accountId3')
        client.remove_account_cache.assert_not_called()
        await api.close('accountId2')
        client.remove_account_cache.assert_called_with('accountId')
        client.remove_reconnect_listener.assert_called_with(api)
        connection_registry.remove_rpc.assert_called_with(account)

    @pytest.mark.asyncio
    async def test_close_connection_only_after_it_has_been_opened(self):
        """Should close connection only after it has been opened."""
        client.remove_account_cache = MagicMock()
        client.remove_reconnect_listener = MagicMock()
        connection_registry.remove_rpc = AsyncMock()
        await api.close('accountId')
        client.remove_account_cache.assert_not_called()
        await api.connect('accountId')
        await api.close('accountId')
        client.remove_account_cache.assert_called_with('accountId')
        client.remove_reconnect_listener.assert_called_with(api)
        connection_registry.remove_rpc.assert_called_with(account)

    @pytest.mark.asyncio
    async def test_on_connected(self):
        """Should process on_connected event."""
        await api.on_connected('vint-hill:1:ps-mpa-1', 1)
        assert api.is_synchronized()

    @pytest.mark.asyncio
    async def test_on_disconnected(self):
        """Should process on_disconnected event."""
        await api.on_connected('vint-hill:1:ps-mpa-1', 1)
        await api.on_connected('vint-hill:1:ps-mpa-2', 1)
        assert api.is_synchronized()
        await api.on_disconnected('vint-hill:1:ps-mpa-1')
        assert api.is_synchronized()
        await api.on_disconnected('vint-hill:1:ps-mpa-2')
        assert not api.is_synchronized()

    @pytest.mark.asyncio
    async def test_on_stream_closed(self):
        """Should process on_stream_closed event."""
        await api.on_connected('vint-hill:1:ps-mpa-1', 1)
        await api.on_connected('vint-hill:1:ps-mpa-2', 1)
        assert api.is_synchronized()
        await api.on_stream_closed('vint-hill:1:ps-mpa-1')
        assert api.is_synchronized()
        await api.on_stream_closed('vint-hill:1:ps-mpa-2')
        assert not api.is_synchronized()

    @pytest.mark.asyncio
    async def test_wait_synchronized(self):
        """Should wait until RPC application is synchronized."""
        async def call_connected():
            await asyncio.sleep(0.05)
            await api.on_connected('vint-hill:1:mpa-1', 1)
        await api.connect('instanceId')
        client.wait_synchronized = AsyncMock(side_effect=[TimeoutException('timeout'), TimeoutException('timeout'),
                                                          MagicMock()])
        asyncio.create_task(call_connected())
        await api.wait_synchronized()

    @pytest.mark.asyncio
    async def test_timeout_synchronization(self):
        """Should time out waiting for synchronization."""
        await api.connect('instanceId')

        async def call_connected():
            await asyncio.sleep(0.05)
            await api.on_connected('vint-hill:1:mpa-1', 1)

        asyncio.create_task(call_connected())

        async def wait_synchronized(account_id: str, instance_index: str, application_pattern: str,
                                    timeout_in_seconds: float, application: str = None):
            await asyncio.sleep(0.1)
            raise TimeoutException('timeout')

        client.wait_synchronized = AsyncMock(side_effect=wait_synchronized)
        try:
            await api.wait_synchronized(0.09)
            raise Exception('TimeoutException is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
        client.wait_synchronized.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_synchronization_if_no_connected(self):
        """Should time out waiting for synchronization if no connected event has arrived."""
        await api.connect('instanceId')
        client.wait_synchronized = AsyncMock()
        try:
            await api.wait_synchronized(0.09)
            raise Exception('TimeoutException is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
        client.wait_synchronized.assert_not_called()

    @pytest.mark.asyncio
    async def test_clear_region_states_on_socket_reconnect(self):
        """Should clear region states on socket reconnect."""
        await api.connect('testId')
        await api.on_connected('new-york:1:ps-mpa-1', 2)
        await api.on_connected('vint-hill:1:ps-mpa-1', 2)
        assert api.is_synchronized()
        await api.on_reconnected('vint-hill', 1)
        assert api.is_synchronized()
        await api.on_reconnected('new-york', 1)
        assert not api.is_synchronized()
