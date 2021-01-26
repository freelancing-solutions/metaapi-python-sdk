from .connectionRegistry import ConnectionRegistry
from .memoryHistoryStorageModel import MemoryHistoryStorageModel
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from ..clients.metaApi.reconnectListener import ReconnectListener
from ..metaApi.models import MetatraderOrder
from ..metaApi.metatraderAccount import MetatraderAccount
from .metaApiConnection import MetaApiConnection
from .models import MetatraderDeal
from mock import MagicMock, AsyncMock, patch
from datetime import datetime
import pytest


class MockClient(MetaApiWebsocketClient):
    async def subscribe(self, account_id: str, instance_index: int = None):
        pass

    def add_synchronization_listener(self, account_id: str, listener):
        pass

    def add_reconnect_listener(self, listener: ReconnectListener):
        pass


class MockStorage(MemoryHistoryStorageModel):

    def __init__(self):
        super().__init__()
        self._deals = []
        self._historyOrders = []

    @property
    def deals(self):
        return self._deals

    @property
    def history_orders(self):
        return self._historyOrders

    @property
    def last_deal_time_by_instance_index(self):
        return {}

    @property
    def last_history_order_time_by_instance_index(self):
        return {}

    def last_deal_time(self, instance_index: int = None) -> datetime:
        pass

    def last_history_order_time(self, instance_index: int = None) -> datetime:
        pass

    def on_deal_added(self, instance_index: int, deal: MetatraderDeal):
        pass

    async def load_data_from_disk(self):
        return {'deals': [], 'history_orders': []}

    def on_history_order_added(self, instance_index: int, history_order: MetatraderOrder):
        pass


mock_client = MockClient('token')
mock_storage = MockStorage()
registry = ConnectionRegistry(mock_client)


class MockAccount(MetatraderAccount):

    def __init__(self, id):
        super().__init__(MagicMock(), MagicMock(), MagicMock(), MagicMock())
        self._id = id

    @property
    def id(self):
        return self._id


@pytest.fixture(scope="module", autouse=True)
def run_around_tests():
    global mock_client
    mock_client = MockClient('token')
    global registry
    registry = ConnectionRegistry(mock_client)
    yield


class TestConnectionRegistry:

    @pytest.mark.asyncio
    async def test_connect_and_add(self):
        """Should connect and add connection to registry."""
        with patch('lib.metaApi.metaApiConnection.MetaApiConnection.initialize', new_callable=AsyncMock):
            mock_client.add_synchronization_listener = MagicMock()
            mock_client.subscribe = AsyncMock()
            account = MockAccount('id')
            connection = await registry.connect(account, mock_storage)
            assert isinstance(connection, MetaApiConnection)
            assert connection.history_storage == mock_storage
            mock_client.add_synchronization_listener.assert_any_call('id', mock_storage)
            mock_client.subscribe.assert_called_with('id')
            connection.initialize.assert_called()
            assert 'id' in registry._connections
            assert registry._connections['id'] == connection

    @pytest.mark.asyncio
    async def test_connect_and_return_previous(self):
        """Should return the same connection on second connect if same account id."""
        mock_client.add_synchronization_listener = MagicMock()
        mock_client.subscribe = AsyncMock()
        accounts = [MockAccount('id0'), MockAccount('id1')]
        connection0 = await registry.connect(accounts[0], mock_storage)
        connection02 = await registry.connect(accounts[0], mock_storage)
        connection1 = await registry.connect(accounts[1], mock_storage)
        mock_client.add_synchronization_listener.assert_any_call('id0', mock_storage)
        mock_client.add_synchronization_listener.assert_any_call('id1', mock_storage)
        mock_client.subscribe.assert_any_call('id0')
        mock_client.subscribe.assert_any_call('id1')
        assert mock_client.subscribe.call_count == 2
        assert registry._connections['id0'] == connection0
        assert registry._connections['id1'] == connection1
        assert connection0 == connection02

    @pytest.mark.asyncio
    async def test_remove(self):
        """Should remove the account from registry."""
        accounts = [MockAccount('id0'), MockAccount('id1')]
        connection0 = await registry.connect(accounts[0], mock_storage)
        connection1 = await registry.connect(accounts[1], mock_storage)
        assert registry._connections['id0'] == connection0
        assert registry._connections['id1'] == connection1
        registry.remove(accounts[0].id)
        assert not accounts[0].id in registry._connections
