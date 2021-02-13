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

    async def clear(self):
        pass

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


mock_client: MockClient = None
mock_storage: MockStorage = None
registry: ConnectionRegistry = None


class MockAccount(MetatraderAccount):

    def __init__(self, id):
        super().__init__(MagicMock(), MagicMock(), MagicMock(), MagicMock())
        self._id = id

    @property
    def id(self):
        return self._id


def create_connection_mock():
    mock = MagicMock()
    mock.initialize = AsyncMock()
    mock.subscribe = AsyncMock()
    return mock


@pytest.fixture(autouse=True)
async def run_around_tests():
    global mock_client
    mock_client = MockClient('token')
    global mock_storage
    mock_storage = MockStorage()
    global registry
    registry = ConnectionRegistry(mock_client)
    yield


class TestConnectionRegistry:

    @pytest.mark.asyncio
    async def test_connect_and_add(self):
        """Should connect and add connection to registry."""
        with patch('lib.metaApi.connectionRegistry.MetaApiConnection') as mock_connection:
            connection_instance = create_connection_mock()
            mock_connection.return_value = connection_instance
            account = MockAccount('id')
            connection = await registry.connect(account, mock_storage)
            assert connection.history_storage == connection_instance.history_storage
            connection_instance.initialize.assert_called()
            connection_instance.subscribe.assert_called()
            assert 'id' in registry._connections
            assert registry._connections['id'] == connection

    @pytest.mark.asyncio
    async def test_connect_and_return_previous(self):
        """Should return the same connection on second connect if same account id."""
        with patch('lib.metaApi.connectionRegistry.MetaApiConnection') as mock_connection:
            connection_mock1 = create_connection_mock()
            connection_mock2 = create_connection_mock()
            connection_mock3 = create_connection_mock()
            mock_connection.side_effect = [connection_mock1, connection_mock2, connection_mock3]
            accounts = [MockAccount('id0'), MockAccount('id1')]
            connection0 = await registry.connect(accounts[0], mock_storage)
            connection02 = await registry.connect(accounts[0], mock_storage)
            connection1 = await registry.connect(accounts[1], mock_storage)
            connection_mock1.initialize.assert_called()
            connection_mock1.subscribe.assert_called()
            connection_mock2.initialize.assert_called()
            connection_mock2.subscribe.assert_called()
            assert registry._connections['id0'] == connection0
            assert registry._connections['id1'] == connection1
            assert connection0 == connection02
            assert connection0 != connection1

    @pytest.mark.asyncio
    async def test_remove(self):
        """Should remove the account from registry."""
        with patch('lib.metaApi.connectionRegistry.MetaApiConnection') as mock_connection:
            connection_instance = create_connection_mock()
            mock_connection.return_value = connection_instance
            accounts = [MockAccount('id0'), MockAccount('id1')]
            connection0 = await registry.connect(accounts[0], mock_storage)
            connection1 = await registry.connect(accounts[1], mock_storage)
            assert registry._connections['id0'] == connection0
            assert registry._connections['id1'] == connection1
            registry.remove(accounts[0].id)
            assert not accounts[0].id in registry._connections
