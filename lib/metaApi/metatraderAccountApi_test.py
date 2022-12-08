from .metatraderAccountApi import MetatraderAccountApi
from .metatraderAccount import MetatraderAccount
from ..clients.errorHandler import NotFoundException
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from ..clients.metaApi.metatraderAccount_client import MetatraderAccountClient, NewMetatraderAccountDto
from .streamingMetaApiConnection import StreamingMetaApiConnection
from ..clients.metaApi.reconnectListener import ReconnectListener
from ..clients.metaApi.historicalMarketData_client import HistoricalMarketDataClient
from .connectionRegistry import ConnectionRegistry
from .memoryHistoryStorage import MemoryHistoryStorage
from .historyStorage import HistoryStorage
from mock import AsyncMock, MagicMock, patch
from .metatraderAccountModel import MetatraderAccountModel
from ..clients.metaApi.expertAdvisor_client import ExpertAdvisorClient
from .expertAdvisor import ExpertAdvisor
from httpx import Response
from datetime import datetime
from .models import date
import pytest


class MockClient(MetatraderAccountClient):
    def get_accounts(self, provisioning_profile_id: str = None) -> Response:
        pass

    def get_account(self, id: str) -> Response:
        pass

    def create_account(self, account: NewMetatraderAccountDto) -> Response:
        pass

    def delete_account(self, id: str) -> Response:
        pass

    def deploy_account(self, id: str) -> Response:
        pass

    def undeploy_account(self, id: str) -> Response:
        pass

    def redeploy_account(self, id: str) -> Response:
        pass


class MockWebsocketClient(MetaApiWebsocketClient):
    def add_synchronization_listener(self, account_id: str, listener):
        pass

    def add_reconnect_listener(self, listener: ReconnectListener):
        pass

    def subscribe(self, account_id: str):
        pass


class MockStorage(MemoryHistoryStorage):
    async def last_history_order_time(self) -> datetime:
        return date('2020-01-01T00:00:00.000Z')

    async def last_deal_time(self) -> datetime:
        return date('2020-01-02T00:00:00.000Z')


class MockRegistry(ConnectionRegistry):
    def connect_rpc(self, account: MetatraderAccountModel):
        pass

    async def remove_rpc(self, account: MetatraderAccountModel):
        pass

    def connect_streaming(self, account: MetatraderAccountModel, history_storage: HistoryStorage,
                          history_start_time: datetime = None):
        pass

    async def remove_streaming(self, account: MetatraderAccountModel):
        pass

    def remove(self, account_id: str):
        pass


client: MockClient = None
websocket_client: MockWebsocketClient = None
registry: MockRegistry = None
api: MetatraderAccountApi = None
ea_client: ExpertAdvisorClient = None
history_client: HistoricalMarketDataClient = None
start_account = {}


@pytest.fixture(autouse=True)
async def run_around_tests():
    global client
    client = MockClient(MagicMock(), MagicMock())
    client.get_account = AsyncMock(return_value={
        '_id': 'id',
        'login': '50194988',
        'name': 'mt5a',
        'server': 'ICMarketsSC-Demo',
        'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
        'magic': 123456,
        'connectionStatus': 'CONNECTED',
        'state': 'DEPLOYED',
        'region': 'vint-hill',
        'type': 'cloud',
        'accountReplicas': [{
          '_id': 'idReplica',
          'state': 'CREATED',
          'magic': 0,
          'connectionStatus': 'CONNECTED',
          'symbol': 'EURUSD',
          'reliability': 'regular',
          'region': 'london'
        }]
      })
    global websocket_client
    websocket_client = MockWebsocketClient(MagicMock(), 'token')
    global registry
    registry = MockRegistry(websocket_client, MagicMock())
    global api
    registry.connect_streaming = MagicMock()
    registry.remove_streaming = AsyncMock()
    registry.connect_streaming = MagicMock()
    registry.remove_streaming = AsyncMock()
    registry.remove = MagicMock()
    global ea_client
    ea_client = ExpertAdvisorClient(MagicMock(), MagicMock())
    global history_client
    history_client = HistoricalMarketDataClient(MagicMock(), MagicMock())
    api = MetatraderAccountApi(client, websocket_client, registry, ea_client, history_client, 'MetaApi')
    yield


class TestMetatraderAccountApi:
    @pytest.mark.asyncio
    async def test_retrieve_mt_accounts(self):
        """Should retrieve MT accounts."""
        client.get_accounts = AsyncMock(return_value=[{'_id': 'id'}])
        accounts = await api.get_accounts({'provisioningProfileId': 'profileId'})
        assert list(map(lambda a: a.id, accounts)) == ['id']
        for account in accounts:
            assert isinstance(account, MetatraderAccount)
        client.get_accounts.assert_called_with({'provisioningProfileId': 'profileId'})

    @pytest.mark.asyncio
    async def test_retrieve_mt_account_by_id(self):
        """Should retrieve MT account by id."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'type': 'cloud',
            'quoteStreamingIntervalInSeconds': 2.5,
            'symbol': 'symbol',
            'reliability': 'high',
            'tags': ['tags'],
            'metadata': 'metadata',
            'resourceSlots': 1,
            'copyFactoryResourceSlots': 1,
            'region': 'region',
            'manualTrades': False,
            'slippage': 30,
            'version': 4,
            'hash': 12345,
            'baseCurrency': 'USD',
            'copyFactoryRoles': ['PROVIDER'],
            'riskManagementApiEnabled': False,
            'metastatsHourlyTarificationEnabled': False,
            'connections': [{
                'region': 'region',
                'zone': 'zone',
                'application': 'application'
            }],
            'primaryReplica': True,
            'userId': 'userId',
            'primaryAccountId': 'primaryId',
            'accountReplicas': [{
                '_id': 'replica0'
            }, {
                '_id': 'replica1'
            }],
            'accessToken': '2RUnoH1ldGbnEneCoqRTgI4QO1XOmVzbH5EVoQsA'
        })
        account = await api.get_account('id')
        assert account.id == 'id'
        assert account.login == '50194988'
        assert account.name == 'mt5a'
        assert account.server == 'ICMarketsSC-Demo'
        assert account.provisioning_profile_id == 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076'
        assert account.magic == 123456
        assert account.connection_status == 'DISCONNECTED'
        assert account.state == 'DEPLOYED'
        assert account.type == 'cloud'
        assert account.quote_streaming_interval_in_seconds == 2.5
        assert account.symbol == 'symbol'
        assert account.reliability == 'high'
        assert account.tags == ['tags']
        assert account.metadata == 'metadata'
        assert account.resource_slots == 1
        assert account.copyfactory_resource_slots == 1
        assert account.region == 'region'
        assert not account.manual_trades
        assert account.slippage == 30
        assert account.version == 4
        assert account.hash == 12345
        assert account.base_currency == 'USD'
        assert account.copy_factory_roles == ['PROVIDER']
        assert not account.risk_management_api_enabled
        assert not account.metastats_hourly_tarification_enabled
        assert account.connections == [{
            'region': 'region',
            'zone': 'zone',
            'application': 'application'
        }]
        assert account.primary_replica is True
        assert account.user_id == 'userId'
        assert account.primary_account_id == 'primaryId'
        for id in range(len(account.replicas)):
            replica = account.replicas[id]
            assert replica._data == {'_id': f'replica{id}'}

        assert account.access_token == '2RUnoH1ldGbnEneCoqRTgI4QO1XOmVzbH5EVoQsA'
        assert isinstance(account, MetatraderAccount)
        client.get_account.assert_called_with('id')

    @pytest.mark.asyncio
    async def test_retrieve_mt_account_by_token(self):
        """Should retrieve MT account by id."""
        client.get_account_by_token = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'type': 'cloud',
            'quoteStreamingIntervalInSeconds': 2.5,
            'symbol': 'symbol',
            'reliability': 'high',
            'tags': ['tags'],
            'metadata': 'metadata',
            'resourceSlots': 1,
            'copyFactoryResourceSlots': 1,
            'region': 'region',
            'manualTrades': False,
            'slippage': 30,
            'version': 4,
            'hash': 12345,
            'baseCurrency': 'USD',
            'copyFactoryRoles': ['PROVIDER'],
            'riskManagementApiEnabled': False,
            'metastatsHourlyTarificationEnabled': False,
            'connections': [{
                'region': 'region',
                'zone': 'zone',
                'application': 'application'
            }],
            'primaryReplica': True,
            'userId': 'userId',
            'accountReplicas': [{
                '_id': 'replica0'
            }, {
                '_id': 'replica1'
            }],
            'accessToken': '2RUnoH1ldGbnEneCoqRTgI4QO1XOmVzbH5EVoQsA'
        })
        account = await api.get_account_by_token()
        assert account.id == 'id'
        assert account.login == '50194988'
        assert account.name == 'mt5a'
        assert account.server == 'ICMarketsSC-Demo'
        assert account.provisioning_profile_id == 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076'
        assert account.magic == 123456
        assert account.connection_status == 'DISCONNECTED'
        assert account.state == 'DEPLOYED'
        assert account.type == 'cloud'
        assert account.quote_streaming_interval_in_seconds == 2.5
        assert account.symbol == 'symbol'
        assert account.reliability == 'high'
        assert account.tags == ['tags']
        assert account.metadata == 'metadata'
        assert account.resource_slots == 1
        assert account.copyfactory_resource_slots == 1
        assert account.region == 'region'
        assert not account.manual_trades
        assert account.slippage == 30
        assert account.version == 4
        assert account.hash == 12345
        assert account.base_currency == 'USD'
        assert account.copy_factory_roles == ['PROVIDER']
        assert not account.risk_management_api_enabled
        assert not account.metastats_hourly_tarification_enabled
        assert account.connections == [{
            'region': 'region',
            'zone': 'zone',
            'application': 'application'
        }]
        assert account.primary_replica is True
        assert account.user_id == 'userId'
        for id in range(len(account.replicas)):
            replica = account.replicas[id]
            assert replica._data == {'_id': f'replica{id}'}

        assert account.access_token == '2RUnoH1ldGbnEneCoqRTgI4QO1XOmVzbH5EVoQsA'
        assert isinstance(account, MetatraderAccount)
        client.get_account_by_token.assert_called_with()

    @pytest.mark.asyncio
    async def test_create_mt_account(self):
        """Should create MT account."""
        client.create_account = AsyncMock(return_value={'id': 'id'})
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accessToken': '2RUnoH1ldGbnEneCoqRTgI4QO1XOmVzbH5EVoQsA'
        })
        new_account_data = {
            'login': '50194988',
            'password': 'Test1234',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'region': 'vint-hill',
            'type': 'cloud',
            'accessToken': 'NyV5no9TMffJyUts2FjI80wly0so3rVCz4xOqiDx'
        }
        account = await api.create_account(new_account_data)
        assert account.id == 'id'
        assert account.login == '50194988'
        assert account.name == 'mt5a'
        assert account.server == 'ICMarketsSC-Demo'
        assert account.provisioning_profile_id == 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076'
        assert account.magic == 123456
        assert account.connection_status == 'DISCONNECTED'
        assert account.state == 'DEPLOYED'
        assert account.type == 'cloud'
        assert account.access_token == '2RUnoH1ldGbnEneCoqRTgI4QO1XOmVzbH5EVoQsA'
        assert isinstance(account, MetatraderAccount)
        client.create_account.assert_called_with(new_account_data)
        client.get_account.assert_called_with('id')

    @pytest.mark.asyncio
    async def test_reload_mt_account(self):
        """Should reload MT account."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
        }])
        account = await api.get_account('id')
        await account.reload()
        assert account.connection_status == 'CONNECTED'
        assert account.state == 'DEPLOYED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_remove_mt_account(self):
        """Should remove MT account."""
        with patch('lib.metaApi.metatraderAccount.FilesystemHistoryDatabase.clear',
                   new_callable=AsyncMock) as delete_mock:
            client.get_account = AsyncMock(side_effect=[{
                '_id': 'id',
                'login': '50194988',
                'name': 'mt5a',
                'server': 'ICMarketsSC-Demo',
                'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
                'magic': 123456,
                'connectionStatus': 'CONNECTED',
                'state': 'DEPLOYED',
                'region': 'vint-hill',
                'type': 'cloud'
            }, {
                '_id': 'id',
                'login': '50194988',
                'name': 'mt5a',
                'server': 'ICMarketsSC-Demo',
                'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
                'magic': 123456,
                'connectionStatus': 'CONNECTED',
                'state': 'DELETING',
                'region': 'vint-hill',
                'type': 'cloud'
            }
            ])
            client.delete_account = AsyncMock()
            account = await api.get_account('id')
            await account.remove()
            delete_mock.assert_called_with('id', 'MetaApi')
            registry.remove.assert_called_with('id')
            assert account.state == 'DELETING'
            client.delete_account.assert_called_with('id')
            client.get_account.assert_called_with('id')
            assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_deploy_mt_account(self):
        """Should deploy MT account."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'UNDEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }])
        client.deploy_account = AsyncMock()
        account = await api.get_account('id')
        await account.deploy()
        assert account.state == 'DEPLOYING'
        client.deploy_account.assert_called_with('id')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_undeploy_mt_account(self):
        """Should undeploy MT account."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'UNDEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }])
        client.undeploy_account = AsyncMock()
        account = await api.get_account('id')
        await account.undeploy()
        registry.remove.assert_called_with('id')
        assert account.state == 'UNDEPLOYING'
        client.undeploy_account.assert_called_with('id')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_redeploy_mt_account(self):
        """Should redeploy MT account."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'UNDEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }])
        client.redeploy_account = AsyncMock()
        account = await api.get_account('id')
        await account.redeploy()
        assert account.state == 'UNDEPLOYING'
        client.redeploy_account.assert_called_with('id')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_increase_reliability(self):
        """Should increase MT account reliability."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'UNDEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud',
            'reliability': 'high'
        }])
        client.increase_reliability = AsyncMock()
        account = await api.get_account('id')
        await account.increase_reliability()
        assert account.reliability == 'high'
        client.increase_reliability.assert_called_with('id')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_enable_account_risk_management_api(self):
        """Should enable account risk management api."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'application': 'MetaApi',
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'application': 'MetaApi',
            'connectionStatus': 'CONNECTED',
            'state': 'UNDEPLOYING',
            'type': 'cloud',
            'riskManagementApiEnabled': True
        }])
        client.enable_risk_management_api = AsyncMock()
        account = await api.get_account('id')
        await account.enable_risk_management_api()
        assert account.risk_management_api_enabled
        client.enable_risk_management_api.assert_called_with('id')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_enable_account_metastats_hourly_tarification(self):
        """Should enable account MetaStats hourly tarification."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'application': 'MetaApi',
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'application': 'MetaApi',
            'connectionStatus': 'CONNECTED',
            'state': 'UNDEPLOYING',
            'type': 'cloud',
            'metastatsHourlyTarificationEnabled': True
        }])
        client.enable_metastats_hourly_tarification = AsyncMock()
        account = await api.get_account('id')
        await account.enable_metastats_hourly_tarification()
        assert account.metastats_hourly_tarification_enabled is True
        client.enable_metastats_hourly_tarification.assert_called_with('id')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_wait_for_deployment(self):
        """Should wait for deployment."""
        deploying_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }
        client.get_account = AsyncMock(side_effect=[deploying_account, deploying_account,
                                                    {
                                                        '_id': 'id',
                                                        'login': '50194988',
                                                        'name': 'mt5a',
                                                        'server': 'ICMarketsSC-Demo',
                                                        'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
                                                        'magic': 123456,

                                                        'connectionStatus': 'CONNECTED',
                                                        'state': 'DEPLOYED',
                                                        'region': 'vint-hill',
                                                        'type': 'cloud'
                                                    }
                                                    ])
        account = await api.get_account('id')
        await account.wait_deployed(1, 50)
        assert account.state == 'DEPLOYED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_deployment(self):
        """Should time out waiting for deployment."""
        deploying_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }
        client.get_account = AsyncMock(return_value=deploying_account)
        account = await api.get_account('id')
        try:
            await account.wait_deployed(1, 50)
            raise Exception('TimeoutError is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            assert account.state == 'DEPLOYING'
        client.get_account.assert_called_with('id')

    @pytest.mark.asyncio
    async def test_wait_for_undeployment(self):
        """Should wait for undeployment."""
        undeploying_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'UNDEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }
        client.get_account = AsyncMock(side_effect=[undeploying_account, undeploying_account,
                                                    {
                                                        '_id': 'id',
                                                        'login': '50194988',
                                                        'name': 'mt5a',
                                                        'server': 'ICMarketsSC-Demo',
                                                        'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
                                                        'magic': 123456,
                                                        'connectionStatus': 'CONNECTED',
                                                        'state': 'UNDEPLOYED',
                                                        'region': 'vint-hill',
                                                        'type': 'cloud'
                                                    }
                                                    ])
        account = await api.get_account('id')
        await account.wait_undeployed(1, 50)
        assert account.state == 'UNDEPLOYED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_undeployment(self):
        """Should wait for undeployment."""
        undeploying_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'UNDEPLOYING',
            'region': 'vint-hill',
            'type': 'cloud'
        }
        client.get_account = AsyncMock(return_value=undeploying_account)
        account = await api.get_account('id')
        try:
            await account.wait_undeployed(1, 50)
            raise Exception('TimeoutException is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            assert account.state == 'UNDEPLOYING'
        client.get_account.assert_called_with('id')

    @pytest.mark.asyncio
    async def test_wait_until_removed(self):
        """Should wait until removed."""
        deleting_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DELETING',
            'region': 'vint-hill',
            'type': 'cloud'
          }
        client.get_account = AsyncMock(side_effect=[deleting_account, deleting_account, NotFoundException('')])
        account = await api.get_account('id')
        await account.wait_removed(1, 50)
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_waiting_until_removed(self):
        """Should wait until removed."""
        deleting_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DELETING',
            'region': 'vint-hill',
            'type': 'cloud'
        }
        client.get_account = AsyncMock(return_value=deleting_account)
        account = await api.get_account('id')
        try:
            await account.wait_removed(1, 50)
            raise Exception('TimeoutException is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
        client.get_account.assert_called_with('id')

    @pytest.mark.asyncio
    async def test_wait_until_broker_connection(self):
        """Should wait util broker connection."""
        disconnected_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
        }
        client.get_account = AsyncMock(side_effect=[disconnected_account, disconnected_account,
                                                    {
                                                        '_id': 'id',
                                                        'login': '50194988',
                                                        'name': 'mt5a',
                                                        'server': 'ICMarketsSC-Demo',
                                                        'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
                                                        'magic': 123456,
                                                        'connectionStatus': 'CONNECTED',
                                                        'state': 'DEPLOYED',
                                                        'region': 'vint-hill',
                                                        'type': 'cloud'
                                                    }])
        account = await api.get_account('id')
        await account.wait_connected(1, 50)
        assert account.connection_status == 'CONNECTED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_waiting_for_broker_connection(self):
        """Should time out waiting for broker connection."""
        disconnected_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
        }
        client.get_account = AsyncMock(return_value=disconnected_account)
        account = await api.get_account('id')
        try:
            await account.wait_connected(1, 50)
            raise Exception('TimeoutException is expected')
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            assert account.connection_status == 'DISCONNECTED'
        client.get_account.assert_called_with('id')

    @pytest.mark.asyncio
    async def test_pass_connected_for_primary_account_if_replica_connected(self):
        """Should pass for primary account if replica is connected."""
        disconnected_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'UNDEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYED',
                'magic': 0,
                'connectionStatus': 'DISCONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        client.get_account = AsyncMock(side_effect=[disconnected_account, disconnected_account, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'state': 'UNDEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYED',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }])
        account = await api.get_account('id')
        await account.wait_connected(1, 50)
        replica = account.replicas[0]
        assert replica.connection_status == 'CONNECTED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_connect_to_mt_terminal(self):
        """Should connect to an MT terminal."""
        with patch('lib.metaApi.streamingMetaApiConnection.StreamingMetaApiConnection.initialize', AsyncMock()):
            websocket_client.add_synchronization_listener = MagicMock()
            websocket_client.subscribe = AsyncMock()
            client.get_account = AsyncMock(return_value={'_id': 'id'})
            account = await api.get_account('id')
            storage = MockStorage()
            account.get_streaming_connection(storage)
            registry.connect_streaming.assert_called_with(account, storage, None)

    @pytest.mark.asyncio
    async def test_connect_to_terminal_if_in_specified_region(self):
        """Should connect to an MT terminal if in specified region."""
        websocket_client._region = 'vint-hill'
        client.get_account = AsyncMock(return_value={'_id': 'id', 'region': 'vint-hill'})
        account = await api.get_account('accountId')
        storage = MockStorage()
        account.get_streaming_connection(storage)
        registry.connect_streaming.assert_called_with(account, storage, None)

    @pytest.mark.asyncio
    async def test_not_connect_to_terminal_if_in_different_region(self):
        """Should not connect to an MT terminal if in different region."""
        websocket_client._region = 'vint-hill'
        client.get_account = AsyncMock(return_value={'_id': 'id', 'region': 'new-york'})
        account = await api.get_account('accountId')
        storage = MockStorage()
        connect_mock = MagicMock()
        registry.connect = connect_mock
        try:
            account.get_streaming_connection(storage)
            pytest.fail()
        except Exception as err:
            assert err.args[0] == \
                   'Account id is not on specified region vint-hill, check error.details for more information'

    @pytest.mark.asyncio
    async def test_create_rpc_connection(self):
        """Should create RPC connection."""
        websocket_client._region = None
        client.get_account = AsyncMock(return_value={'_id': 'id', 'region': 'vint-hill'})
        account = await api.get_account('accountId')
        connect_mock = MagicMock()
        registry.connect = connect_mock
        account.get_rpc_connection()

    @pytest.mark.asyncio
    async def test_create_rpc_connection_if_in_specified_region(self):
        """Should create RPC connection if in specified region."""
        websocket_client._region = 'vint-hill'
        client.get_account = AsyncMock(return_value={'_id': 'id', 'region': 'vint-hill'})
        account = await api.get_account('accountId')
        connect_mock = MagicMock()
        registry.connect = connect_mock
        account.get_rpc_connection()

    @pytest.mark.asyncio
    async def test_not_create_rpc_connection_if_in_different_region(self):
        """Should not create RPC connection if in different region."""
        websocket_client._region = 'vint-hill'
        client.get_account = AsyncMock(return_value={'_id': 'id', 'region': 'new-york'})
        account = await api.get_account('accountId')
        connect_mock = MagicMock()
        registry.connect = connect_mock
        try:
            account.get_rpc_connection()
            pytest.fail()
        except Exception as err:
            assert err.args[0] == 'Account id is not on specified region vint-hill, check error.details for ' +\
                'more information'

    @pytest.mark.asyncio
    async def test_update_mt_account(self):
        """Should update MT account."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
          }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a__',
            'server': 'OtherMarkets-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud'
          }])
        client.update_account = AsyncMock()
        account = await api.get_account('id')
        await account.update({
          'name': 'mt5a__',
          'password': 'moreSecurePass',
          'server': 'OtherMarkets-Demo',
        })
        assert account.name == 'mt5a__'
        assert account.server == 'OtherMarkets-Demo'
        client.update_account.assert_called_with('id', {
          'name': 'mt5a__',
          'password': 'moreSecurePass',
          'server': 'OtherMarkets-Demo',
        })
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_retrieve_expert_advisors(self):
        """Should retrieve expert advisors."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g1'
        })
        ea_client.get_expert_advisors = AsyncMock(return_value=[{'expertId': 'ea'}])
        account = await api.get_account('id')
        experts = await account.get_expert_advisors()
        assert list(map(lambda e: e.expert_id, experts)) == ['ea']
        for ea in experts:
            assert isinstance(ea, ExpertAdvisor)
        ea_client.get_expert_advisors.assert_called_with('id')

    @pytest.mark.asyncio
    async def test_retrieve_expert_advisor(self):
        """Should retrieve expert advisor by expert id."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g1'
        })
        ea_client.get_expert_advisor = AsyncMock(return_value={
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        })
        account = await api.get_account('id')
        expert = await account.get_expert_advisor('ea')
        assert expert.expert_id == 'ea'
        assert expert.period == '1H'
        assert expert.symbol == 'EURUSD'
        assert not expert.file_uploaded
        assert isinstance(expert, ExpertAdvisor)
        ea_client.get_expert_advisor.assert_called_with('id', 'ea')

    @pytest.mark.asyncio
    async def test_validate_account_version(self):
        """Should validate account version."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 5,
            'type': 'cloud-g1'
        })
        ea_client.get_expert_advisors = AsyncMock(return_value=[{
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }])
        ea_client.get_expert_advisor = AsyncMock(return_value={
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        })
        ea_client.update_expert_advisor = AsyncMock()
        new_expert_advisor = {
            'period': '1H',
            'symbol': 'EURUSD',
            'preset': 'preset'
        }
        account = await api.get_account('id')
        try:
            await account.get_expert_advisors()
            pytest.fail()
        except Exception:
            pass
        try:
            await account.get_expert_advisor('ea')
            pytest.fail()
        except Exception:
            pass
        try:
            await account.create_expert_advisor('ea', new_expert_advisor)
            pytest.fail()
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_validate_account_type(self):
        """Should validate account type."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g2'
        })
        ea_client.get_expert_advisors = AsyncMock(return_value=[{
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }])
        ea_client.get_expert_advisor = AsyncMock(return_value={
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        })
        ea_client.update_expert_advisor = AsyncMock()
        new_expert_advisor = {
            'period': '1H',
            'symbol': 'EURUSD',
            'preset': 'preset'
        }
        account = await api.get_account('id')
        try:
            await account.get_expert_advisors()
            pytest.fail()
        except Exception:
            pass
        try:
            await account.get_expert_advisor('ea')
            pytest.fail()
        except Exception:
            pass
        try:
            await account.create_expert_advisor('ea', new_expert_advisor)
            pytest.fail()
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_create_expert_advisor(self):
        """Should create expert advisor."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g1'
        })
        ea_client.update_expert_advisor = AsyncMock()
        ea_client.get_expert_advisor = AsyncMock(return_value={
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        })
        new_expert_advisor = {
          'period': '1H',
          'symbol': 'EURUSD',
          'preset': 'preset'
        }
        account = await api.get_account('id')
        expert = await account.create_expert_advisor('ea', new_expert_advisor)
        assert expert.expert_id == 'ea'
        assert expert.period == '1H'
        assert expert.symbol == 'EURUSD'
        assert not expert.file_uploaded
        assert isinstance(expert, ExpertAdvisor)
        ea_client.update_expert_advisor.assert_called_with('id', 'ea', new_expert_advisor)
        ea_client.get_expert_advisor.assert_called_with('id', 'ea')

    @pytest.mark.asyncio
    async def test_reload_expert_advisor(self):
        """Should reload expert advisor."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g1'
        })
        ea_client.get_expert_advisor = AsyncMock(side_effect=[{
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }, {
            'expertId': 'ea',
            'period': '4H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }])
        account = await api.get_account('id')
        expert = await account.get_expert_advisor('ea')
        await expert.reload()
        assert expert.period == '4H'
        ea_client.get_expert_advisor.assert_called_with('id', 'ea')
        assert ea_client.get_expert_advisor.call_count == 2

    @pytest.mark.asyncio
    async def test_update_expert_advisor(self):
        """Should update expert advisor."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g1'
        })
        ea_client.get_expert_advisor = AsyncMock(side_effect=[{
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }, {
            'expertId': 'ea',
            'period': '4H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }])
        new_expert_advisor = {
            'period': '4H',
            'symbol': 'EURUSD',
            'preset': 'preset'
        }
        ea_client.update_expert_advisor = AsyncMock()
        account = await api.get_account('id')
        expert = await account.get_expert_advisor('ea')
        await expert.update(new_expert_advisor)
        assert expert.period == '4H'
        ea_client.update_expert_advisor.assert_called_with('id', 'ea', new_expert_advisor)
        assert ea_client.get_expert_advisor.call_count == 2
        ea_client.get_expert_advisor.assert_called_with('id', 'ea')

    @pytest.mark.asyncio
    async def test_upload_expert_advisor_file(self):
        """Should upload expert advisor file."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g1'
        })
        ea_client.get_expert_advisor = AsyncMock(side_effect=[{
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }, {
            'expertId': 'ea',
            'period': '4H',
            'symbol': 'EURUSD',
            'fileUploaded': True
        }])
        ea_client.upload_expert_advisor_file = AsyncMock()
        account = await api.get_account('id')
        expert = await account.get_expert_advisor('ea')
        await expert.upload_file('/path/to/file')
        assert expert.file_uploaded
        ea_client.upload_expert_advisor_file.assert_called_with('id', 'ea', '/path/to/file')
        assert ea_client.get_expert_advisor.call_count == 2
        ea_client.get_expert_advisor.assert_called_with('id', 'ea')

    @pytest.mark.asyncio
    async def test_remove_expert_advisor(self):
        """Should remove expert advisor."""
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'version': 4,
            'type': 'cloud-g1'
        })
        ea_client.get_expert_advisor = AsyncMock(return_value={
            'expertId': 'ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        })
        ea_client.delete_expert_advisor = AsyncMock(return_value={'_id': 'id'})
        account = await api.get_account('id')
        expert = await account.get_expert_advisor('ea')
        await expert.remove()
        ea_client.delete_expert_advisor.assert_called_with('id', 'ea')

    @pytest.mark.asyncio
    async def test_create_replica(self):
        """Should create MT account replica."""
        client.get_account = AsyncMock(side_effect=[{
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'type': 'cloud'
        }, {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'CREATED',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }])
        client.create_account_replica = AsyncMock()
        account = await api.get_account('id')
        replica = await account.create_replica({
            'magic': 0,
            'symbol': 'EURUSD',
            'reliability': 'regular',
            'region': 'london'
        })

        assert replica.id == 'idReplica'
        assert replica.state == 'CREATED'
        assert replica.magic == 0
        assert replica.connection_status == 'CONNECTED'
        assert replica.reliability == 'regular'
        assert replica.region == 'london'
        client.create_account_replica.assert_called_with('id', {
            'magic': 0,
            'symbol': 'EURUSD',
            'reliability': 'regular',
            'region': 'london'
        })
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 2

    @pytest.mark.asyncio
    async def test_retrieve_mt_account_replica_by_id(self):
        """Should retrieve MT account replica by id."""
        client.get_account_replica = AsyncMock(return_value={
            '_id': 'id',
            'state': 'DEPLOYED',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'quoteStreamingIntervalInSeconds': 2.5,
            'symbol': 'symbol',
            'reliability': 'high',
            'tags': ['tags'],
            'metadata': 'metadata',
            'resourceSlots': 1,
            'copyFactoryResourceSlots': 1,
            'region': 'region',
            'primaryAccount': {
                '_id': 'id',
                'primaryReplica': True
            }
        })
        replica = await api.get_account_replica('accountId', 'replicaId')
        assert replica.id == 'id'
        assert replica.magic == 123456
        assert replica.connection_status == 'DISCONNECTED'
        assert replica.state == 'DEPLOYED'
        assert replica.quote_streaming_interval_in_seconds == 2.5
        assert replica.symbol == 'symbol'
        assert replica.reliability == 'high'
        assert replica.tags == ['tags']
        assert replica.metadata == 'metadata'
        assert replica.resource_slots == 1
        assert replica.copyfactory_resource_slots == 1
        assert replica.region == 'region'
        assert replica.primary_account_from_dto == {
            '_id': 'id',
            'primaryReplica': True
        }
        client.get_account_replica.assert_called_with('accountId', 'replicaId')

    @pytest.mark.asyncio
    async def test_retrieve_mt_account_replicas(self):
        """Should retrieve MT account replicas."""
        client.get_account_replicas = AsyncMock(return_value=[{
            '_id': 'id0',
            'state': 'DEPLOYED',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'quoteStreamingIntervalInSeconds': 2.5,
            'symbol': 'symbol',
            'reliability': 'high',
            'tags': ['tags'],
            'metadata': 'metadata',
            'resourceSlots': 1,
            'copyFactoryResourceSlots': 1,
            'region': 'region',
            'primaryAccount': {
                '_id': 'id',
                'primaryReplica': True
            }
        }, {
            '_id': 'id1',
            'state': 'DEPLOYED',
            'magic': 123456,
            'connectionStatus': 'DISCONNECTED',
            'quoteStreamingIntervalInSeconds': 2.5,
            'symbol': 'symbol',
            'reliability': 'high',
            'tags': ['tags'],
            'metadata': 'metadata',
            'resourceSlots': 1,
            'copyFactoryResourceSlots': 1,
            'region': 'region',
            'primaryAccount': {
                '_id': 'id',
                'primaryReplica': True
            }
        }])
        replicas = await api.get_account_replicas('accountId')
        for id in range(len(replicas)):
            replica = replicas[id]
            assert replica.id == f'id{id}'
            assert replica.magic == 123456
            assert replica.connection_status == 'DISCONNECTED'
            assert replica.state == 'DEPLOYED'
            assert replica.quote_streaming_interval_in_seconds == 2.5
            assert replica.symbol == 'symbol'
            assert replica.reliability == 'high'
            assert replica.tags == ['tags']
            assert replica.metadata == 'metadata'
            assert replica.resource_slots == 1
            assert replica.copyfactory_resource_slots == 1
            assert replica.region == 'region'
            assert replica.primary_account_from_dto == {
                '_id': 'id',
                'primaryReplica': True
            }
        client.get_account_replicas.assert_called_with('accountId')

    @pytest.mark.asyncio
    async def test_remove_replica(self):
        """Should remove MT account replica."""
        account = await api.get_account('id')
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DELETING',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        })
        client.delete_account_replica = AsyncMock()
        replica = account.replicas[0]
        await replica.remove()
        assert replica.state == 'DELETING'
        client.delete_account_replica.assert_called_with('id', 'idReplica')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 1

    @pytest.mark.asyncio
    async def test_deploy_replica(self):
        """Should deploy MT account replica."""
        account = await api.get_account('id')
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYING',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        })
        client.deploy_account_replica = AsyncMock()
        replica = account.replicas[0]
        await replica.deploy()
        assert replica.state == 'DEPLOYING'
        client.deploy_account_replica.assert_called_with('id', 'idReplica')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 1

    @pytest.mark.asyncio
    async def test_undeploy_replica(self):
        """Should undeploy MT account replica."""
        account = await api.get_account('id')
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'UNDEPLOYING',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        })
        client.undeploy_account_replica = AsyncMock()
        replica = account.replicas[0]
        await replica.undeploy()
        assert replica.state == 'UNDEPLOYING'
        client.undeploy_account_replica.assert_called_with('id', 'idReplica')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 1

    @pytest.mark.asyncio
    async def test_redeploy_replica(self):
        """Should redeploy MT account replica."""
        account = await api.get_account('id')
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'UNDEPLOYING',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        })
        client.redeploy_account_replica = AsyncMock()
        replica = account.replicas[0]
        await replica.redeploy()
        assert replica.state == 'UNDEPLOYING'
        client.redeploy_account_replica.assert_called_with('id', 'idReplica')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 1

    @pytest.mark.asyncio
    async def test_update_replica(self):
        """Should update MT account replica."""
        account = await api.get_account('id')
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYED',
                'magic': 12345,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        })
        client.update_account_replica = AsyncMock()
        replica = account.replicas[0]
        await replica.update({
            'magic': 12345
        })
        assert replica.magic == 12345
        client.update_account_replica.assert_called_with('id', 'idReplica', {
            'magic': 12345
        })
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 1

    @pytest.mark.asyncio
    async def test_increase_replica_reliability(self):
        """Should increase MT account replica reliability."""
        account = await api.get_account('id')
        client.get_account = AsyncMock(return_value={
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYING',
                'magic': 0,
                'connectionStatus': 'DISCONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'high',
                'region': 'london'
            }]
        })
        client.increase_reliability = AsyncMock()
        replica = account.replicas[0]
        await replica.increase_reliability()
        assert replica.reliability == 'high'
        client.increase_reliability.assert_called_with('idReplica')
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 1

    @pytest.fixture()
    async def wait_deploy(self):
        global start_account
        start_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYING',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        yield

    @pytest.mark.asyncio
    async def test_wait_deploy_replica(self, wait_deploy):
        """Should wait for deployment."""
        account = await api.get_account('id')
        updated_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYED',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        client.get_account = AsyncMock(side_effect=[start_account, start_account, updated_account])
        replica = account.replicas[0]
        await replica.wait_deployed(1, 50)
        assert replica.state == 'DEPLOYED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_deploy_replica(self, wait_deploy):
        """Should time out waiting for deployment."""
        client.get_account = AsyncMock(return_value=start_account)
        account = await api.get_account('id')
        replica = account.replicas[0]
        try:
            await replica.wait_deployed(1, 50)
            pytest.fail()
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            assert replica.state == 'DEPLOYING'
        client.get_account.assert_called_with('id')

    @pytest.fixture()
    async def wait_undeploy(self):
        global start_account
        start_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'UNDEPLOYING',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        yield

    @pytest.mark.asyncio
    async def test_wait_undeploy_replica(self, wait_undeploy):
        """Should wait for undeployment."""
        account = await api.get_account('id')
        updated_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'UNDEPLOYED',
                'magic': 0,
                'connectionStatus': 'DISCONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        client.get_account = AsyncMock(side_effect=[start_account, start_account, updated_account])
        replica = account.replicas[0]
        await replica.wait_undeployed(1, 50)
        assert replica.state == 'UNDEPLOYED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_undeploy_replica(self, wait_undeploy):
        """Should time out waiting for undeployment."""
        client.get_account = AsyncMock(return_value=start_account)
        account = await api.get_account('id')
        replica = account.replicas[0]
        try:
            await replica.wait_undeployed(1, 50)
            pytest.fail()
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            assert replica.state == 'UNDEPLOYING'
        client.get_account.assert_called_with('id')

    @pytest.fixture()
    async def wait_remove(self):
        global start_account
        start_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DELETING',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        yield

    @pytest.mark.asyncio
    async def test_wait_remove_replica(self, wait_remove):
        """Should wait until removed."""
        updated_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': []
        }
        client.get_account = AsyncMock(side_effect=[start_account, start_account, updated_account])
        account = await api.get_account('id')
        replica = account.replicas[0]
        await replica.wait_removed(1, 50)
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_remove_replica(self, wait_remove):
        """Should time out waiting until removed."""
        client.get_account = AsyncMock(return_value=start_account)
        account = await api.get_account('id')
        replica = account.replicas[0]
        try:
            await replica.wait_removed(1, 50)
            pytest.fail()
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            assert replica.state == 'DELETING'
        client.get_account.assert_called_with('id')

    @pytest.fixture()
    async def wait_connected(self):
        global start_account
        start_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'region': 'vint-hill',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYED',
                'magic': 0,
                'connectionStatus': 'DISCONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        yield

    @pytest.mark.asyncio
    async def test_wait_connected_replica(self, wait_connected):
        """Should wait until broker connection."""
        updated_account = {
            '_id': 'id',
            'login': '50194988',
            'name': 'mt5a',
            'server': 'ICMarketsSC-Demo',
            'provisioningProfileId': 'f9ce1f12-e720-4b9a-9477-c2d4cb25f076',
            'magic': 123456,
            'connectionStatus': 'CONNECTED',
            'state': 'DEPLOYED',
            'type': 'cloud',
            'accountReplicas': [{
                '_id': 'idReplica',
                'state': 'DEPLOYED',
                'magic': 0,
                'connectionStatus': 'CONNECTED',
                'symbol': 'EURUSD',
                'reliability': 'regular',
                'region': 'london'
            }]
        }
        client.get_account = AsyncMock(side_effect=[start_account, start_account, updated_account])
        account = await api.get_account('id')
        replica = account.replicas[0]
        await replica.wait_connected(1, 50)
        assert replica.connection_status == 'CONNECTED'
        client.get_account.assert_called_with('id')
        assert client.get_account.call_count == 3

    @pytest.mark.asyncio
    async def test_time_out_connected_replica(self, wait_connected):
        """Should time out waiting for broker connection."""
        client.get_account = AsyncMock(return_value=start_account)
        account = await api.get_account('id')
        replica = account.replicas[0]
        try:
            await replica.wait_connected(1, 50)
            pytest.fail()
        except Exception as err:
            assert err.__class__.__name__ == 'TimeoutException'
            assert replica.connection_status == 'DISCONNECTED'
        client.get_account.assert_called_with('id')
