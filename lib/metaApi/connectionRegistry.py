from .rpcMetaApiConnection import RpcMetaApiConnection
from .streamingMetaApiConnection import StreamingMetaApiConnection
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from ..clients.metaApi.clientApi_client import ClientApiClient
from .metatraderAccountModel import MetatraderAccountModel
from .historyStorage import HistoryStorage
from .connectionRegistryModel import ConnectionRegistryModel
from .rpcMetaApiConnectionInstance import RpcMetaApiConnectionInstance
from .streamingMetaApiConnectionInstance import StreamingMetaApiConnectionInstance
from datetime import datetime
import asyncio


class ConnectionRegistry(ConnectionRegistryModel):
    """Manages account connections"""

    def __init__(self, meta_api_websocket_client: MetaApiWebsocketClient, client_api_client: ClientApiClient,
                 application: str = 'MetaApi', refresh_subscriptions_opts: dict = None):
        """Inits a MetaTrader connection registry instance.

        Args:
            meta_api_websocket_client: MetaApi websocket client.
            client_api_client: Client API client.
            application: Application type.
            refresh_subscriptions_opts: Subscriptions refresh options.
        """
        refresh_subscriptions_opts = refresh_subscriptions_opts or {}
        self._meta_api_websocket_client = meta_api_websocket_client
        self._client_api_client = client_api_client
        self._application = application
        self._refresh_subscriptions_opts = refresh_subscriptions_opts
        self._rpcConnections = {}
        self._streamingConnections = {}
        self._connectionLocks = {}

    def connect_streaming(self, account: MetatraderAccountModel, history_storage: HistoryStorage,
                          history_start_time: datetime = None) -> StreamingMetaApiConnectionInstance:
        """Creates and returns a new account connection if doesnt exist, otherwise returns old.

        Args:
            account: MetaTrader account to connect to.
            history_storage: Terminal history storage.
            history_start_time: History start time.

        Returns:
            A coroutine resolving with account connection.
        """
        if account.id not in self._streamingConnections:
            self._streamingConnections[account.id] = StreamingMetaApiConnection(
                self._meta_api_websocket_client, self._client_api_client, account, history_storage, self,
                history_start_time, self._refresh_subscriptions_opts)

        return StreamingMetaApiConnectionInstance(self._meta_api_websocket_client,
                                                  self._streamingConnections[account.id])

    async def remove_streaming(self, account: MetatraderAccountModel):
        """Removes a streaming connection from registry.

        Args:
            account: MetaTrader account to remove from registry.
        """
        if account.id in self._streamingConnections:
            del self._streamingConnections[account.id]
        if account.id not in self._rpcConnections:
            await self._close_last_connection(account)

    def connect_rpc(self, account: MetatraderAccountModel) -> RpcMetaApiConnectionInstance:
        """Creates and returns a new account connection if doesnt exist, otherwise returns old.

        Args:
            account: MetaTrader account to connect to.

        Returns:
            A coroutine resolving with account connection.
        """
        if account.id not in self._rpcConnections:
            self._rpcConnections[account.id] = RpcMetaApiConnection(self._meta_api_websocket_client, account, self)
        return RpcMetaApiConnectionInstance(self._meta_api_websocket_client, self._rpcConnections[account.id])

    async def remove_rpc(self, account: MetatraderAccountModel):
        """Removes an RPC connection from registry.

        Args:
            account: MetaTrader account to remove from registry.
        """
        if account.id in self._rpcConnections:
            del self._rpcConnections[account.id]
        if account.id not in self._streamingConnections:
            await self._close_last_connection(account)

    def remove(self, account_id: str):
        """Removes an account from registry.

        Args:
            account_id: MetaTrader account id to remove.
        """
        if account_id in self._rpcConnections:
            del self._rpcConnections[account_id]
        if account_id in self._streamingConnections:
            del self._streamingConnections[account_id]

    @property
    def application(self) -> str:
        """Returns application type.

        Returns:
            Application type.
        """
        return self._application

    async def _close_last_connection(self, account: MetatraderAccountModel):
        account_regions = account.account_regions
        await asyncio.gather(*list(map(lambda replica_id: self._meta_api_websocket_client.unsubscribe(replica_id),
                                       account_regions.values())))
