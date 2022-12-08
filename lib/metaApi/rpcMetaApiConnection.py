from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from .metatraderAccountModel import MetatraderAccountModel
from datetime import datetime
from typing import TypedDict
import asyncio
from ..logger import LoggerManager
from .metaApiConnection import MetaApiConnection
from .connectionRegistryModel import ConnectionRegistryModel
from ..clients.timeoutException import TimeoutException


class RpcMetaApiConnectionDict(TypedDict, total=False):
    instanceIndex: int
    synchronized: bool
    disconnected: bool


class RpcMetaApiConnection(MetaApiConnection):
    """Exposes MetaApi MetaTrader RPC API connection to consumers."""

    def __init__(self, websocket_client: MetaApiWebsocketClient, account: MetatraderAccountModel,
                 connection_registry: ConnectionRegistryModel):
        """Inits MetaApi MetaTrader RPC Api connection.

        Args:
            websocket_client: MetaApi websocket client.
            account: MetaTrader account id to connect to.
        """
        super().__init__(websocket_client, account, 'RPC')
        self._connectionRegistry = connection_registry
        self._websocketClient.add_synchronization_listener(account.id, self)
        self._stateByInstanceIndex = {}
        self._openedInstances = []
        for replica_id in account.account_regions.values():
            self._websocketClient.add_reconnect_listener(self, replica_id)
        self._logger = LoggerManager.get_logger('RpcMetaApiConnection')

    async def connect(self, instance_id: str):
        """Opens the connection. Can only be called the first time, next calls will be ignored.

        Args:
            instance_id: Connection instance id.

        Returns:
            A coroutine resolving when the connection is opened
        """
        if instance_id not in self._openedInstances:
            self._openedInstances.append(instance_id)
        if not self._opened:
            self._opened = True
            account_regions = self._account.account_regions
            self._websocketClient.add_account_cache(self.account.id, account_regions)
            for region in account_regions.keys():
                self._websocketClient.ensure_subscribe(account_regions[region], 0)
                self._websocketClient.ensure_subscribe(account_regions[region], 1)

    async def close(self, instance_id: str):
        """Closes the connection. The instance of the class should no longer be used after this method is invoked.

        Args:
            instance_id: Connection instance id.
        """
        if self._opened:
            self._openedInstances = list(filter(lambda id: id != instance_id, self._openedInstances))
            if not len(self._openedInstances) and not self._closed:
                await self._connectionRegistry.remove_rpc(self.account)
                self._websocketClient.remove_synchronization_listener(self.account.id, self)
                self._websocketClient.remove_account_cache(self.account.id)
                self._websocketClient.remove_reconnect_listener(self)
                self._closed = True

    async def on_connected(self, instance_index: str, replicas: int):
        """Invoked when connection to MetaTrader terminal established.

        Args:
            instance_index: Index of an account instance connected.
            replicas: Number of account replicas launched.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['synchronized'] = True

    async def on_disconnected(self, instance_index: str):
        """Invoked when connection to MetaTrader terminal terminated.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
             A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['synchronized'] = False
        self._logger.debug(f'{self._account.id}:{instance_index}: disconnected from broker')

    async def on_stream_closed(self, instance_index: str):
        """Invoked when a stream for an instance index is closed.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        if instance_index in self._stateByInstanceIndex:
            del self._stateByInstanceIndex[instance_index]

    def is_synchronized(self) -> bool:
        """Returns flag indicating status of state synchronization with MetaTrader terminal.

        Returns:
            A coroutine resolving with a flag indicating status of state synchronization with MetaTrader terminal.
        """
        return True in list(map(lambda instance: instance['synchronized'], self._stateByInstanceIndex.values()))

    async def wait_synchronized(self, timeout_in_seconds: float = 300):
        """Waits until synchronization to RPC application is completed.

        Args:
            timeout_in_seconds: Timeout for synchronization.

        Returns:
            A coroutine which resolves when synchronization to RPC application is completed.

        Raises:
            TimeoutException: If application failed to synchronize with the terminal within timeout allowed.
        """
        self._check_is_connection_active()
        start_time = datetime.now().timestamp()
        synchronized = self.is_synchronized()
        while not synchronized and (start_time + timeout_in_seconds > datetime.now().timestamp()):
            await asyncio.sleep(1)
            synchronized = self.is_synchronized()
        if not synchronized:
            raise TimeoutException('Timed out waiting for MetaApi to synchronize to MetaTrader account ' +
                                   self._account.id)

        while True:
            try:
                await self._websocketClient.wait_synchronized(self._account.id, None, 'RPC', 5, 'RPC')
                break
            except Exception as err:
                if datetime.now().timestamp() > start_time + timeout_in_seconds:
                    raise err

    async def on_reconnected(self, region: str, instance_number: int):
        """Invoked when connection to MetaTrader terminal re-established.

        Args:
            region: Reconnected region.
            instance_number: Reconnected instance number.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        instance_template = f'{region}:{instance_number}'
        for key in list(filter(lambda key: key.startswith(f'{instance_template}:'),
                               self._stateByInstanceIndex.keys())):
            del self._stateByInstanceIndex[key]

    def _get_state(self, instance_index: str) -> RpcMetaApiConnectionDict:
        if instance_index not in self._stateByInstanceIndex:
            self._stateByInstanceIndex[instance_index] = {
                'instanceIndex': instance_index,
                'synchronized': False,
            }
        return self._stateByInstanceIndex[instance_index]
