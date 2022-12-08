from ..clients.metaApi.synchronizationListener import SynchronizationListener
from ..clients.metaApi.reconnectListener import ReconnectListener
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from .metatraderAccountModel import MetatraderAccountModel
from .models import string_format_error
from typing import Coroutine
from abc import abstractmethod
from ..logger import LoggerManager
from typing import Optional, Union
from typing_extensions import TypedDict
import asyncio


class MetaApiConnectionDict(TypedDict, total=False):
    instanceIndex: int
    ordersSynchronized: dict
    dealsSynchronized: dict
    shouldSynchronize: Optional[str]
    synchronizationRetryIntervalInSeconds: float
    synchronized: bool
    lastDisconnectedSynchronizationId: Optional[str]
    lastSynchronizationId: Optional[str]
    disconnected: bool
    synchronizationTimeout: Union[asyncio.Task, None]
    ensureSynchronizeTimeout: Union[asyncio.Task, None]


class MetaApiConnection(SynchronizationListener, ReconnectListener):
    """Exposes MetaApi MetaTrader API connection to consumers."""

    def __init__(self, websocket_client: MetaApiWebsocketClient, account: MetatraderAccountModel,
                 application: str = None):
        """Inits MetaApi MetaTrader Api connection.

        Args:
            websocket_client: MetaApi websocket client.
            account: MetaTrader account id to connect to.
            application: Application to use.
        """
        super().__init__()
        self._websocketClient = websocket_client
        self._account = account
        self._logger = LoggerManager.get_logger('MetaApiConnection')
        self._application = application
        self._stateByInstanceIndex = {}
        self._opened = False
        self._closed = False

    @abstractmethod
    async def connect(self, instance_id: str):
        """Opens the connection. Can only be called the first time, next calls will be ignored.

        Args:
            instance_id: Connection instance id.

        Returns:
            A coroutine resolving when the connection is opened
        """
        pass

    @abstractmethod
    async def close(self, instance_id: str):
        """Closes the connection. The instance of the class should no longer be used after this method is invoked.

        Args:
            instance_id: Connection instance id.
        """
        pass

    def on_reconnected(self, region: str, instance_number: int):
        """Invoked when connection to MetaApi websocket API restored after a disconnect.

        Args:
            region: Reconnected region.
            instance_number: Reconnected instance number.

        Returns:
            A coroutine which resolves when connection to MetaApi websocket API restored after a disconnect.
        """
        pass

    @property
    def account(self) -> MetatraderAccountModel:
        """Returns MetaApi account.

        Returns:
            MetaApi account.
        """
        return self._account

    @property
    def application(self) -> str:
        """Returns connection application.

        Returns:
            Connection application.
        """
        return self._application

    async def synchronize(self, instance_id: str):
        """Closes the connection. The instance of the class should no longer be used after this method is invoked.

        Args:
            instance_id: Connection instance id.
        """
        pass

    def _get_state(self, instance_index: str) -> MetaApiConnectionDict:
        if instance_index not in self._stateByInstanceIndex:
            self._stateByInstanceIndex[instance_index] = {
                'instanceIndex': instance_index,
                'ordersSynchronized': {},
                'dealsSynchronized': {},
                'shouldSynchronize': None,
                'synchronizationRetryIntervalInSeconds': 1,
                'synchronized': False,
                'lastDisconnectedSynchronizationId': None,
                'lastSynchronizationId': None,
                'disconnected': False,
                'synchronizationTimeout': None,
                'ensureSynchronizeTimeout': None
            }
        return self._stateByInstanceIndex[instance_index]

    def _check_is_connection_active(self):
        if not self._opened:
            raise Exception('This connection has not been initialized yet, please invoke await connection.connect()')

        if self._closed:
            raise Exception('This connection has been closed, please create a new connection')
