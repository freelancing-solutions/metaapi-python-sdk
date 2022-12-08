from .metaApiConnectionInstance import MetaApiConnectionInstance
from ..clients.metaApi.metaApiWebsocket_client import MetaApiWebsocketClient
from .terminalState import TerminalState
from .connectionHealthMonitor import ConnectionHealthMonitor
from .historyStorage import HistoryStorage
from .models import MarketDataSubscription, MarketDataUnsubscription
from typing import Coroutine, List, Optional, Dict, Callable
from typing_extensions import TypedDict
from .streamingMetaApiConnection import StreamingMetaApiConnection
from ..logger import LoggerManager
from .metatraderAccountModel import MetatraderAccountModel


class SynchronizationOptions(TypedDict, total=False):
    instanceIndex: Optional[int]
    """Index of an account instance to ensure synchronization on, default is to wait for the first instance to
    synchronize."""
    applicationPattern: Optional[str]
    """Application regular expression pattern, default is .*"""
    synchronizationId: Optional[str]
    """synchronization id, last synchronization request id will be used by default"""
    timeoutInSeconds: Optional[float]
    """Wait timeout in seconds, default is 5m."""
    intervalInMilliseconds: Optional[float]
    """Interval between account reloads while waiting for a change, default is 1s."""


class StreamingMetaApiConnectionInstance(MetaApiConnectionInstance):
    """Exposes MetaApi MetaTrader streaming API connection to consumers."""

    def __init__(self, websocket_client: MetaApiWebsocketClient, meta_api_connection: StreamingMetaApiConnection):
        """Inits MetaApi MetaTrader streaming Api connection instance.

        Args:
            websocket_client: MetaApi websocket client.
            meta_api_connection: Streaming MetaApi connection.
        """
        super().__init__(websocket_client, meta_api_connection)
        self._metaApiConnection = meta_api_connection
        self._synchronizationListeners = []
        self._logger = LoggerManager.get_logger('StreamingMetaApiConnectionInstance')

    async def connect(self):
        """Opens the connection. Can only be called the first time, next calls will be ignored.

        Returns:
            A coroutine resolving when the connection is opened
        """
        if not self._opened:
            self._opened = True
            try:
                await self._metaApiConnection.connect(self.instance_id)
            except Exception as err:
                await self.close()
                raise err

    def remove_application(self):
        """Clears the order and transaction history of a specified application and removes application (see
        https://metaapi.cloud/docs/client/websocket/api/removeApplication/).

        Returns:
            A coroutine resolving when the history is cleared and application is removed.
        """
        return self._metaApiConnection.remove_application()

    async def subscribe_to_market_data(self, symbol: str, subscriptions: List[MarketDataSubscription] = None,
                                       timeout_in_seconds: float = None, wait_for_quote: bool = True) -> Coroutine:
        """Subscribes on market data of specified symbol (see
        https://metaapi.cloud/docs/client/websocket/marketDataStreaming/subscribeToMarketData/).

        Args:
            symbol: Symbol (e.g. currency pair or an index).
            subscriptions: Array of market data subscription to create or update. Please note that this feature is
            not fully implemented on server-side yet.
            timeout_in_seconds: Timeout to wait for prices in seconds, default is 30.
            wait_for_quote: If set to false, the method will resolve without waiting for the first quote to
            arrive. Default is to wait for quote if quotes subscription is requested.

        Returns:
            Promise which resolves when subscription request was processed.
        """
        self._check_is_connection_active()
        return await self._metaApiConnection.subscribe_to_market_data(symbol, subscriptions, timeout_in_seconds,
                                                                      wait_for_quote)

    async def unsubscribe_from_market_data(self, symbol: str, subscriptions: List[MarketDataUnsubscription] = None) \
            -> Coroutine:
        """Unsubscribes from market data of specified symbol (see
        https://metaapi.cloud/docs/client/websocket/marketDataStreaming/subscribeToMarketData/).

        Args:
            symbol: Symbol (e.g. currency pair or an index).
            subscriptions: Array of subscriptions to cancel.

        Returns:
            Promise which resolves when subscription request was processed.
        """
        self._check_is_connection_active()
        return await self._metaApiConnection.unsubscribe_from_market_data(symbol, subscriptions)

    @property
    def subscribed_symbols(self) -> List[str]:
        """Returns list of the symbols connection is subscribed to.

        Returns:
            List of the symbols connection is subscribed to.
        """
        return self._metaApiConnection.subscribed_symbols

    def subscriptions(self, symbol) -> List[MarketDataSubscription]:
        """Returns subscriptions for a symbol.

        Args:
            symbol: Symbol to retrieve subscriptions for.

        Returns:
            List of market data subscriptions for the symbol.
        """
        return self._metaApiConnection.subscriptions(symbol)

    def save_uptime(self, uptime: Dict):
        """Sends client uptime stats to the server.

        Args:
            uptime: Uptime statistics to send to the server.

        Returns:
            A coroutine which resolves when uptime statistics is submitted.
        """
        self._check_is_connection_active()
        return self._websocketClient.save_uptime(self._metaApiConnection.account.id, uptime)

    @property
    def terminal_state(self) -> TerminalState:
        """Returns local copy of terminal state.

        Returns:
            Local copy of terminal state.
        """
        return self._metaApiConnection.terminal_state

    @property
    def history_storage(self) -> HistoryStorage:
        """Returns local history storage.

        Returns:
            Local history storage.
        """
        return self._metaApiConnection.history_storage

    def add_synchronization_listener(self, listener):
        """Adds synchronization listener.

        Args:
            listener: Synchronization listener to add.
        """
        self._synchronizationListeners.append(listener)
        self._websocketClient.add_synchronization_listener(self._metaApiConnection.account.id, listener)

    def remove_synchronization_listener(self, listener):
        """Removes synchronization listener for specific account.

        Args:
            listener: Synchronization listener to remove.
        """
        self._synchronizationListeners = list(filter(lambda lis: lis != listener, self._synchronizationListeners))
        self._websocketClient.remove_synchronization_listener(self._metaApiConnection.account.id, listener)

    async def wait_synchronized(self, opts: SynchronizationOptions = None):
        """Waits until synchronization to MetaTrader terminal is completed.

        Args:
            opts: Synchronization options.

        Returns:
            A coroutine which resolves when synchronization to MetaTrader terminal is completed.

        Raises:
            TimeoutException: If application failed to synchronize with the terminal within timeout allowed.
        """
        self._check_is_connection_active()
        return await self._metaApiConnection.wait_synchronized(opts)

    def queue_event(self, name: str, callable: Callable):
        """Queues an event for processing among other synchronization events within same account.

        Args:
            name: Event label name.
            callable: Async or regular function to execute.
        """
        self._websocketClient.queue_event(self._metaApiConnection.account.id, name, callable)

    async def close(self):
        """Closes the connection. The instance of the class should no longer be used after this method is invoked."""
        if not self._closed:
            for listener in self._synchronizationListeners:
                self._websocketClient.remove_synchronization_listener(self._metaApiConnection.account.id, listener)
            self._closed = True
            await self._metaApiConnection.close(self.instance_id)

    @property
    def synchronized(self) -> bool:
        """Returns synchronization status.

        Returns:
            Synchronization status.
        """
        return self._metaApiConnection.synchronized

    @property
    def account(self) -> MetatraderAccountModel:
        """Returns synchronization status.

        Returns:
            Synchronization status.
        """
        return self._metaApiConnection.account

    @property
    def health_monitor(self) -> ConnectionHealthMonitor:
        """Returns connection health monitor instance.

        Returns:
            Connection health monitor instance.
        """
        return self._metaApiConnection.health_monitor
