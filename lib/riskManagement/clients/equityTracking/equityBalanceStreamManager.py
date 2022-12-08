from ..domain_client import DomainClient
from ...models import random_id
from ....metaApi.models import MetatraderSymbolPrice, MetatraderAccountInformation
import asyncio
from asyncio import Future
from ....logger import LoggerManager
from .equityBalanceListener import EquityBalanceListener
from ....clients.metaApi.synchronizationListener import SynchronizationListener
from ....metaApi.streamingMetaApiConnectionInstance import StreamingMetaApiConnectionInstance
from .... import MetaApi
from typing import Dict, List


class EquityBalanceStreamManager:
    """Manager for handling equity balance event listeners."""

    def __init__(self, domain_client: DomainClient, meta_api: MetaApi):
        """Constructs equity balance event listener manager instance.

        Args:
            domain_client: Domain client.
            meta_api: MetaApi SDK instance.
        """
        self._domainClient = domain_client
        self._metaApi = meta_api
        self._equityBalanceListeners: Dict[str, Dict[str, EquityBalanceListener]] = {}
        self._accountsByListenerId: Dict[str, str] = {}
        self._equityBalanceConnections: Dict[str, StreamingMetaApiConnectionInstance] = {}
        self._equityBalanceCaches = {}
        self._accountSynchronizationFlags: Dict[str, bool] = {}
        self._pendingInitalizationResolves: Dict[str, List[Future]] = {}
        self._retryIntervalInSeconds = 1
        self._logger = LoggerManager.get_logger('EquityBalanceStreamManager')

    def get_account_listeners(self, account_id: str) -> Dict[str, EquityBalanceListener]:
        """Returns listeners for account.

        Args:
            account_id: Account id to return listeners for.

        Returns:
            Dictionary of account equity balance event listeners.
        """
        if account_id not in self._equityBalanceListeners:
            self._equityBalanceListeners[account_id] = {}
        return self._equityBalanceListeners[account_id]

    async def add_equity_balance_listener(self, listener: EquityBalanceListener, account_id: str) -> str:
        """Adds an equity balance event listener.

        Args:
            listener: Equity balance event listener.
            account_id: Account id.

        Returns:
            Listener id.
        """
        if account_id not in self._equityBalanceCaches:
            self._equityBalanceCaches[account_id] = {
                'balance': None,
                'equity': None,
                'pendingInitalizationResolves': []
            }
        cache = self._equityBalanceCaches[account_id]
        connection: StreamingMetaApiConnectionInstance = None
        retry_interval_in_seconds = self._retryIntervalInSeconds

        def get_account_listeners():
            return self.get_account_listeners(account_id)

        pending_initialization_resolves = self._pendingInitalizationResolves
        synchronization_flags = self._accountSynchronizationFlags

        async def process_equity_balance_event(equity=None, balance=None):
            if account_id in self._equityBalanceCaches:
                if equity != cache['equity'] or (balance and balance != cache['balance']):
                    cache['equity'] = equity
                    if balance:
                        cache['balance'] = balance
                    if cache['equity'] is not None and cache['balance'] is not None:
                        for account_listener in self.get_account_listeners(account_id).values():
                            await account_listener.on_equity_or_balance_updated({
                                'equity': cache['equity'],
                                'balance': cache['balance']
                            })

        class EquityBalanceStreamListener(SynchronizationListener):

            async def on_deals_synchronized(self, instance_index: str, synchronization_id: str):
                if account_id not in synchronization_flags or not synchronization_flags[account_id]:
                    synchronization_flags[account_id] = True
                    for account_listener in get_account_listeners().values():
                        asyncio.create_task(account_listener.on_connected())
                if account_id in pending_initialization_resolves:
                    for promise in pending_initialization_resolves[account_id]:
                        promise.set_result(True)
                        del pending_initialization_resolves[account_id]

            async def on_disconnected(self, instance_index: str):
                if account_id in synchronization_flags and not connection.health_monitor.health_status['synchronized']:
                    synchronization_flags[account_id] = False
                    for account_listener in get_account_listeners().values():
                        asyncio.create_task(account_listener.on_disconnected())

            async def on_symbol_price_updated(self, instance_index: str, price: MetatraderSymbolPrice):
                if account_id in pending_initialization_resolves:
                    for promise in pending_initialization_resolves[account_id]:
                        promise.set_result(True)
                        del pending_initialization_resolves[account_id]
                # price data only contains equity
                await process_equity_balance_event(price['equity'])

            async def on_account_information_updated(self, instance_index: str,
                                                     account_information: MetatraderAccountInformation):
                await process_equity_balance_event(account_information['equity'], account_information['balance'])

        listener_id = random_id(10)
        account_listeners = self.get_account_listeners(account_id)
        account_listeners[listener_id] = listener
        self._accountsByListenerId[listener_id] = account_id
        if account_id not in self._equityBalanceConnections:
            account = await self._metaApi.metatrader_account_api.get_account(account_id)
            is_deployed = False
            while not is_deployed:
                try:
                    await account.wait_deployed()
                    is_deployed = True
                except Exception as err:
                    asyncio.create_task(listener.on_error(err))
                    self._logger.error(f'Error wait for account {account_id} to deploy, retrying', err)
                    await asyncio.sleep(retry_interval_in_seconds)
                    retry_interval_in_seconds = min(retry_interval_in_seconds * 2, 300)

            retry_interval_in_seconds = self._retryIntervalInSeconds
            connection = account.get_streaming_connection()
            self._equityBalanceConnections[account_id] = connection
            sync_listener = EquityBalanceStreamListener()
            connection.add_synchronization_listener(sync_listener)

            is_synchronized = False
            while not is_synchronized:
                try:
                    await connection.connect()
                    await connection.wait_synchronized()
                    is_synchronized = True
                except Exception as err:
                    asyncio.create_task(listener.on_error(err))
                    self._logger.error('Error configuring equity balance stream listener ' +
                                       f'for account {account_id}, retrying', err)
                    await asyncio.sleep(retry_interval_in_seconds)
                    retry_interval_in_seconds = min(retry_interval_in_seconds * 2, 300)

            retry_interval_in_seconds = self._retryIntervalInSeconds
        else:
            connection = self._equityBalanceConnections[account_id]
            if not connection.health_monitor.health_status['synchronized']:
                if account_id not in self._pendingInitalizationResolves:
                    self._pendingInitalizationResolves[account_id] = []
                initialize_promise = Future()
                self._pendingInitalizationResolves[account_id].append(initialize_promise)
                await initialize_promise
        return listener_id

    def remove_equity_balance_listener(self, listener_id: str):
        """Removes equity balance event listener by id.

        Args:
            listener_id: Listener id.
        """
        if listener_id in self._accountsByListenerId:
            account_id = self._accountsByListenerId[listener_id]
            if account_id in self._accountSynchronizationFlags:
                del self._accountSynchronizationFlags[account_id]
            if listener_id in self._accountsByListenerId:
                del self._accountsByListenerId[listener_id]
            if account_id in self._equityBalanceListeners and listener_id in self._equityBalanceListeners[account_id]:
                del self._equityBalanceListeners[account_id][listener_id]
            if account_id in self._equityBalanceConnections and not \
                    len(self._equityBalanceListeners[account_id].keys()):
                asyncio.create_task(self._equityBalanceConnections[account_id].close())
                del self._equityBalanceConnections[account_id]
