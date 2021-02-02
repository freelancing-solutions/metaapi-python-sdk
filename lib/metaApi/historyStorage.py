from ..clients.metaApi.synchronizationListener import SynchronizationListener
from .models import MetatraderOrder, MetatraderDeal
from datetime import datetime
from abc import abstractmethod, ABC


class HistoryStorage(SynchronizationListener, ABC):
    """ Abstract class which defines MetaTrader history storage interface."""

    def __init__(self):
        """Inits the history storage"""
        super().__init__()
        self._orderSynchronizationFinished = {}
        self._dealSynchronizationFinished = {}

    async def initialize(self):
        """Initializes the storage and loads required data from a persistent storage."""
        pass

    @property
    def order_synchronization_finished(self) -> bool:
        """Returns flag indicating whether order history synchronization has finished.

        Returns:
            A flag indicating whether order history synchronization has finished.
        """
        return True in list(self._orderSynchronizationFinished.values())

    @property
    def deal_synchronization_finished(self) -> bool:
        """Returns flag indicating whether deal history synchronization has finished.

        Returns:
            A flag indicating whether order history synchronization has finished.
        """
        return True in list(self._dealSynchronizationFinished.values())

    @abstractmethod
    async def clear(self):
        """Clears the storage and deletes persistent data."""
        pass

    @abstractmethod
    async def last_history_order_time(self, instance_index: int = None) -> datetime:
        """Returns the time of the last history order record stored in the history storage.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
            The time of the last history order record stored in the history storage.
        """
        pass

    @abstractmethod
    async def last_deal_time(self, instance_index: int = None) -> datetime:
        """Returns the time of the last history deal record stored in the history storage.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
            The time of the last history deal record stored in the history storage.
        """
        pass

    @abstractmethod
    async def on_history_order_added(self, instance_index: int, history_order: MetatraderOrder):
        """Invoked when a new MetaTrader history order is added.

        Args:
            instance_index: Index of an account instance connected.
            history_order: New MetaTrader history order.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    @abstractmethod
    async def on_deal_added(self, instance_index: int, deal: MetatraderDeal):
        """Invoked when a new MetaTrader history deal is added.

        Args:
            instance_index: Index of an account instance connected.
            deal: New MetaTrader history deal.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_deal_synchronization_finished(self, instance_index: int, synchronization_id: str):
        """Invoked when a synchronization of history deals on a MetaTrader account have finished.

        Args:
            instance_index: Index of an account instance connected.
            synchronization_id: Synchronization request id.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        self._dealSynchronizationFinished[str(instance_index)] = True

    async def on_order_synchronization_finished(self, instance_index: int, synchronization_id: str):
        """Invoked when a synchronization of history orders on a MetaTrader account have finished.

        Args:
            instance_index: Index of an account instance connected.
            synchronization_id: Synchronization request id.

        Returns:
             A coroutine which resolves when the asynchronous event is processed.
        """
        self._orderSynchronizationFinished[str(instance_index)] = True

    async def on_connected(self, instance_index: int, replicas: int):
        """Invoked when connection to MetaTrader terminal established.

        Args:
            instance_index: Index of an account instance connected.
            replicas: Number of account replicas launched.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        self._orderSynchronizationFinished[str(instance_index)] = False
        self._dealSynchronizationFinished[str(instance_index)] = False
