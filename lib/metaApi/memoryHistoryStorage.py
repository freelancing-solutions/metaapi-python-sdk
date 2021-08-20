from .models import MetatraderDeal, MetatraderOrder
from typing import List
from .memoryHistoryStorageModel import MemoryHistoryStorageModel
from .historyFileManager import HistoryFileManager
from datetime import datetime
from .models import date
import pytz


class MemoryHistoryStorage(MemoryHistoryStorageModel):
    """History storage which stores MetaTrader history in RAM."""

    def __init__(self, account_id: str, application: str = 'MetaApi'):
        """Inits the in-memory history store instance"""
        super().__init__()
        self._accountId = account_id
        self._fileManager = HistoryFileManager(account_id, application, self)
        self._deals = []
        self._historyOrders = []
        self._lastDealTimeByInstanceIndex = {}
        self._lastHistoryOrderTimeByInstanceIndex = {}
        self._fileManager.start_update_job()

    async def initialize(self):
        """Initializes the storage and loads required data from a persistent storage."""
        await self.load_data_from_disk()

    @property
    def deals(self) -> List[MetatraderDeal]:
        """Returns all deals stored in history storage.

        Returns:
            All deals stored in history storage.
        """
        return self._deals

    @property
    def history_orders(self) -> List[MetatraderOrder]:
        """Returns all history orders stored in history storage.

        Returns:
            All history orders stored in history storage.
        """
        return self._historyOrders

    @property
    def last_deal_time_by_instance_index(self) -> dict:
        """Returns times of last deals by instance indices.

        Returns:
            Dictionary of last deal times by instance indices."""
        return self._lastDealTimeByInstanceIndex

    @property
    def last_history_order_time_by_instance_index(self) -> dict:
        """Returns times of last history orders by instance indices.

        Returns:
            Dictionary of last history orders times by instance indices."""
        return self._lastHistoryOrderTimeByInstanceIndex

    async def clear(self):
        """Clears the storage and deletes persistent data."""
        self._deals = []
        self._historyOrders = []
        self._lastDealTimeByInstanceIndex = {}
        self._lastHistoryOrderTimeByInstanceIndex = {}
        await self._fileManager.delete_storage_from_disk()

    async def load_data_from_disk(self):
        """Loads history data from the file manager.

        Returns:
            A coroutine which resolves when the history is loaded.
        """
        history = await self._fileManager.get_history_from_disk()
        self._deals = history['deals']
        self._historyOrders = history['historyOrders']
        self._lastDealTimeByInstanceIndex = history['lastDealTimeByInstanceIndex'] if 'lastDealTimeByInstanceIndex' \
                                                                                      in history else {}
        self._lastHistoryOrderTimeByInstanceIndex = history['lastHistoryOrderTimeByInstanceIndex'] if \
            'lastHistoryOrderTimeByInstanceIndex' in history else {}

    async def update_disk_storage(self):
        """Saves unsaved history items to disk storage.

        Returns:
            A coroutine which resolves when the history is saved.
        """
        await self._fileManager.update_disk_storage()

    async def last_history_order_time(self, instance_number: int = None) -> datetime:
        """Returns the time of the last history order record stored in the history storage.

        Args:
            instance_number: Index of an account instance connected.

        Returns:
            The time of the last history order record stored in the history storage.
        """
        if instance_number is not None:
            return date(self._lastHistoryOrderTimeByInstanceIndex[str(instance_number)] if str(instance_number) in
                        self._lastHistoryOrderTimeByInstanceIndex else 0)
        else:
            return date(max(list(self._lastHistoryOrderTimeByInstanceIndex.values())))

    async def last_deal_time(self, instance_number: int = None) -> datetime:
        """Returns the time of the last history deal record stored in the history storage.

        Args:
            instance_number: Index of an account instance connected.

        Returns:
            The time of the last history deal record stored in the history storage.
        """
        if instance_number is not None:
            return date(self._lastDealTimeByInstanceIndex[str(instance_number)] if str(instance_number) in
                        self._lastDealTimeByInstanceIndex else 0)
        else:
            return date(max(list(self._lastDealTimeByInstanceIndex.values())))

    async def on_history_order_added(self, instance_index: str, history_order: MetatraderOrder):
        """Invoked when a new MetaTrader history order is added.

        Args:
            instance_index: Index of an account instance connected.
            history_order: New MetaTrader history order.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        instance = str(self.get_instance_number(instance_index))
        insert_index = 0
        replacement_index = -1

        def get_done_time(order):
            if 'doneTime' in order:
                return order['doneTime'].timestamp() if (isinstance(order['doneTime'], datetime)) \
                    else date(order['doneTime']).timestamp()
            else:
                return 0

        new_history_order_time = get_done_time(history_order)
        if instance not in self._lastHistoryOrderTimeByInstanceIndex or \
                self._lastHistoryOrderTimeByInstanceIndex[instance] < new_history_order_time:
            self._lastHistoryOrderTimeByInstanceIndex[instance] = new_history_order_time

        for i in range(len(self._historyOrders)):
            index = len(self._historyOrders) - 1 - i
            order = self._historyOrders[index]
            order_time = get_done_time(order)
            if (order_time < new_history_order_time) or \
               (order_time == new_history_order_time and order['id'] <= history_order['id']):
                if (order_time == new_history_order_time and order['id'] == history_order['id'] and
                   order['type'] == history_order['type']):
                    replacement_index = index
                else:
                    insert_index = index + 1
                break
        if replacement_index != -1:
            self._historyOrders[replacement_index] = history_order
            self._fileManager.set_start_new_order_index(replacement_index)
        else:
            self._historyOrders.insert(insert_index, history_order)
            self._fileManager.set_start_new_order_index(insert_index)

    async def on_deal_added(self, instance_index: str, new_deal: MetatraderDeal):
        """Invoked when a new MetaTrader history deal is added.

        Args:
            instance_index: Index of an account instance connected.
            new_deal: New MetaTrader history deal.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        instance = str(self.get_instance_number(instance_index))
        insert_index = 0
        replacement_index = -1

        def get_time(deal):
            if 'time' in deal:
                return deal['time'].timestamp() if (isinstance(deal['time'], datetime)) \
                    else date(deal['time']).timestamp()
            else:
                return 0

        new_deal_time = get_time(new_deal)
        if instance not in self._lastDealTimeByInstanceIndex or \
                self._lastDealTimeByInstanceIndex[instance] < new_deal_time:
            self._lastDealTimeByInstanceIndex[instance] = new_deal_time
        for i in range(len(self._deals)):
            index = len(self._deals) - 1 - i
            deal = self._deals[index]
            deal_time = get_time(deal)
            if (deal_time < new_deal_time) or \
                    (deal_time == new_deal_time and deal['id'] <= new_deal['id']) or \
                    (deal_time == new_deal_time and deal['id'] == new_deal['id'] and
                     deal['entryType'] <= new_deal['entryType']):
                if (deal_time == new_deal_time and deal['id'] == new_deal['id'] and
                        deal['type'] == new_deal['type']):
                    replacement_index = index
                else:
                    insert_index = index + 1
                break
        if replacement_index != -1:
            self._deals[replacement_index] = new_deal
            self._fileManager.set_start_new_deal_index(replacement_index)
        else:
            self._deals.insert(insert_index, new_deal)
            self._fileManager.set_start_new_deal_index(insert_index)

    async def on_deals_synchronized(self, instance_index: str, synchronization_id: str):
        """Invoked when a synchronization of history deals on a MetaTrader account have finished to indicate progress
        of an initial terminal state synchronization.

        Args:
            instance_index: Index of an account instance connected.
            synchronization_id: Synchronization request id.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        instance = str(self.get_instance_number(instance_index))
        self._dealSynchronizationFinished[instance] = True
        await self.update_disk_storage()
