from .historyStorage import HistoryStorage
from .models import MetatraderDeal, MetatraderOrder
from typing import List
from abc import abstractmethod


class MemoryHistoryStorageModel(HistoryStorage):
    """Abstract class which defines MetaTrader memory history storage interface."""

    @property
    @abstractmethod
    def deals(self) -> List[MetatraderDeal]:
        """Returns all deals stored in history storage.

        Returns:
            All deals stored in history storage.
        """
        pass

    @property
    @abstractmethod
    def history_orders(self) -> List[MetatraderOrder]:
        """Returns all history orders stored in history storage.

        Returns:
            All history orders stored in history storage.
        """
        pass

    @property
    @abstractmethod
    def last_deal_time_by_instance_index(self) -> dict:
        """Returns times of last deals by instance indices.

        Returns:
            Dictionary of last deal times by instance indices."""
        pass

    @property
    @abstractmethod
    def last_history_order_time_by_instance_index(self) -> dict:
        """Returns times of last history orders by instance indices.

        Returns:
            Dictionary of last history orders times by instance indices."""
        pass
