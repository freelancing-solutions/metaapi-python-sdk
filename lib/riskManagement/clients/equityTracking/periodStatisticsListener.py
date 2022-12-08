from .equityTracking_client_model import PeriodStatistics
from abc import abstractmethod
from typing import List


class PeriodStatisticsListener:
    """Period statistics event listener for handling a stream of period statistics events."""

    @abstractmethod
    async def on_period_statistics_updated(self, period_statistics_event: List[PeriodStatistics]):
        """Processes period statistics event which occurs when new period statistics data arrives.

        Args:
            period_statistics_event: Period statistics event.
        """
        pass

    @abstractmethod
    async def on_period_statistics_completed(self):
        """Processes period statistics event which occurs when a statistics period ends."""
        pass

    @abstractmethod
    async def on_tracker_completed(self):
        """Processes period statistics event which occurs when the tracker period ends."""
        pass

    @abstractmethod
    async def on_connected(self):
        """Processes an event which occurs when connection has been established."""
        pass

    @abstractmethod
    async def on_disconnected(self):
        """Processes an event which occurs when connection has been lost."""
        pass

    async def on_error(self, error: Exception):
        """Processes an error event.

        Args:
            error: Error received.
        """
        pass
