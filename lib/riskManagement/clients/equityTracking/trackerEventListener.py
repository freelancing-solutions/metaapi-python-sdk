from abc import abstractmethod


class TrackerEventListener:
    """Tracker event listener for handling a stream of profit/drawdown events."""

    @abstractmethod
    async def on_tracker_event(self, tracker_event):
        """Processes profit/drawdown event which occurs when a profit/drawdown limit is exceeded in a tracker.

        Args:
            tracker_event: Profit/drawdown event.
        """
        pass

    async def on_error(self, error: Exception):
        """Processes an error event.

        Args:
            error: Error received.
        """
        pass
