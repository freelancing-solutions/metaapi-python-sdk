from abc import abstractmethod
from typing import TypedDict


class EquityBalanceData(TypedDict, total=False):
    """Equity balance data for account."""
    equity: float
    """Account equity."""
    balance: float
    """Account balance."""


class EquityBalanceListener:
    """Equity balance event listener for handling a stream of equity and balance updates."""

    @abstractmethod
    async def on_equity_or_balance_updated(self, equity_balance_data: EquityBalanceData):
        """Processes an update event when equity or balance changes.

        Args:
            equity_balance_data: Equity and balance updated data.
        """
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
