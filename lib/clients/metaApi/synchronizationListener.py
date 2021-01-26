from ...metaApi.models import MetatraderPosition, MetatraderAccountInformation, MetatraderOrder, \
    MetatraderDeal, MetatraderSymbolSpecification, MetatraderSymbolPrice
from abc import ABC
from typing import List, Optional
from typing_extensions import TypedDict


class HealthStatus(TypedDict):
    """Server-side application health status."""
    restApiHealthy: Optional[bool]
    """Flag indicating that REST API is healthy."""
    copyFactorySubscriberHealthy: Optional[bool]
    """Flag indicating that CopyFactory subscriber is healthy."""
    copyFactoryProviderHealthy: Optional[bool]
    """Flag indicating that CopyFactory provider is healthy."""


class SynchronizationListener(ABC):
    """Defines interface for a synchronization listener class."""

    async def on_connected(self, instance_index: int, replicas: int):
        """Invoked when connection to MetaTrader terminal established.

        Args:
            instance_index: Index of an account instance connected.
            replicas: Number of account replicas launched.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_health_status(self, instance_index: int, status: HealthStatus):
        """Invoked when a server-side application health status is received from MetaApi.

        Args:
            instance_index: Index of an account instance connected.
            status: Server-side application health status.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_disconnected(self, instance_index: int):
        """Invoked when connection to MetaTrader terminal terminated.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
             A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_broker_connection_status_changed(self, instance_index: int, connected: bool):
        """Invoked when broker connection status have changed.

        Args:
            instance_index: Index of an account instance connected.
            connected: Is MetaTrader terminal is connected to broker.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_synchronization_started(self, instance_index: int):
        """Invoked when MetaTrader terminal state synchronization is started.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_account_information_updated(self, instance_index: int,
                                             account_information: MetatraderAccountInformation):
        """Invoked when MetaTrader position is updated.

        Args:
            instance_index: Index of an account instance connected.
            account_information: Updated MetaTrader position.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_positions_replaced(self, instance_index: int, positions: List[MetatraderPosition]):
        """Invoked when the positions are replaced as a result of initial terminal state synchronization.

        Args:
            instance_index: Index of an account instance connected.
            positions: Updated array of positions.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_position_updated(self, instance_index: int, position: MetatraderPosition):
        """Invoked when MetaTrader position is updated.

        Args:
            instance_index: Index of an account instance connected.
            position: Updated MetaTrader position.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_position_removed(self, instance_index: int, position_id: str):
        """Invoked when MetaTrader position is removed.

        Args:
            instance_index: Index of an account instance connected.
            position_id: Removed MetaTrader position id.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_orders_replaced(self, instance_index: int, orders: List[MetatraderOrder]):
        """Invoked when the orders are replaced as a result of initial terminal state synchronization.

        Args:
            instance_index: Index of an account instance connected.
            orders: Updated array of orders.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_order_updated(self, instance_index: int, order: MetatraderOrder):
        """Invoked when MetaTrader order is updated.

        Args:
            instance_index: Index of an account instance connected.
            order: Updated MetaTrader order.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_order_completed(self, instance_index: int, order_id: str):
        """Invoked when MetaTrader order is completed (executed or canceled).

        Args:
            instance_index: Index of an account instance connected.
            order_id: Completed MetaTrader order id.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_history_order_added(self, instance_index: int, history_order: MetatraderOrder):
        """Invoked when a new MetaTrader history order is added.

        Args:
            instance_index: Index of an account instance connected.
            history_order: New MetaTrader history order.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

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
        pass

    async def on_order_synchronization_finished(self, instance_index: int, synchronization_id: str):
        """Invoked when a synchronization of history orders on a MetaTrader account have finished.

        Args:
            instance_index: Index of an account instance connected.
            synchronization_id: Synchronization request id.

        Returns:
             A coroutine which resolves when the asynchronous event is processed
        """
        pass

    async def on_symbol_specification_updated(self, instance_index: int, specification: MetatraderSymbolSpecification):
        """Invoked when a symbol specification was updated

        Args:
            instance_index: Index of an account instance connected.
            specification: Updated MetaTrader symbol specification.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_symbol_price_updated(self, instance_index: int, price: MetatraderSymbolPrice):
        """Invoked when a symbol price was updated.

        Args:
            instance_index: Index of an account instance connected.
            price: Updated MetaTrader symbol price.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass

    async def on_symbol_prices_updated(self, instance_index: int, prices: List[MetatraderSymbolPrice],
                                       equity: float = None, margin: float = None, free_margin: float = None,
                                       margin_level: float = None):
        """Invoked when prices for several symbols were updated.

        Args:
            instance_index: Index of an account instance connected.
            prices: Updated MetaTrader symbol prices.
            equity: Account liquidation value.
            margin: Margin used.
            free_margin: Free margin.
            margin_level: Margin level calculated as % of equity/margin.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        pass
