import asyncio
from random import uniform
from ..errorHandler import TooManyRequestsException
from ...metaApi.models import date
from datetime import datetime


class SubscriptionManager:
    """Subscription manager to handle account subscription logic."""

    def __init__(self, websocket_client):
        """Inits the subscription manager.

        Args:
            websocket_client: Websocket client to use for sending requests.
        """
        self._websocketClient = websocket_client
        self._subscriptions = {}

    async def subscribe(self, account_id: str, instance_index: int = None):
        """Schedules to send subscribe requests to an account until cancelled.

        Args:
            account_id: Id of the MetaTrader account.
            instance_index: Instance index.
        """

        instance_id = account_id + ':' + str(instance_index or 0)
        if instance_id not in self._subscriptions:
            self._subscriptions[instance_id] = {
                'shouldRetry': True,
                'task': None,
                'wait_task': None,
                'future': None
            }
            subscribe_retry_interval_in_seconds = 3
            while self._subscriptions[instance_id]['shouldRetry']:
                async def subscribe_task():
                    try:
                        await self._websocketClient.subscribe(account_id, instance_index)
                    except TooManyRequestsException as err:
                        nonlocal subscribe_retry_interval_in_seconds
                        retry_time = date(err.metadata['recommendedRetryTime']).timestamp()
                        if datetime.now().timestamp() + subscribe_retry_interval_in_seconds < retry_time:
                            await asyncio.sleep(retry_time - datetime.now().timestamp() -
                                                subscribe_retry_interval_in_seconds)
                    except Exception as err:
                        pass

                self._subscriptions[instance_id]['task'] = asyncio.create_task(subscribe_task())
                await asyncio.wait({self._subscriptions[instance_id]['task']})
                if not self._subscriptions[instance_id]['shouldRetry']:
                    break
                retry_interval = subscribe_retry_interval_in_seconds
                subscribe_retry_interval_in_seconds = min(subscribe_retry_interval_in_seconds * 2, 300)
                subscribe_future = asyncio.Future()

                async def subscribe_task():
                    await asyncio.sleep(retry_interval)
                    subscribe_future.set_result(True)

                self._subscriptions[instance_id]['wait_task'] = asyncio.create_task(subscribe_task())
                self._subscriptions[instance_id]['future'] = subscribe_future
                result = await self._subscriptions[instance_id]['future']
                self._subscriptions[instance_id]['future'] = None
                if not result:
                    break
            del self._subscriptions[instance_id]

    def cancel_subscribe(self, instance_id: str):
        """Cancels active subscription tasks for an instance id.

        Args:
            instance_id: Instance id to cancel subscription task for.
        """
        if instance_id in self._subscriptions:
            subscription = self._subscriptions[instance_id]
            if subscription['future'] and not subscription['future'].done():
                subscription['future'].set_result(False)
                subscription['wait_task'].cancel()
            if subscription['task']:
                subscription['task'].cancel()
            subscription['shouldRetry'] = False

    def cancel_account(self, account_id):
        """Cancels active subscription tasks for an account.

        Args:
            account_id: Account id to cancel subscription tasks for.
        """
        for instance_id in list(filter(lambda key: key.startswith(account_id), self._subscriptions.keys())):
            self.cancel_subscribe(instance_id)

    def on_timeout(self, account_id: str, instance_index: int = None):
        """Invoked on account timeout.

        Args:
            account_id: Id of the MetaTrader account.
            instance_index: Instance index.
        """
        if self._websocketClient.connected:
            asyncio.create_task(self.subscribe(account_id, instance_index))

    async def on_disconnected(self, account_id: str, instance_index: int = None):
        """Invoked when connection to MetaTrader terminal terminated.

        Args:
            account_id: Id of the MetaTrader account.
            instance_index: Instance index.
        """
        await asyncio.sleep(uniform(1, 5))
        asyncio.create_task(self.subscribe(account_id, instance_index))

    def on_reconnected(self):
        """Invoked when connection to MetaApi websocket API restored after a disconnect."""
        for instance_id in self._subscriptions.keys():
            self.cancel_subscribe(instance_id)
