from ..domain_client import DomainClient
from ...models import random_id
from .trackerEventListener import TrackerEventListener
import asyncio


class TrackerEventListenerManager:
    """Manager for handling tracking event listeners."""

    def __init__(self, domain_client: DomainClient):
        """Inits tracker event listener manager instance.

        Args:
            domain_client: Domain client.
        """
        self._domainClient = domain_client
        self._trackerEventListeners = {}
        self._errorThrottleTime = 1

    @property
    def tracker_event_listeners(self):
        """Returns the dictionary of tracker event listeners.

        Returns:
            Dictionary of tracker event listeners.
        """
        return self._trackerEventListeners

    def add_tracker_event_listener(self, listener: TrackerEventListener, account_id: str = None, tracker_id: str = None,
                                   sequence_number: int = None) -> str:
        """Adds a tracker event listener.

        Args:
            listener: Tracker event listener.
            account_id: Account id.
            tracker_id: Tracker id.
            sequence_number: Event sequence number.

        Returns:
            Tracker event listener id.
        """
        listener_id = random_id(10)
        self._trackerEventListeners[listener_id] = listener
        asyncio.create_task(self._start_tracker_event_job(listener_id, listener, account_id, tracker_id,
                                                          sequence_number))
        return listener_id

    def remove_tracker_event_listener(self, listener_id: str):
        """Removes tracker event listener by id.

        Args:
            listener_id: Listener id.
        """
        if listener_id in self._trackerEventListeners:
            del self._trackerEventListeners[listener_id]

    async def _start_tracker_event_job(self, listener_id: str, listener: TrackerEventListener, account_id: str = None,
                                       tracker_id: str = None, sequence_number: int = None):
        throttle_time = self._errorThrottleTime
        while listener_id in self._trackerEventListeners:
            opts = {
                'url': '/users/current/tracker-events/stream',
                'method': 'GET',
                'qs': {
                    'previousSequenceNumber': sequence_number,
                    'accountId': account_id,
                    'trackerId': tracker_id,
                    'limit': 1000
                },
            }
            try:
                packets = await self._domainClient.request_api(opts, True)
                for packet in packets:
                    await listener.on_tracker_event(packet)
                throttle_time = self._errorThrottleTime
                if listener_id in self._trackerEventListeners and len(packets):
                    sequence_number = packets[-1]['sequenceNumber']
            except Exception as err:
                asyncio.create_task(listener.on_error(err))
                await asyncio.sleep(throttle_time)
                throttle_time = min(throttle_time * 2, 30)
