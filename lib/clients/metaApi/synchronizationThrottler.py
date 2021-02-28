from typing import Dict
from collections import deque
from datetime import datetime
import asyncio


class SynchronizationThrottler:
    """Synchronization throttler used to limit the amount of concurrent synchronizations to prevent application
    from being overloaded due to excessive number of synchronisation responses being sent."""

    def __init__(self, client, max_concurrent_synchronizations: int = 10):
        """Inits the synchronization throttler.

        Args:
            max_concurrent_synchronizations: Limit of concurrent synchronizations.
        """
        self._maxConcurrentSynchronizations = max_concurrent_synchronizations
        self._client = client
        self._synchronizationIds = {}
        self._accountsBySynchronizationIds = {}
        self._synchronizationQueue = deque([])
        self._removeOldSyncIdsInterval = None
        self._processQueueInterval = None

    def start(self):
        """Initializes the synchronization throttler."""
        async def remove_old_sync_ids_interval():
            while True:
                await self._remove_old_sync_ids_job()
                await asyncio.sleep(1)

        async def process_queue_interval():
            while True:
                await self._process_queue_job()
                await asyncio.sleep(1)

        if not self._removeOldSyncIdsInterval:
            self._removeOldSyncIdsInterval = asyncio.create_task(remove_old_sync_ids_interval())
            self._processQueueInterval = asyncio.create_task(process_queue_interval())

    def stop(self):
        """Deinitializes the throttler."""
        self._removeOldSyncIdsInterval.cancel()
        self._removeOldSyncIdsInterval = None
        self._processQueueInterval.cancel()
        self._processQueueInterval = None

    async def _remove_old_sync_ids_job(self):
        now = datetime.now().timestamp()
        for key in list(self._synchronizationIds.keys()):
            if (now - self._synchronizationIds[key]) > 10:
                del self._synchronizationIds[key]
                self._advance_queue()
        while len(self._synchronizationQueue) and \
                (datetime.now().timestamp() - self._synchronizationQueue[0]['queueTime']) > 300:
            self._remove_from_queue(self._synchronizationQueue[0]['synchronizationId'])
        await asyncio.sleep(1)

    def update_synchronization_id(self, synchronization_id: str):
        """Fills a synchronization slot with synchronization id.

        Args:
            synchronization_id: Synchronization id.
        """
        if synchronization_id in self._accountsBySynchronizationIds:
            self._synchronizationIds[synchronization_id] = datetime.now().timestamp()

    @property
    def is_synchronization_available(self) -> bool:
        """Whether there are free slots for synchronization requests."""
        synchronizing_accounts = []
        for key in self._synchronizationIds:
            account_data = self._accountsBySynchronizationIds[key] if key in \
                                                                      self._accountsBySynchronizationIds else None
            if account_data and (account_data['accountId'] not in synchronizing_accounts):
                synchronizing_accounts.append(account_data['accountId'])
        return len(synchronizing_accounts) < self._maxConcurrentSynchronizations

    def remove_synchronization_id(self, synchronization_id: str):
        """Removes synchronization id from slots and removes ids for the same account from the queue.

        Args:
            synchronization_id: Synchronization id.
        """
        if synchronization_id in self._accountsBySynchronizationIds:
            account_id = self._accountsBySynchronizationIds[synchronization_id]['accountId']
            instance_index = self._accountsBySynchronizationIds[synchronization_id]['instanceIndex']
            for key in list(self._accountsBySynchronizationIds.keys()):
                if self._accountsBySynchronizationIds[key]['accountId'] == account_id and \
                        self._accountsBySynchronizationIds[key]['instanceIndex'] == instance_index:
                    self._remove_from_queue(key)
                    del self._accountsBySynchronizationIds[key]
        if synchronization_id in self._synchronizationIds:
            del self._synchronizationIds[synchronization_id]
        self._advance_queue()

    def on_disconnect(self):
        """Clears synchronization ids on disconnect."""
        self._synchronizationIds = {}
        self._advance_queue()

    def _advance_queue(self):
        if self.is_synchronization_available and len(self._synchronizationQueue):
            if not self._synchronizationQueue[0]['promise'].done():
                self._synchronizationQueue[0]['promise'].set_result(True)

    def _remove_from_queue(self, synchronization_id: str):
        for i in range(len(self._synchronizationQueue)):
            if self._synchronizationQueue[i]['synchronizationId'] == synchronization_id and \
              not self._synchronizationQueue[i]['promise'].done():
                self._synchronizationQueue[i]['promise'].set_result(False)
        self._synchronizationQueue = deque(filter(lambda item: item['synchronizationId'] != synchronization_id,
                                                  self._synchronizationQueue))

    async def _process_queue_job(self):
        while len(self._synchronizationQueue) and (len(self._synchronizationIds.values()) <
                                                   self._maxConcurrentSynchronizations):
            await self._synchronizationQueue[0]['promise']
            self._synchronizationQueue.popleft()

    async def schedule_synchronize(self, account_id: str, request: Dict):
        """Schedules to send a synchronization request for account.

        Args:
            account_id: Account id.
            request: Request to send.
        """
        synchronization_id = request['requestId']
        for key in list(self._accountsBySynchronizationIds.keys()):
            if self._accountsBySynchronizationIds[key]['accountId'] == account_id and \
                     self._accountsBySynchronizationIds[key]['instanceIndex'] == \
                    (request['instanceIndex'] if 'instanceIndex' in request else None):
                self.remove_synchronization_id(key)
        self._accountsBySynchronizationIds[synchronization_id] = {
            'accountId': account_id, 'instanceIndex': request['instanceIndex'] if 'instanceIndex' in request else None
        }
        if not self.is_synchronization_available:
            request_resolve = asyncio.Future()
            self._synchronizationQueue.append({
                'synchronizationId': synchronization_id,
                'promise': request_resolve,
                'queueTime': datetime.now().timestamp()
            })
            result = await request_resolve
            if not result:
                return None
        self.update_synchronization_id(synchronization_id)
        return await self._client._rpc_request(account_id, request)
