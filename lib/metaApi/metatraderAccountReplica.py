from typing import List, Dict
from ..clients.metaApi.metatraderAccount_client import MetatraderAccountReplicaDto, \
    UpdatedMetatraderAccountReplicaDto, MetatraderAccountClient
from .metatraderAccountModel import MetatraderAccountModel
from datetime import datetime, timedelta
from ..clients.timeoutException import TimeoutException
import asyncio
from .metatraderAccountReplicaModel import MetatraderAccountReplicaModel


class MetatraderAccountReplica(MetatraderAccountReplicaModel):
    """Implements a MetaTrader account replica entity."""

    def __init__(self, data: MetatraderAccountReplicaDto, primary_account: MetatraderAccountModel,
                 metatrader_account_client: MetatraderAccountClient):
        """Constructs a MetaTrader account entity.

        Args:
            data: MetaTrader account replica data.
            primary_account: primary MetaTrader account.
            metatrader_account_client: MetaTrader account REST API client.
        """
        self._data = data
        self._primaryAccount = primary_account
        self._metatraderAccountClient = metatrader_account_client

    @property
    def id(self) -> str:
        """Returns account replica id.

        Returns:
            Unique account replica id.
        """
        return self._data['_id']

    @property
    def state(self) -> str:
        """Returns current account replica state. One of CREATED, DEPLOYING, DEPLOYED, DEPLOY_FAILED, UNDEPLOYING,
        UNDEPLOYED, UNDEPLOY_FAILED, DELETING, DELETE_FAILED, REDEPLOY_FAILED.

        Returns:
            Current account replica state.
        """
        return self._data['state']

    @property
    def magic(self) -> int:
        """Returns MetaTrader magic to place trades using.

        Returns:
            MetaTrader magic to place trades using.
        """
        return self._data['magic']

    @property
    def connection_status(self) -> str:
        """Returns terminal & broker connection status, one of CONNECTED, DISCONNECTED, DISCONNECTED_FROM_BROKER.

        Returns:
            Terminal & broker connection status.
        """
        return self._data['connectionStatus']

    @property
    def quote_streaming_interval_in_seconds(self) -> str:
        """Returns quote streaming interval in seconds.

        Returns:
            Quote streaming interval in seconds.
        """
        return self._data['quoteStreamingIntervalInSeconds']

    @property
    def symbol(self) -> str:
        """Returns symbol provided by broker.

        Returns:
            Any symbol provided by broker.
        """
        return self._data['symbol']

    @property
    def reliability(self) -> str:
        """Returns reliability value. Possible values are regular and high.

        Returns:
            Account replica reliability value.
        """
        return self._data['reliability']

    @property
    def tags(self) -> List[str]:
        """Returns user-defined account replica tags.

        Returns:
            User-defined account replica tags.
        """
        return self._data['tags'] if 'tags' in self._data else None

    @property
    def metadata(self) -> Dict:
        """Returns extra information which can be stored together with your account replica.

        Returns:
            Extra information which can be stored together with your account replica.
        """
        return self._data['metadata'] if 'metadata' in self._data else None

    @property
    def resource_slots(self) -> int:
        """Returns number of resource slots to allocate to account replica. Allocating extra resource slots
        results in better account performance under load which is useful for some applications. E.g. if you have many
        accounts copying the same strategy via CooyFactory API, then you can increase resourceSlots to get a lower
        trade copying latency. Please note that allocating extra resource slots is a paid option. Please note that high
        reliability accounts use redundant infrastructure, so that each resource slot for a high reliability account
        is billed as 2 standard resource slots.

        Returns:
            Number of resource slots to allocate to account replica.
        """
        return self._data['resourceSlots'] if 'resourceSlots' in self._data else None

    @property
    def copyfactory_resource_slots(self) -> int:
        """Returns the number of CopyFactory 2 resource slots to allocate to account replica. Allocating extra resource
        slots results in lower trade copying latency. Please note that allocating extra resource slots is a paid option.
        Please also note that CopyFactory 2 uses redundant infrastructure so that each CopyFactory resource slot is
        billed as 2 standard resource slots. You will be billed for CopyFactory 2 resource slots only if you have
        added your account replica to CopyFactory 2 by specifying copyFactoryRoles field.

        Returns:
            Number of CopyFactory 2 resource slots to allocate to account.
        """
        return self._data['copyFactoryResourceSlots'] if 'copyFactoryResourceSlots' in self._data else None

    @property
    def region(self) -> str:
        """Returns account region.

        Returns:
            Account region value.
        """
        return self._data['region']

    @property
    def primary_account_from_dto(self) -> dict:
        """Returns primary MetaTrader account of the replica from DTO.

        Returns:
            Primary MetaTrader account of the replica from DTO.
        """
        return self._data['primaryAccount']

    @property
    def primary_account(self) -> MetatraderAccountModel:
        """Returns primary MetaTrader account of the replica.

        Returns:
            Primary MetaTrader account of the replica.
        """
        return self._primaryAccount

    def update_data(self, data: MetatraderAccountReplicaDto):
        """Updates replica data.

        Args:
            data: MetaTrader account replica data.
        """
        self._data = data

    async def remove(self):
        """Removes MetaTrader account replica. Cloud account transitions to DELETING state.
        It takes some time for an account to be eventually deleted. Self-hosted account is deleted immediately.

        Returns:
            A coroutine resolving when account replica is scheduled for deletion.
        """
        await self._metatraderAccountClient.delete_account_replica(self.primary_account.id, self.id)
        try:
            await self._primaryAccount.reload()
        except Exception as err:
            if err.__class__.__name__ != 'NotFoundException':
                raise err

    async def deploy(self):
        """Schedules account replica for deployment. It takes some time for API server to be started and
        account replica to reach the DEPLOYED state.

        Returns:
            A coroutine resolving when account replica is scheduled for deployment.
        """
        await self._metatraderAccountClient.deploy_account_replica(self.primary_account.id, self.id)
        await self._primaryAccount.reload()

    async def undeploy(self):
        """Schedules account replica for undeployment. It takes some time for API server to be stopped and account to
        reach the UNDEPLOYED state.

        Returns:
            A coroutine resolving when account is scheduled for undeployment.
        """
        await self._metatraderAccountClient.undeploy_account_replica(self.primary_account.id, self.id)
        await self._primaryAccount.reload()

    async def redeploy(self):
        """Schedules account replica for redeployment. It takes some time for API server to be restarted and account
        replica to reach the DEPLOYED state.

        Returns:
            A coroutine resolving when account replica is scheduled for redeployment.
        """
        await self._metatraderAccountClient.redeploy_account_replica(self.primary_account.id, self.id)
        await self._primaryAccount.reload()

    async def increase_reliability(self):
        """Increases MetaTrader account replica reliability. The account replica will be temporary stopped to perform
        this action.

        Returns:
            A coroutine resolving when account replica reliability is increased.
        """
        await self._metatraderAccountClient.increase_reliability(self.id)
        await self._primaryAccount.reload()

    async def wait_deployed(self, timeout_in_seconds=300, interval_in_milliseconds=1000):
        """Waits until API server has finished deployment and account replica reached the DEPLOYED state.

        Args:
            timeout_in_seconds: Wait timeout in seconds, default is 5m.
            interval_in_milliseconds: Interval between account replica reloads while waiting for a change, default
            is 1s.

        Returns:
            A coroutine which resolves when account replica is deployed.

        Raises:
            TimeoutException: If account replica has not reached the DEPLOYED state within timeout allowed.
        """
        start_time = datetime.now()
        await self._primaryAccount.reload()
        while self.state != 'DEPLOYED' and (start_time + timedelta(seconds=timeout_in_seconds) > datetime.now()):
            await self._delay(interval_in_milliseconds)
            await self._primaryAccount.reload()
        if self.state != 'DEPLOYED':
            raise TimeoutException('Timed out waiting for account replica ' + self.id + ' to be deployed')

    async def wait_undeployed(self, timeout_in_seconds=300, interval_in_milliseconds=1000):
        """Waits until API server has finished undeployment and account replica reached the UNDEPLOYED state.

        Args:
            timeout_in_seconds: Wait timeout in seconds, default is 5m.
            interval_in_milliseconds: Interval between account replica reloads while waiting for a change, default
            is 1s.

        Returns:
            A coroutine which resolves when account replica is undeployed.

        Raises:
            TimeoutException: If account replica has not reached the UNDEPLOYED state within timeout allowed.
        """
        start_time = datetime.now()
        await self._primaryAccount.reload()
        while self.state != 'UNDEPLOYED' and (start_time + timedelta(seconds=timeout_in_seconds) > datetime.now()):
            await self._delay(interval_in_milliseconds)
            await self._primaryAccount.reload()
        if self.state != 'UNDEPLOYED':
            raise TimeoutException('Timed out waiting for account replica ' + self.id + ' to be undeployed')

    async def wait_removed(self, timeout_in_seconds=300, interval_in_milliseconds=1000):
        """Waits until account replica has been deleted.

        Args:
            timeout_in_seconds: Wait timeout in seconds, default is 5m.
            interval_in_milliseconds: Interval between account replica reloads while waiting for a change, default
            is 1s.

        Returns:
            A coroutine which resolves when account replica is deleted.

        Raises:
            TimeoutException: If account replica was not deleted within timeout allowed.
        """
        start_time = datetime.now()
        await self._primaryAccount.reload()
        while (start_time + timedelta(seconds=timeout_in_seconds)) > datetime.now() and \
                self.region in self._primaryAccount.account_regions and \
                self._primaryAccount.account_regions[self.region] == self.id:
            await self._delay(interval_in_milliseconds)
            await self._primaryAccount.reload()
        if self.region in self._primaryAccount.account_regions and \
                self._primaryAccount.account_regions[self.region] == self.id:
            raise TimeoutException('Timed out waiting for account replica ' + self.id + ' to be deleted')

    async def wait_connected(self, timeout_in_seconds=300, interval_in_milliseconds=1000):
        """Waits until API server has connected to the terminal and terminal has connected to the broker.

        Args:
            timeout_in_seconds: Wait timeout in seconds, default is 5m
            interval_in_milliseconds: Interval between account replica reloads while waiting for a change, default
            is 1s.

        Returns:
            A coroutine which resolves when API server is connected to the broker.

        Raises:
            TimeoutException: If account replica has not connected to the broker within timeout allowed.
        """
        start_time = datetime.now()
        await self._primaryAccount.reload()
        while self.connection_status != 'CONNECTED' and (start_time +
                                                         timedelta(seconds=timeout_in_seconds)) > datetime.now():
            await self._delay(interval_in_milliseconds)
            await self._primaryAccount.reload()
        if self.connection_status != 'CONNECTED':
            raise TimeoutException('Timed out waiting for account replica ' + self.id + ' to connect to the broker')

    async def update(self, account: UpdatedMetatraderAccountReplicaDto):
        """Updates MetaTrader account replica data.

        Args:
            account: MetaTrader account replica update.

        Returns:
            A coroutine resolving when account is updated.
        """
        await self._metatraderAccountClient.update_account_replica(self._primaryAccount.id, self.id, account)
        await self._primaryAccount.reload()

    async def _delay(self, timeout_in_milliseconds):
        await asyncio.sleep(timeout_in_milliseconds / 1000)
