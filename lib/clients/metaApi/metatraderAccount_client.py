from ..metaApi_client import MetaApiClient
from typing_extensions import TypedDict
from typing import List, Union, Optional, Dict, Literal
from httpx import Response


State = Literal['CREATED', 'DEPLOYING', 'DEPLOYED', 'DEPLOY_FAILED', 'UNDEPLOYING', 'UNDEPLOYED', 'UNDEPLOY_FAILED',
                'DELETING', 'DELETE_FAILED', 'REDEPLOY_FAILED']
"""Account state."""


ConnectionStatus = Literal['CONNECTED', 'DISCONNECTED', 'DISCONNECTED_FROM_BROKER']
"""Account connection status."""


Reliability = Literal['high', 'regular']
"""Account reliability."""


Type = Literal['cloud-g1', 'cloud-g2']
"""Account type"""


Platform = Literal['mt4', 'mt5']
"""MT platform."""


Version = Literal[4, 5]
"""MT version."""


CopyFactoryRoles = Literal['SUBSCRIBER', 'PROVIDER']
"""CopyFactory roles."""


class AccountsFilter(TypedDict, total=False):

    offset: Optional[int]
    """Search offset (defaults to 0) (must be greater or equal to 0)."""
    limit: Optional[int]
    """Search limit (defaults to 1000) (must be greater or equal to 1 and less or equal to 1000)."""
    version: Optional[Union[List[int], int]]
    """MT version (allowed values are 4 and 5)"""
    type: Optional[Union[List[str], str]]
    """Account type. Allowed values are 'cloud' and 'self-hosted'"""
    state: Optional[Union[List[State], State]]
    """Account state."""
    connectionStatus: Optional[Union[List[ConnectionStatus], ConnectionStatus]]
    """Connection status."""
    query: Optional[str]
    """Searches over _id, name, server and login to match query."""
    provisioningProfileId: Optional[str]
    """Provisioning profile id."""


class MetatraderAccountIdDto(TypedDict):
    """MetaTrader account id model"""

    id: str
    """MetaTrader account unique identifier"""


class MetatraderAccountReplicaDto(TypedDict, total=False):
    """Metatrader account replica model"""

    _id: str
    """Unique account replica id."""
    state: State
    """Current account replica state."""
    magic: int
    """Magic value the trades should be performed using."""
    connectionStatus: ConnectionStatus
    """Connection status of the MetaTrader terminal to the application."""
    quoteStreamingIntervalInSeconds: str
    """Quote streaming interval in seconds. Set to 0 in order to receive quotes on each tick. Default value is
    2.5 seconds. Intervals less than 2.5 seconds are supported only for G2."""
    symbol: Optional[str]
    """Any symbol provided by broker (required for G1 only)."""
    reliability: Reliability
    """Used to increase the reliability of the account replica. High is a recommended value for production
    environment."""
    tags: List[str]
    """User-defined account replica tags."""
    metadata: Optional[Dict]
    """Extra information which can be stored together with your account."""
    resourceSlots: Optional[int]
    """Number of resource slots to allocate to account. Allocating extra resource slots
    results in better account performance under load which is useful for some applications. E.g. if you have many
    accounts copying the same strategy via CopyFactory API, then you can increase resourceSlots to get a lower trade
    copying latency. Please note that allocating extra resource slots is a paid option. Please note that high
    reliability accounts use redundant infrastructure, so that each resource slot for a high reliability account
    is billed as 2 standard resource slots."""
    copyFactoryResourceSlots: Optional[int]
    """Number of CopyFactory 2 resource slots to allocate to account replica.
    Allocating extra resource slots results in lower trade copying latency. Please note that allocating extra resource
    slots is a paid option. Please also note that CopyFactory 2 uses redundant infrastructure so that
    each CopyFactory resource slot is billed as 2 standard resource slots. You will be billed for CopyFactory 2
    resource slots only if you have added your account to CopyFactory 2 by specifying copyFactoryRoles field."""
    region: str
    """Region id to deploy account at. One of returned by the /users/current/regions endpoint."""
    primaryAccount: dict
    """Primary account."""


class AccountConnection(TypedDict, total=False):
    """Account connection."""
    region: str
    """Region the account is connected at."""
    zone: str
    """Availability zone the account is connected at."""
    application: str
    """Application the account is connected to, one of `MetaApi`, `CopyFactory subscriber`,
    `CopyFactory provider`, `CopyFactory history import`, `Risk management`."""


class MetatraderAccountDto(TypedDict, total=False):
    """MetaTrader account model"""

    _id: str
    """Unique account id."""
    state: State
    """Current account state."""
    magic: int
    """MetaTrader magic to place trades using."""
    connectionStatus: ConnectionStatus
    """Connection status of the MetaTrader terminal to the application."""
    quoteStreamingIntervalInSeconds: str
    """Quote streaming interval in seconds. Set to 0 in order to receive quotes on each tick. Default value is
    2.5 seconds. Intervals less than 2.5 seconds are supported only for G2."""
    symbol: Optional[str]
    """Any symbol provided by broker (required for G1 only)."""
    reliability: Reliability
    """Used to increase the reliability of the account. High is a recommended value for production environment."""
    tags: Optional[List[str]]
    """User-defined account tags."""
    metadata: Optional[Dict]
    """Extra information which can be stored together with your account. Total length of this field after serializing
    it to JSON is limited to 1024 characters."""
    resourceSlots: int
    """Number of resource slots to allocate to account. Allocating extra resource slots
    results in better account performance under load which is useful for some applications. E.g. if you have many
    accounts copying the same strategy via CopyFactory API, then you can increase resourceSlots to get a lower trade
    copying latency. Please note that allocating extra resource slots is a paid option. Please note that high
    reliability accounts use redundant infrastructure, so that each resource slot for a high reliability account
    is billed as 2 standard resource slots."""
    copyFactoryResourceSlots: int
    """Number of CopyFactory 2 resource slots to allocate to account.
    Allocating extra resource slots results in lower trade copying latency. Please note that allocating extra resource
    slots is a paid option. Please also note that CopyFactory 2 uses redundant infrastructure so that
    each CopyFactory resource slot is billed as 2 standard resource slots. You will be billed for CopyFactory 2
    resource slots only if you have added your account to CopyFactory 2 by specifying copyFactoryRoles field."""
    region: str
    """Region id to deploy account at. One of returned by the /users/current/regions endpoint."""
    name: str
    """Human-readable account name."""
    manualTrades: bool
    """Flag indicating if trades should be placed as manual trades. Supported on G2 only."""
    slippage: Optional[float]
    """Default trade slippage in points. Should be greater or equal to zero. If not specified, system internal setting
    will be used which we believe is reasonable for most cases."""
    provisioningProfileId: Optional[str]
    """Id of the provisioning profile that was used as the basis for creating this account."""
    login: str
    """MetaTrader account number."""
    server: str
    """MetaTrader server name to connect to."""
    type: Type
    """Account type. Executing accounts as cloud-g2 is faster and cheaper."""
    version: Version
    """MetaTrader version."""
    hash: float
    """Hash-code of the account."""
    baseCurrency: str
    """3-character ISO currency code of the account base currency. The setting is to be used for copy trading accounts
    which use national currencies only, such as some Brazilian brokers. You should not alter this setting unless you
    understand what you are doing."""
    copyFactoryRoles: List[CopyFactoryRoles]
    """Account roles for CopyFactory2 application."""
    riskManagementApiEnabled: bool
    """Flag indicating that risk management API is enabled on account."""
    metastatsHourlyTarificationEnabled: bool
    """Flag indicating that MetaStats hourly tarification is enabled on account."""
    accessToken: str
    """Authorization token to be used for accessing single account data. Intended to be used in browser API."""
    connections: List[AccountConnection]
    """Active account connections."""
    primaryReplica: bool
    """Flag indicating that account is primary."""
    userId: str
    """User id."""
    primaryAccountId: Optional[str]
    """Primary account id. Only replicas can have this field."""
    accountReplicas: Optional[List[MetatraderAccountReplicaDto]]
    """MetaTrader account replicas."""


class NewMetatraderAccountDto(TypedDict, total=False):
    """New MetaTrader account model"""
    symbol: Optional[str]
    """Any MetaTrader symbol your broker provides historical market data for. This value should be specified for G1
    accounts only and only in case your MT account fails to connect to broker."""
    magic: int
    """Magic value the trades should be performed using. When manualTrades field is set to true, magic value
    must be 0."""
    quoteStreamingIntervalInSeconds: Optional[str]
    """Quote streaming interval in seconds. Set to 0 in order to receive quotes on each tick. Default value is 2.5
    seconds. Intervals less than 2.5 seconds are supported only for G2."""
    tags: Optional[List[str]]
    """User-defined account tags."""
    metadata: Optional[Dict]
    """Extra information which can be stored together with your account. Total length of this field after serializing
    it to JSON is limited to 1024 characters."""
    reliability: Optional[Reliability]
    """Used to increase the reliability of the account. High is a recommended value for production environment.
    Default value is high."""
    resourceSlots: Optional[int]
    """Number of resource slots to allocate to account. Allocating extra resource slots
    results in better account performance under load which is useful for some applications. E.g. if you have many
    accounts copying the same strategy via CooyFactory API, then you can increase resourceSlots to get a lower trade
    copying latency. Please note that allocating extra resource slots is a paid option. Please note that high
    reliability accounts use redundant infrastructure, so that each resource slot for a high reliability account
    is billed as 2 standard resource slots. Default is 1."""
    copyFactoryResourceSlots: Optional[int]
    """Number of CopyFactory 2 resource slots to allocate to account.
    Allocating extra resource slots results in lower trade copying latency. Please note that allocating extra resource
    slots is a paid option. Please also note that CopyFactory 2 uses redundant infrastructure so that
    each CopyFactory resource slot is billed as 2 standard resource slots. You will be billed for CopyFactory 2
    resource slots only if you have added your account to CopyFactory 2 by specifying copyFactoryRoles field.
    Default is 1."""
    region: str
    """Region id to deploy account at. One of returned by the /users/current/regions endpoint."""
    name: str
    """Human-readable account name."""
    manualTrades: Optional[bool]
    """Flag indicating if trades should be placed as manual trades. Supported on G2 only. Default is false."""
    slippage: Optional[float]
    """Default trade slippage in points. Should be greater or equal to zero. If not specified, system internal setting
    will be used which we believe is reasonable for most cases."""
    provisioningProfileId: Optional[str]
    """Id of the provisioning profile that was used as the basis for creating this account.
    Required for cloud account."""
    login: str
    """MetaTrader account number. Only digits are allowed."""
    password: str
    """MetaTrader account password. The password can be either investor password for read-only
    access or master password to enable trading features. Required for cloud account."""
    server: str
    """MetaTrader server name to connect to."""
    platform: Optional[Platform]
    """MetaTrader platform."""
    type: Optional[Type]
    """Account type. Executing accounts as cloud-g2 is faster and cheaper. Default value is cloud-g2."""
    baseCurrency: Optional[str]
    """3-character ISO currency code of the account base currency. Default value is USD. The setting is to be used
    for copy trading accounts which use national currencies only, such as some Brazilian
    brokers. You should not alter this setting unless you understand what you are doing."""
    copyFactoryRoles: Optional[List[CopyFactoryRoles]]
    """Account roles for CopyFactory2 API."""
    riskManagementApiEnabled: Optional[bool]
    """Flag indicating that risk management API should be enabled on account. Default is false."""
    metastatsHourlyTarificationEnabled: Optional[bool]
    """Flag indicating that MetaStats hourly tarification should be enabled on account. Default is false"""


class MetatraderAccountUpdateDto(TypedDict, total=False):
    """Updated MetaTrader account data"""

    name: str
    """Human-readable account name."""
    password: str
    """MetaTrader account password. The password can be either investor password for read-only
    access or master password to enable trading features. Required for cloud account."""
    server: str
    """MetaTrader server name to connect to."""
    magic: Optional[int]
    """Magic value the trades should be performed using. When manualTrades field is set to true, magic value must
    be 0."""
    manualTrades: Optional[bool]
    """Flag indicating if trades should be placed as manual trades. Supported for G2 only. Default is false."""
    slippage: Optional[float]
    """Default trade slippage in points. Should be greater or equal to zero. If not specified,
    system internal setting will be used which we believe is reasonable for most cases."""
    quoteStreamingIntervalInSeconds: Optional[float]
    """Quote streaming interval in seconds. Set to 0 in order to receive quotes on each tick. Intervals less than 2.5
    seconds are supported only for G2. Default value is 2.5 seconds"""
    tags: Optional[List[str]]
    """MetaTrader account tags."""
    metadata: Optional[Dict]
    """Extra information which can be stored together with your account. Total length of this field after serializing
    it to JSON is limited to 1024 characters."""
    resourceSlots: Optional[int]
    """Number of resource slots to allocate to account. Allocating extra resource slots
    results in better account performance under load which is useful for some applications. E.g. if you have many
    accounts copying the same strategy via CooyFactory API, then you can increase resourceSlots to get a lower trade
    copying latency. Please note that allocating extra resource slots is a paid option. Default is 1."""
    copyFactoryResourceSlots: Optional[int]
    """Number of CopyFactory 2 resource slots to allocate to account.
    Allocating extra resource slots results in lower trade copying latency. Please note that allocating extra resource
    slots is a paid option. Please also note that CopyFactory 2 uses redundant infrastructure so that
    each CopyFactory resource slot is billed as 2 standard resource slots. You will be billed for CopyFactory 2
    resource slots only if you have added your account to CopyFactory 2 by specifying copyFactoryRoles field.
    Default is 1."""


class NewMetaTraderAccountReplicaDto(TypedDict, total=False):
    """New MetaTrader account replica model"""

    symbol: Optional[str]
    """Any MetaTrader symbol your broker provides historical market data for.
    This value should be specified for G1 accounts only and only in case your MT account fails to connect to broker."""
    magic: int
    """Magic value the trades should be performed using. When manualTrades field is set to true, magic value must
    be 0."""
    quoteStreamingIntervalInSeconds: Optional[str]
    """Quote streaming interval in seconds. Set to 0 in order to receive quotes on each tick. Default value is 2.5
    seconds. Intervals less than 2.5 seconds are supported only for G2."""
    tags: Optional[List[str]]
    """User-defined account replica tags."""
    metadata: Optional[Dict]
    """Extra information which can be stored together with your account. Total length of this field after serializing
    it to JSON is limited to 1024 characters."""
    reliability: Optional[Reliability]
    """Used to increase the reliability of the account replica. High is a recommended value for production environment.
    Default value is high."""
    resourceSlots: Optional[int]
    """Number of resource slots to allocate to account replica. Allocating extra resource slots
    results in better account performance under load which is useful for some applications. E.g. if you have many
    accounts copying the same strategy via CooyFactory API, then you can increase resourceSlots to get a lower trade
    copying latency. Please note that allocating extra resource slots is a paid option. Please note that high
    reliability accounts use redundant infrastructure, so that each resource slot for a high reliability account
    is billed as 2 standard resource slots. Default is 1."""
    copyFactoryResourceSlots: Optional[int]
    """Number of CopyFactory 2 resource slots to allocate to account replica.
    Allocating extra resource slots results in lower trade copying latency. Please note that allocating extra resource
    slots is a paid option. Please also note that CopyFactory 2 uses redundant infrastructure so that
    each CopyFactory resource slot is billed as 2 standard resource slots. You will be billed for CopyFactory 2
    resource slots only if you have added your account to CopyFactory 2 by specifying copyFactoryRoles field.
    Default is 1."""
    region: str
    """Region id to deploy account replica at. One of returned by the /users/current/regions endpoint."""


class UpdatedMetatraderAccountReplicaDto(TypedDict, total=False):
    """Updated MetaTrader account replica data"""

    magic: Optional[int]
    """Magic value the trades should be performed using. When manualTrades field is set to true, magic value must
    be 0."""
    quoteStreamingIntervalInSeconds: float
    """Quote streaming interval in seconds. Set to 0 in order to receive quotes on each tick. Default value is
    2.5 seconds. Intervals less than 2.5 seconds are supported only for G2."""
    tags: Optional[List[str]]
    """MetaTrader account tags."""
    metadata: Dict
    """Extra information which can be stored together with your account."""
    copyFactoryRoles: List[str]
    """Account roles for CopyFactory2 application. Allowed values are `PROVIDER` and `SUBSCRIBER`."""
    resourceSlots: Optional[int]
    """Number of resource slots to allocate to account. Allocating extra resource slots results in better account
    performance under load which is useful for some applications. E.g. if you have many accounts copying the same
    strategy via CopyFactory API, then you can increase resourceSlots to get a lower trade copying latency. Please
    note that allocating extra resource slots is a paid option. Default is 1."""
    copyFactoryResourceSlots: Optional[int]
    """Number of CopyFactory 2 resource slots to allocate to account. Allocating extra resource slots results in lower
    trade copying latency. Please note that allocating extra resource slots is a paid option. Please also note that
    CopyFactory 2 uses redundant infrastructure so that each CopyFactory resource slot is billed as 2 standard resource
    slots. You will be billed for CopyFactory 2 resource slots only if you have added your account to CopyFactory 2 by
    specifying copyFactoryRoles field. Default is 1."""


class MetatraderAccountClient(MetaApiClient):
    """metaapi.cloud MetaTrader account API client (see https://metaapi.cloud/docs/provisioning/)

    Attributes:
        _httpClient: HTTP client
        _host: domain to connect to
        _token: authorization token
    """

    async def get_accounts(self, accounts_filter: AccountsFilter = None) -> Response:
        """Retrieves MetaTrader accounts owned by user
        (see https://metaapi.cloud/docs/provisioning/api/account/readAccounts/)

        Args:
            accounts_filter: Optional filter.

        Returns:
            A coroutine resolving with List[MetatraderAccountDto] - MetaTrader accounts found.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('get_accounts')
        opts = {
            'url': f'{self._host}/users/current/accounts',
            'method': 'GET',
            'params': accounts_filter or {},
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'get_accounts')

    async def get_account(self, id: str) -> Response:
        """Retrieves a MetaTrader account by id (see https://metaapi.cloud/docs/provisioning/api/account/readAccount/).
        Throws an error if account is not found.

        Args:
            id: MetaTrader account id.

        Returns:
            A coroutine resolving with MetatraderAccountDto - MetaTrader account found.
        """
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}',
            'method': 'GET',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'get_account')

    async def get_account_replica(self, primary_account_id: str, replica_id: str) -> Response:
        """Retrieves a MetaTrader account replica by id
        (see https://metaapi.cloud/docs/provisioning/api/accountReplica/readAccountReplica/).
        Throws an error if account is not found.

        Args:
            primary_account_id: MetaTrader account id.
            replica_id: MetaTrader account replica id.

        Returns:
            A coroutine resolving with MetatraderAccountReplicaDto - MetaTrader account replica found.
        """
        opts = {
            'url': f'{self._host}/users/current/accounts/{primary_account_id}/replicas/{replica_id}',
            'method': 'GET',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'get_account_replica')

    async def get_account_replicas(self, primary_account_id: str) -> Response:
        """Retrieves a MetaTrader account replicas (see
        https://metaapi.cloud/docs/provisioning/api/accountReplica/readAccountReplicas/).
        Throws an error if account is not found.

        Args:
            primary_account_id: MetaTrader account id.

        Returns:
            A coroutine resolving with MetatraderAccountReplicaDto - MetaTrader account replica found.
        """
        opts = {
            'url': f'{self._host}/users/current/accounts/{primary_account_id}/replicas',
            'method': 'GET',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'get_account_replicas')

    async def get_account_by_token(self) -> 'Response[MetatraderAccountDto]':
        """Retrieves a MetaTrader account by token
        (see https://metaapi.cloud/docs/provisioning/api/account/readAccount/). Throws an error if account is
        not found. Method is accessible only with account access token.

        Returns:
            A coroutine resolving with MetaTrader account found.
        """
        if self._is_not_account_token():
            return self._handle_no_access_exception('get_account_by_token')
        opts = {
            'url': f'{self._host}/users/current/accounts/accessToken/{self._token}',
            'method': 'GET'
        }
        return await self._httpClient.request(opts, 'get_account_by_token')

    async def create_account(self, account: NewMetatraderAccountDto) -> Response:
        """Starts cloud API server for a MetaTrader account using specified provisioning profile
        (see https://metaapi.cloud/docs/provisioning/api/account/createAccount/).
        It takes some time to launch the terminal and connect the terminal to the broker, you can use the
        connectionStatus field to monitor the current status of the terminal.

        Args:
            account: MetaTrader account to create.

        Returns:
            A coroutine resolving with MetatraderAccountIdDto - an id of the MetaTrader account created.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('create_account')
        opts = {
            'url': f'{self._host}/users/current/accounts',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            },
            'body': account
        }
        return await self._httpClient.request(opts, 'create_account')

    async def create_account_replica(self, account_id: str, replica: NewMetaTraderAccountReplicaDto) -> Response:
        """Starts cloud API server for a MetaTrader account replica using specified primary account
        (see https://metaapi.cloud/docs/provisioning/api/accountReplica/createAccountReplica/).
        It takes some time to launch the terminal and connect the terminal to the broker, you can use the
        connectionStatus field to monitor the current status of the terminal.
        Method is accessible only with API access token.

        Args:
            account_id: Primary MetaTrader account id.
            replica: MetaTrader account replica to create

        Returns:
            A coroutine resolving with MetatraderAccountIdDto - an id of the MetaTrader account replica created.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('create_account_replica')
        opts = {
            'url': f'{self._host}/users/current/accounts/{account_id}/replicas',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            },
            'body': replica
        }
        return await self._httpClient.request(opts, 'create_account_replica')

    async def deploy_account(self, id: str) -> Response:
        """Starts API server for MetaTrader account. This request will be ignored if the account has already
        been deployed. (see https://metaapi.cloud/docs/provisioning/api/account/deployAccount/)

        Args:
            id: MetaTrader account id to deploy.

        Returns:
            A coroutine resolving when MetaTrader account is scheduled for deployment
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('deploy_account')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}/deploy',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'deploy_account')

    async def deploy_account_replica(self, primary_account_id: str, replica_id: str) -> Response:
        """Starts API server for MetaTrader account replica. This request will be ignored if the replica has already
        been deployed. (see https://metaapi.cloud/docs/provisioning/api/accountReplica/deployAccountReplica/)

        Args:
            primary_account_id: MetaTrader account id.
            replica_id: MetaTrader account replica id to deploy.

        Returns:
            A coroutine resolving when MetaTrader account replica is scheduled for deployment.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('deploy_account_replica')
        opts = {
            'url': f'{self._host}/users/current/accounts/{primary_account_id}/replicas/{replica_id}/deploy',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'deploy_account_replica')

    async def undeploy_account(self, id: str) -> Response:
        """Stops API server for a MetaTrader account. Terminal data such as downloaded market history data will
        be preserved. (see https://metaapi.cloud/docs/provisioning/api/account/undeployAccount/)

        Args:
            id: MetaTrader account id to undeploy.

        Returns:
            A coroutine resolving when MetaTrader account is scheduled for undeployment.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('undeploy_account')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}/undeploy',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'undeploy_account')

    async def undeploy_account_replica(self, primary_account_id: str, replica_id: str) -> Response:
        """Stops API server for MetaTrader account replica. Terminal data such as downloaded market history data will
        be preserved. (see https://metaapi.cloud/docs/provisioning/api/accountReplica/undeployAccountReplica/)

        Args:
            primary_account_id: MetaTrader account id to undeploy.
            replica_id: MetaTrader account replica id to undeploy.

        Returns:
            A coroutine resolving when MetaTrader account replica is scheduled for undeployment.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('undeploy_account_replica')
        opts = {
            'url': f'{self._host}/users/current/accounts/{primary_account_id}/replicas/{replica_id}/undeploy',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'undeploy_account_replica')

    async def redeploy_account(self, id: str) -> Response:
        """Redeploys MetaTrader account. This is equivalent to undeploy immediately followed by deploy.
        (see https://metaapi.cloud/docs/provisioning/api/account/redeployAccount/)

        Args:
            id: MetaTrader account id to redeploy.

        Returns:
            A coroutine resolving when MetaTrader account is scheduled for redeployment.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('redeploy_account')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}/redeploy',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'redeploy_account')

    async def redeploy_account_replica(self, primary_account_id: str, replica_id: str) -> Response:
        """Redeploys MetaTrader account replica. This is equivalent to undeploy immediately followed by deploy.
        (see https://metaapi.cloud/docs/provisioning/api/account/redeployAccountReplica/)

        Args:
            primary_account_id: MetaTrader account id.
            replica_id: MetaTrader account replica id to redeploy.

        Returns:
            A coroutine resolving when MetaTrader account replica is scheduled for redeployment.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('redeploy_account_replica')
        opts = {
            'url': f'{self._host}/users/current/accounts/{primary_account_id}/replicas/{replica_id}/redeploy',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'redeploy_account_replica')

    async def delete_account(self, id: str) -> Response:
        """Stops and deletes an API server for a specified MetaTrader account. The terminal state such as downloaded
        market data history will be deleted as well when you delete the account.
        (see https://metaapi.cloud/docs/provisioning/api/account/deleteAccount/)

        Args:
            id: MetaTrader account id to delete.

        Returns:
            A coroutine resolving when MetaTrader account is scheduled for deletion.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('delete_account')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}',
            'method': 'DELETE',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'delete_account')

    async def delete_account_replica(self, primary_account_id: str, replica_id: str) -> Response:
        """Stops and deletes an API server for a specified MetaTrader account. The terminal state such as downloaded
        market data history will be deleted as well when you delete the account.
        (see https://metaapi.cloud/docs/provisioning/api/account/deleteAccountReplica/).
        Method is accessible only with API access token.

        Args:
            primary_account_id: MetaTrader account id.
            replica_id: MetaTrader account replica id to delete.

        Returns:
            A coroutine resolving when MetaTrader account is scheduled for deletion.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('delete_account_replica')
        opts = {
            'url': f'{self._host}/users/current/accounts/{primary_account_id}/replicas/{replica_id}',
            'method': 'DELETE',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'delete_account_replica')

    async def update_account(self, id: str, account: MetatraderAccountUpdateDto) -> Response:
        """Updates existing metatrader account data (see
        https://metaapi.cloud/docs/provisioning/api/account/updateAccount/)

        Args:
            id: MetaTrader account id.
            account: Updated MetaTrader account.

        Returns:
            A coroutine resolving when MetaTrader account is updated.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('update_account')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}',
            'method': 'PUT',
            'headers': {
                'auth-token': self._token
            },
            'body': account
        }
        return await self._httpClient.request(opts, 'update_account')

    async def update_account_replica(self, primary_account_id: str, replica_id: str,
                                     account: UpdatedMetatraderAccountReplicaDto) -> Response:
        """ Updates existing metatrader account replica data (see
        https://metaapi.cloud/docs/provisioning/api/account/updateAccountReplica/)

        Args:
            primary_account_id: MetaTrader account id.
            replica_id: MetaTrader account replica id.
            account: Updated MetaTrader account.

        Returns:
            A coroutine resolving when MetaTrader account replica is updated.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('update_account_replica')
        opts = {
            'url': f'{self._host}/users/current/accounts/{primary_account_id}/replicas/{replica_id}',
            'method': 'PUT',
            'headers': {
                'auth-token': self._token
            },
            'body': account
        }
        return await self._httpClient.request(opts, 'update_account_replica')

    async def increase_reliability(self, id: str):
        """Increases MetaTrader account reliability. The account will be temporary stopped to perform this action. (see
        https://metaapi.cloud/docs/provisioning/api/account/increaseReliability/).
        Method is accessible only with API access token.

        Args:
            id: MetaTrader account id.

        Returns:
            A coroutine resolving when MetaTrader account reliability is increased.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('increase_reliability')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}/increase-reliability',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'increase_reliability')

    async def enable_risk_management_api(self, id: str):
        """Enable risk management API for an account. The account will be temporary stopped to perform this action.
        Note that this is a paid option. (see
        https://metaapi.cloud/docs/provisioning/api/account/enableRiskManagementApi/).
        Method is accessible only with API access token.

        Args:
            id: Account id.

        Returns:
            A coroutine resolving when account risk management is enabled.
        """
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('enable_risk_management_api')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}/enable-risk-management-api',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'enable_risk_management_api')

    async def enable_metastats_hourly_tarification(self, id: str):
        """Enable MetaStats hourly tarification for an account. The account will be temporary stopped to perform this
        action. Note that this is a paid option. (see
        https://metaapi.cloud/docs/provisioning/api/account/enableMetaStatsHourlyTarification/).
        Method is accessible only with API access token."""
        if self._is_not_jwt_token():
            return self._handle_no_access_exception('enable_metastats_hourly_tarification')
        opts = {
            'url': f'{self._host}/users/current/accounts/{id}/enable-metastats-hourly-tarification',
            'method': 'POST',
            'headers': {
                'auth-token': self._token
            }
        }
        return await self._httpClient.request(opts, 'enable_metastats_hourly_tarification')
