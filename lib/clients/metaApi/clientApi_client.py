from ..metaApi_client import MetaApiClient
from ..httpClient import HttpClient
from ..domain_client import DomainClient
from typing_extensions import TypedDict
from typing import List
from datetime import datetime
import asyncio


class TypeHashingIgnoredFieldLists(TypedDict):
    """Type hashing ignored field lists"""
    specification: List[str]
    """Specification ignored fields."""
    position: List[str]
    """Position ignored fields."""
    order: List[str]
    """Order ignored fields."""


class HashingIgnoredFieldLists(TypedDict):
    """Hashing ignored field lists."""
    g1: TypeHashingIgnoredFieldLists
    """G1 hashing ignored field lists."""
    g2: TypeHashingIgnoredFieldLists
    """G2 hashing ignored field lists."""


class ClientApiClient(MetaApiClient):
    """metaapi.cloud client API client (see https://metaapi.cloud/docs/client/)"""

    def __init__(self, http_client: HttpClient, domain_client: DomainClient):
        """Inits client API client instance.

        Args:
            http_client: HTTP client.
            domain_client: Domain client.
        """
        super().__init__(http_client, domain_client)
        self._host = f'https://mt-client-api-v1'
        self._ignoredFieldListsCache = {
            'lastUpdated': 0,
            'data': None,
            'requestPromise': None
        }

    async def get_hashing_ignored_field_lists(self, region: str) -> HashingIgnoredFieldLists:
        """Retrieves hashing ignored field lists.

        Args:
            region: Account region.

        Returns:
            A coroutine resolving with hashing ignored field lists
        """
        if self._ignoredFieldListsCache['data'] is None or \
                datetime.now().timestamp() - self._ignoredFieldListsCache['lastUpdated'] > 60 * 60:
            if self._ignoredFieldListsCache['requestPromise']:
                await self._ignoredFieldListsCache['requestPromise']
            else:
                future = asyncio.Future()
                self._ignoredFieldListsCache['requestPromise'] = future
                host = await self._domainClient.get_url(self._host, region)
                opts = {
                    'url': f'{host}/hashing-ignored-field-lists',
                    'method': 'GET',
                    'headers': {
                        'auth-token': self._token
                    }
                }

                try:
                    response = await self._httpClient.request(opts, 'get_hashing_ignored_field_lists')
                    self._ignoredFieldListsCache = {
                        'lastUpdated': datetime.now().timestamp(), 'data': response, 'requestPromise': None}
                    future.set_result(response)
                except Exception as err:
                    future.set_exception(err)
                    raise err

        return self._ignoredFieldListsCache['data']
