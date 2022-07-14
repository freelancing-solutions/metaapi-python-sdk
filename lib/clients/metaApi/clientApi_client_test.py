import pytest
import respx
from httpx import Response
import asyncio
from ..httpClient import HttpClient
from .clientApi_client import ClientApiClient
from freezegun import freeze_time
from mock import AsyncMock, MagicMock

CLIENT_API_URL = 'https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai'
token = 'header.payload.sign'
http_client = HttpClient()
client_api_client: ClientApiClient = None
expected = None
domain_client: MagicMock = None


@pytest.fixture(autouse=True)
async def run_around_tests():
    global expected
    expected = {
        'g1': {
            'specification': ['description'],
            'position': ['time'],
            'order': ['expirationTime']
        },
        'g2': {
            'specification': ['pipSize'],
            'position': ['comment'],
            'order': ['brokerComment']
        }
    }
    global http_client
    http_client = HttpClient()
    global domain_client
    domain_client = MagicMock()
    domain_client.token = token
    domain_client.domain = 'agiliumtrade.agiliumtrade.ai'
    domain_client.get_url = AsyncMock(return_value=CLIENT_API_URL)
    global client_api_client
    client_api_client = ClientApiClient(http_client, domain_client)
    yield


class TestClientApiClient:
    @respx.mock
    @pytest.mark.asyncio
    async def test_retrieve(self):
        """Should retrieve hashing ignored field lists."""
        rsps = respx.get(f'{CLIENT_API_URL}/hashing-ignored-field-lists') \
            .mock(return_value=Response(200, json=expected))
        ignored_fields = await client_api_client.get_hashing_ignored_field_lists('vint-hill')
        assert rsps.calls[0].request.url == \
            f'{CLIENT_API_URL}/hashing-ignored-field-lists'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert ignored_fields == expected
        domain_client.get_url.assert_called_with('https://mt-client-api-v1', 'vint-hill')

    @respx.mock
    @pytest.mark.asyncio
    async def test_return_cached_data(self):
        """Should return cached data if requested recently."""
        rsps = respx.get(f'{CLIENT_API_URL}/hashing-ignored-field-lists') \
            .mock(return_value=Response(200, json=expected))
        ignored_fields = await client_api_client.get_hashing_ignored_field_lists('vint-hill')
        assert ignored_fields == expected
        ignored_fields2 = await client_api_client.get_hashing_ignored_field_lists('vint-hill')
        assert ignored_fields2 == expected
        assert len(rsps.calls) == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_update_when_caching_time_expired(self):
        """Should update data when caching time expired."""
        with freeze_time() as frozen_datetime:
            rsps = respx.get(f'{CLIENT_API_URL}/hashing-ignored-field-lists') \
                .mock(return_value=Response(200, json=expected))
            ignored_fields = await client_api_client.get_hashing_ignored_field_lists('vint-hill')
            assert ignored_fields == expected
            frozen_datetime.tick(3601)
            ignored_fields2 = await client_api_client.get_hashing_ignored_field_lists('vint-hill')
            assert ignored_fields2 == expected
            assert len(rsps.calls) == 2

    @respx.mock
    @pytest.mark.asyncio
    async def test_send_one_request_if_two_sync(self):
        """Should send one request if two concurrent synchronizations."""
        rsps = respx.get(f'{CLIENT_API_URL}/hashing-ignored-field-lists') \
            .mock(return_value=Response(200, json=expected))
        ignored_fields = await asyncio.gather(*[
            asyncio.create_task(client_api_client.get_hashing_ignored_field_lists('vint-hill')),
            asyncio.create_task(client_api_client.get_hashing_ignored_field_lists('vint-hill'))
        ])
        assert ignored_fields[0] == expected
        assert ignored_fields[1] == expected
        assert len(rsps.calls) == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_return_error_to_promise(self):
        """Should return error to promise."""
        client_api_client._httpClient.request = AsyncMock(side_effect=Exception('test'))
        responses = [
            asyncio.create_task(client_api_client.get_hashing_ignored_field_lists('vint-hill')),
            asyncio.create_task(client_api_client.get_hashing_ignored_field_lists('vint-hill'))
        ]
        try:
            await responses[0]
            pytest.fail()
        except Exception as err:
            assert err.args[0] == 'test'
        try:
            await responses[1]
            pytest.fail()
        except Exception as err:
            assert err.args[0] == 'test'
