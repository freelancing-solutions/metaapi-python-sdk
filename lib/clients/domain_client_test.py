from .httpClient import HttpClient
from .domain_client import DomainClient
import pytest
import respx
from mock import AsyncMock
from httpx import Response
from freezegun import freeze_time
import asyncio
http_client: HttpClient = None
domain_client: DomainClient = None
rsps: respx.Route = None
token = 'header.payload.sign'
host = 'https://mt-client-api-v1'
region = 'vint-hill'
host_data = {}
expected = 'https://mt-client-api-v1.vint-hill.agiliumtrade.ai'


@pytest.fixture(autouse=True)
async def run_around_tests():
    global http_client
    http_client = HttpClient()
    global domain_client
    domain_client = DomainClient(http_client, token)
    global host_data
    host_data = {
        'url': 'https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai',
        'hostname': 'mt-client-api-v1',
        'domain': 'agiliumtrade.ai'
    }
    global rsps
    rsps = respx.get('https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/'
                     'servers/mt-client-api').mock(return_value=Response(200, json=host_data))
    yield


class TestDomainClient:

    @respx.mock
    @pytest.mark.asyncio
    async def test_return_url(self):
        """Should return url."""
        url = await domain_client.get_url(host, region)
        assert url == expected
        assert len(rsps.calls) == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_return_cached_url_if_requested_again(self):
        """Should return cached url if requested again."""
        url = await domain_client.get_url(host, region)
        assert url == expected
        url2 = await domain_client.get_url(host, region)
        assert url2 == expected
        assert len(rsps.calls) == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_make_new_request_if_cache_expired(self):
        """Should make a new request if cache expired."""
        with freeze_time() as frozen_datetime:
            url = await domain_client.get_url(host, region)
            assert url == expected
            frozen_datetime.tick(11 * 60)
            url2 = await domain_client.get_url(host, region)
            assert url2 == expected
            assert len(rsps.calls) == 2

    @respx.mock
    @pytest.mark.asyncio
    async def test_wait_for_promise_if_url_is_being_requested(self):
        """Should wait for promise if url is being requested."""
        ignored_fields = await asyncio.gather(*[
            asyncio.create_task(domain_client.get_url(host, region)),
            asyncio.create_task(domain_client.get_url(host, region))
        ])
        assert ignored_fields[0] == expected
        assert ignored_fields[1] == expected
        assert len(rsps.calls) == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_return_error_to_promise(self):
        """Should return error to promise."""
        domain_client._httpClient.request = AsyncMock(side_effect=Exception('test'))
        responses = [
            asyncio.create_task(domain_client.get_url(host, region)),
            asyncio.create_task(domain_client.get_url(host, region))
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

        domain_client._httpClient.request.assert_called_once()
