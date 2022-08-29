from .httpClient import HttpClient
from .domain_client import DomainClient
import pytest
import respx
from mock import AsyncMock, patch
from httpx import Response
from freezegun import freeze_time
import asyncio
from asyncio import sleep
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
    async def test_retry_request_if_received_error(self):
        """Should retry request if received error."""
        with patch('lib.clients.domain_client.asyncio.sleep', new=lambda x: sleep(x / 60)):
            call_number = 0

            def request_stub(opts1, opts2):
                nonlocal call_number
                call_number += 1
                if call_number < 3:
                    raise Exception('test')
                else:
                    return host_data

            domain_client._httpClient.request = AsyncMock(side_effect=request_stub)
            responses = [
                asyncio.create_task(domain_client.get_url(host, region)),
                asyncio.create_task(domain_client.get_url(host, region))
            ]
            await sleep(0.11)
            assert await responses[0] == expected
            assert await responses[1] == expected

            assert domain_client._httpClient.request.call_count == 3

    @respx.mock
    @pytest.mark.asyncio
    async def test_return_domain_settings(self):
        """Should return domain settings."""
        settings = await domain_client.get_settings()

        assert settings == {
            'hostname': 'mt-client-api-v1',
            'domain': 'agiliumtrade.ai'
        }
        assert len(rsps.calls) == 1
