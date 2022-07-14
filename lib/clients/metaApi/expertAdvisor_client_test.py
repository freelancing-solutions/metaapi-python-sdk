import json
from mock import MagicMock, patch, mock_open
import pytest
import respx
from httpx import Response
from ..httpClient import HttpClient
from .expertAdvisor_client import ExpertAdvisorClient

PROVISIONING_API_URL = 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai'
httpClient = HttpClient()
token = 'header.payload.sign'
account_token = 'token'
domain_client: MagicMock = None
expert_advisor_client: ExpertAdvisorClient = None


@pytest.fixture(autouse=True)
async def run_around_tests():
    global http_client
    http_client = HttpClient()
    global domain_client
    domain_client = MagicMock()
    domain_client.token = token
    domain_client.domain = 'agiliumtrade.agiliumtrade.ai'
    global expert_advisor_client
    expert_advisor_client = ExpertAdvisorClient(http_client, domain_client)
    yield


class TestExpertAdvisorClient:
    @respx.mock
    @pytest.mark.asyncio
    async def test_retrieve_many(self):
        """Should retrieve expert advisors from API."""
        expected = [{
            'expertId': 'my-ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }]
        rsps = respx.get(f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors') \
            .mock(return_value=Response(200, json=expected))
        profiles = await expert_advisor_client.get_expert_advisors('id')
        assert rsps.calls[0].request.url == \
            f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert profiles == expected

    @pytest.mark.asyncio
    async def test_not_retrieve_advisors_with_account_token(self):
        """Should not retrieve expert advisors from API with account token."""
        domain_client.token = account_token
        expert_advisor_client = ExpertAdvisorClient(http_client, domain_client)
        try:
            await expert_advisor_client.get_expert_advisors('id')
        except Exception as err:
            assert err.__str__() == 'You can not invoke get_expert_advisors method, because you ' + \
                'have connected with account access token. Please use API access token from ' + \
                'https://app.metaapi.cloud/token page to invoke this method.'

    @respx.mock
    @pytest.mark.asyncio
    async def test_retrieve_one(self):
        """Should retrieve expert advisor from API."""
        expected = {
            'expertId': 'my-ea',
            'period': '1H',
            'symbol': 'EURUSD',
            'fileUploaded': False
        }
        rsps = respx.get(f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea') \
            .mock(return_value=Response(200, json=expected))
        profiles = await expert_advisor_client.get_expert_advisor('id', 'my-ea')
        assert rsps.calls[0].request.url == \
               f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert profiles == expected

    @pytest.mark.asyncio
    async def test_not_retrieve_advisor_with_account_token(self):
        """Should not retrieve expert advisor from API with account token."""
        domain_client.token = account_token
        expert_advisor_client = ExpertAdvisorClient(http_client, domain_client)
        try:
            await expert_advisor_client.get_expert_advisor('id', 'my-ea')
            pytest.fail()
        except Exception as err:
            assert err.__str__() == 'You can not invoke get_expert_advisor method, because you ' + \
                   'have connected with account access token. Please use API access token from ' + \
                   'https://app.metaapi.cloud/token page to invoke this method.'

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete(self):
        """Should delete expert advisor via API."""
        rsps = respx.delete(f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea') \
            .mock(return_value=Response(200))
        await expert_advisor_client.delete_expert_advisor('id', 'my-ea')
        assert rsps.calls[0].request.url == \
               f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'

    @pytest.mark.asyncio
    async def test_not_delete_advisor_with_account_token(self):
        """Should not delete expert advisor from API with account token."""
        domain_client.token = account_token
        expert_advisor_client = ExpertAdvisorClient(http_client, domain_client)
        try:
            await expert_advisor_client.delete_expert_advisor('id', 'my-ea')
            pytest.fail()
        except Exception as err:
            assert err.__str__() == 'You can not invoke delete_expert_advisor method, because you ' + \
                    'have connected with account access token. Please use API access token from ' + \
                   'https://app.metaapi.cloud/token page to invoke this method.'

    @respx.mock
    @pytest.mark.asyncio
    async def test_update(self):
        """Should update expert advisor via API."""
        advisor = {
            'preset': 'a2V5MT12YWx1ZTEKa2V5Mj12YWx1ZTIKa2V5Mz12YWx1ZTMKc3VwZXI9dHJ1ZQ==',
            'period': '15m',
            'symbol': 'EURUSD'
        }
        rsps = respx.put(f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea') \
            .mock(return_value=Response(200))
        await expert_advisor_client.update_expert_advisor('id', 'my-ea', advisor)
        assert rsps.calls[0].request.url == \
               f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert rsps.calls[0].request.content == json.dumps(advisor).encode('utf-8')

    @pytest.mark.asyncio
    async def test_not_update_advisor_with_account_token(self):
        """Should not update expert advisor via API with account token."""
        advisor = {
            'preset': 'a2V5MT12YWx1ZTEKa2V5Mj12YWx1ZTIKa2V5Mz12YWx1ZTMKc3VwZXI9dHJ1ZQ==',
            'period': '15m',
            'symbol': 'EURUSD'
        }
        domain_client.token = account_token
        expert_advisor_client = ExpertAdvisorClient(http_client, domain_client)
        try:
            await expert_advisor_client.update_expert_advisor('id', 'my-ea', advisor)
            pytest.fail()
        except Exception as err:
            assert err.__str__() == 'You can not invoke update_expert_advisor method, because you ' + \
                   'have connected with account access token. Please use API access token from ' + \
                   'https://app.metaapi.cloud/token page to invoke this method.'

    @respx.mock
    @pytest.mark.asyncio
    async def test_upload_file(self):
        """Should upload file to an expert advisor via API."""
        rsps = respx.put(f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea/file') \
            .mock(return_value=Response(204))
        with patch('__main__.open', new=mock_open(read_data='test')) as file:
            file.return_value = json.dumps('test').encode()
            await expert_advisor_client.upload_expert_advisor_file('id', 'my-ea', file())
            assert rsps.calls[0].request.url == \
                   f'{PROVISIONING_API_URL}/users/current/accounts/id/expert-advisors/my-ea/file'
            assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'

    @pytest.mark.asyncio
    async def test_not_upload_file_with_account_token(self):
        """Should not upload file to an expert advisor via API with account token."""
        with patch('__main__.open', new=mock_open(read_data='test')) as file:
            domain_client.token = account_token
            expert_advisor_client = ExpertAdvisorClient(http_client, domain_client)
            try:
                await expert_advisor_client.upload_expert_advisor_file('id', 'my-ea', file())
                pytest.fail()
            except Exception as err:
                assert err.__str__() == 'You can not invoke upload_expert_advisor_file method, because you ' + \
                       'have connected with account access token. Please use API access token from ' + \
                       'https://app.metaapi.cloud/token page to invoke this method.'
