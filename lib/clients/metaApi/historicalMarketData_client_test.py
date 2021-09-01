from ..httpClient import HttpClient
from .historicalMarketData_client import HistoricalMarketDataClient
from ...metaApi.models import date
import pytest
import respx
from httpx import Response
market_data_client_api_url = 'https://mt-market-data-client-api-v1.agiliumtrade.agiliumtrade.ai'
http_client = HttpClient()
client = HistoricalMarketDataClient(http_client, 'header.payload.sign')


@pytest.fixture(autouse=True)
async def run_around_tests():
    global http_client
    global client
    http_client = HttpClient()
    client = HistoricalMarketDataClient(http_client, 'header.payload.sign')


class TestHistoricalMarketDataClient:

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_candles(self):
        """Should download historical candles from API."""
        expected = [{
            'symbol': 'AUDNZD',
            'timeframe': '15m',
            'time': '2020-04-07T03:45:00.000Z',
            'brokerTime': '2020-04-07 06:45:00.000',
            'open': 1.03297,
            'high': 1.06309,
            'low': 1.02705,
            'close': 1.043,
            'tickVolume': 1435,
            'spread': 17,
            'volume': 345
        }]

        rsps = respx.get(f'{market_data_client_api_url}/users/current/accounts/accountId/historical-market-data/'
                         'symbols/AUDNZD/timeframes/15m/candles').mock(return_value=Response(200, json=expected))
        candles = await client.get_historical_candles('accountId', 'AUDNZD', '15m', date('2020-04-07T03:45:00.000Z'), 1)
        expected[0]['time'] = date(expected[0]['time'])
        assert rsps.calls[0].request.url == f'{market_data_client_api_url}/users/current/accounts/accountId/' \
                                            'historical-market-data/symbols/AUDNZD/timeframes/15m/candles' \
                                            '?startTime=2020-04-07T03%3A45%3A00.000Z&limit=1'
        assert rsps.calls[0].request.method == 'GET'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert candles == expected

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_candles_with_special_characters(self):
        """Should download historical candles from API for symbol with special characters."""
        expected = [{
            'symbol': 'GBPJPY#',
            'timeframe': '15m',
            'time': '2020-04-07T03:45:00.000Z',
            'brokerTime': '2020-04-07 06:45:00.000',
            'open': 1.03297,
            'high': 1.06309,
            'low': 1.02705,
            'close': 1.043,
            'tickVolume': 1435,
            'spread': 17,
            'volume': 345
        }]

        rsps = respx.get().mock(return_value=Response(200, json=expected))
        candles = await client.get_historical_candles('accountId', 'GBPJPY#', '15m',
                                                      date('2020-04-07T03:45:00.000Z'), 1)
        expected[0]['time'] = date(expected[0]['time'])
        assert rsps.calls[0].request.url == f'{market_data_client_api_url}/users/current/accounts/accountId/' \
                                            'historical-market-data/symbols/GBPJPY%23/timeframes/15m/candles' \
                                            '?startTime=2020-04-07T03%3A45%3A00.000Z&limit=1'
        assert rsps.calls[0].request.method == 'GET'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert candles == expected

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_ticks(self):
        """Should download historical ticks from API."""
        expected = [{
            'symbol': 'AUDNZD',
            'time': '2020-04-07T03:45:00.000Z',
            'brokerTime': '2020-04-07 06:45:00.000',
            'bid': 1.05297,
            'ask': 1.05309,
            'last': 0.5298,
            'volume': 0.13,
            'side': 'buy'
        }]

        rsps = respx.get(f'{market_data_client_api_url}/users/current/accounts/accountId/historical-market-data/'
                         'symbols/AUDNZD/ticks').mock(return_value=Response(200, json=expected))
        ticks = await client.get_historical_ticks('accountId', 'AUDNZD', date('2020-04-07T03:45:00.000Z'), 0, 1)
        expected[0]['time'] = date(expected[0]['time'])
        assert rsps.calls[0].request.url == f'{market_data_client_api_url}/users/current/accounts/accountId/' \
                                            'historical-market-data/symbols/AUDNZD/ticks' \
                                            '?startTime=2020-04-07T03%3A45%3A00.000Z&offset=0&limit=1'
        assert rsps.calls[0].request.method == 'GET'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert ticks == expected

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_ticks_with_special_characters(self):
        """Should download historical ticks from API."""
        expected = [{
            'symbol': 'GBPJPY#',
            'time': '2020-04-07T03:45:00.000Z',
            'brokerTime': '2020-04-07 06:45:00.000',
            'bid': 1.05297,
            'ask': 1.05309,
            'last': 0.5298,
            'volume': 0.13,
            'side': 'buy'
        }]

        rsps = respx.get().mock(return_value=Response(200, json=expected))
        ticks = await client.get_historical_ticks('accountId', 'GBPJPY#', date('2020-04-07T03:45:00.000Z'), 0, 1)
        expected[0]['time'] = date(expected[0]['time'])
        assert rsps.calls[0].request.url == f'{market_data_client_api_url}/users/current/accounts/accountId/' \
                                            'historical-market-data/symbols/GBPJPY%23/ticks' \
                                            '?startTime=2020-04-07T03%3A45%3A00.000Z&offset=0&limit=1'
        assert rsps.calls[0].request.method == 'GET'
        assert rsps.calls[0].request.headers['auth-token'] == 'header.payload.sign'
        assert ticks == expected
