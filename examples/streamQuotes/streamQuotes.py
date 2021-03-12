import os
import asyncio
from metaapi_cloud_sdk import MetaApi
from metaapi_cloud_sdk.clients.metaApi.synchronizationListener import SynchronizationListener
from metaapi_cloud_sdk.metaApi.models import MetatraderSymbolPrice

token = os.getenv('TOKEN') or '<put in your token here>'
account_id = os.getenv('ACCOUNT_ID') or '<put in your account id here>'
symbol = os.getenv('SYMBOL') or 'EURUSD'


class QuoteListener(SynchronizationListener):
    async def on_symbol_price_updated(self, instance_index: int, price: MetatraderSymbolPrice):
        if price['symbol'] == symbol:
            print(symbol + ' price updated', price)


async def stream_quotes():
    api = MetaApi(token)
    try:
        account = await api.metatrader_account_api.get_account(account_id)

        #  wait until account is deployed and connected to broker
        print('Deploying account')
        if account.state != 'DEPLOYED':
            await account.deploy()
        else:
            print('Account already deployed')
        print('Waiting for API server to connect to broker (may take couple of minutes)')
        await account.wait_connected()

        # connect to MetaApi API
        connection = await account.connect()

        quote_listener = QuoteListener()
        connection.add_synchronization_listener(quote_listener)

        # wait until terminal state synchronized to the local state
        print('Waiting for SDK to synchronize to terminal state (may take some time depending on your history size), the price streaming will start once synchronization finishes')
        await connection.wait_synchronized({'timeoutInSeconds': 1200})

        # Add symbol to MarketWatch if not yet added
        await connection.subscribe_to_market_data(symbol)

        print('Streaming ' + symbol + ' quotes now...')

        while True:
            await asyncio.sleep(1)

    except Exception as err:
        print(api.format_error(err))

asyncio.run(stream_quotes())
