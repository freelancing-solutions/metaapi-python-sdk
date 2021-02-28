metaapi.cloud SDK for Python
############################

MetaApi is a powerful, fast, cost-efficient, easy to use and standards-driven cloud forex trading API for MetaTrader 4 and MetaTrader 5 platform designed for traders, investors and forex application developers to boost forex application development process. MetaApi can be used with any broker and does not require you to be a brokerage.

CopyFactory is a simple yet powerful copy-trading API which is a part of MetaApi. See below for CopyFactory readme section.

MetaApi is a paid service, but API access to one MetaTrader account is free of charge.

The `MetaApi pricing <https://metaapi.cloud/#pricing>`_ was developed with the intent to make your charges less or equal to what you would have to pay
for hosting your own infrastructure. This is possible because over time we managed to heavily optimize
our MetaTrader infrastructure. And with MetaApi you can save significantly on application development and
maintenance costs and time thanks to high-quality API, open-source SDKs and convenience of a cloud service.

Official REST and websocket API documentation: https://metaapi.cloud/docs/client/

Please note that this SDK provides an abstraction over REST and websocket API to simplify your application logic.

For more information about SDK APIs please check docstring documentation in source codes located inside lib folder of this package.

Installation
============
.. code-block:: bash

    pip install metaapi-cloud-sdk

Working code examples
=====================
Please check `this short video <https://youtu.be/LIqFOOOLP-g>`_ to see how you can download samples via our web application.

You can find code examples at `examples folder of our github repo <https://github.com/agiliumtrade-ai/metaapi-python-sdk/tree/master/examples>`_ or in the examples folder of the pip package.

We have composed a `short guide explaining how to use the example code <https://metaapi.cloud/docs/client/usingCodeExamples>`_

Connecting to MetaApi
=====================
Please use one of these ways:

1. https://app.metaapi.cloud/token web UI to obtain your API token.
2. An account access token which grants access to a single account. See section below on instructions on how to retrieve account access token.

Supply token to the MetaApi class constructor.

.. code-block:: python

    from metaapi_cloud_sdk import MetaApi

    token = '...'
    api = MetaApi(token=token)

Retrieving account access token
===============================
Account access token grants access to a single account. You can retrieve account access token via API:

.. code-block:: python

    account_id = '...'
    account = await api.metatrader_account_api.get_account(account_id=account_id)
    account_access_token = account.access_token
    print(account_access_token)

Alternatively, you can retrieve account access token via web UI on https://app.metaapi.cloud/accounts page (see `this video <https://youtu.be/PKYiDns6_xI>`_).

Managing MetaTrader accounts (API servers for MT accounts)
==========================================================
Before you can use the API you have to add an MT account to MetaApi and start an API server for it.

However, before you can create an account, you have to create a provisioning profile.

Managing provisioning profiles via web UI
-----------------------------------------
You can manage provisioning profiles here: https://app.metaapi.cloud/provisioning-profiles

Creating a provisioning profile via API
---------------------------------------
.. code-block:: python

    # if you do not have created a provisioning profile for your broker,
    # you should do it before creating an account
    provisioningProfile = await api.provisioning_profile_api.create_provisioning_profile(profile={
        'name': 'My profile',
        'version': 5,
        'brokerTimezone': 'EET',
        'brokerDSTSwitchTimezone': 'EET'
    })
    # servers.dat file is required for MT5 profile and can be found inside
    # config directory of your MetaTrader terminal data folder. It contains
    # information about available broker servers
    await provisioningProfile.upload_file(file_name='servers.dat', file='/path/to/servers.dat')
    # for MT4, you should upload an .srv file instead
    await provisioningProfile.upload_file(file_name='broker.srv', file='/path/to/broker.srv')

Retrieving existing provisioning profiles via API
-------------------------------------------------
.. code-block:: python

    provisioningProfiles = await api.provisioning_profile_api.get_provisioning_profiles()
    provisioningProfile = await api.provisioning_profile_api.get_provisioning_profile(provisioning_profile_id='profileId')

Updating a provisioning profile via API
---------------------------------------
.. code-block:: python

    await provisioningProfile.update(profile={'name': 'New name'})
    # for MT5, you should upload a servers.dat file
    await provisioningProfile.upload_file(file_name='servers.dat', file='/path/to/servers.dat')
    # for MT4, you should upload an .srv file instead
    await provisioningProfile.upload_file(file_name='broker.srv', file='/path/to/broker.srv')

Removing a provisioning profile
-------------------------------
.. code-block:: python

    await provisioningProfile.remove()

Managing MetaTrader accounts (API servers) via web UI
-----------------------------------------------------
You can manage MetaTrader accounts here: https://app.metaapi.cloud/accounts

Create a MetaTrader account (API server) via API
------------------------------------------------
.. code-block:: python

    account = await api.metatrader_account_api.create_account(account={
      'name': 'Trading account #1',
      'type': 'cloud',
      'login': '1234567',
      # password can be investor password for read-only access
      'password': 'qwerty',
      'server': 'ICMarketsSC-Demo',
      'provisioningProfileId': provisioningProfile.id,
      'application': 'MetaApi',
      'magic': 123456,
      'quoteStreamingIntervalInSeconds': 2.5, # set to 0 to receive quote per tick
      'reliability': 'regular' # set this field to 'high' value if you want to increase uptime of your account (recommended for production environments)
    })

Retrieving existing accounts via API
------------------------------------
.. code-block:: python

    # filter and paginate accounts, see doc for full list of filter options available
    accounts = await api.metatrader_account_api.get_accounts(accounts_filter={
        'limit': 10,
        'offset': 0,
        'query': 'ICMarketsSC-MT5',
        'state': ['DEPLOYED']
    })
    # get accounts without filter (returns 1000 accounts max)
    accounts = await api.metatrader_account_api.get_accounts();

    account = await api.metatrader_account_api.get_account(account_id='accountId')

Updating an existing account via API
------------------------------------
.. code-block:: python

    await account.update(account={
        'name': 'Trading account #1',
        'login': '1234567',
        # password can be investor password for read-only access
        'password': 'qwerty',
        'server': 'ICMarketsSC-Demo',
        'quoteStreamingIntervalInSeconds': 2.5
    })

Removing an account
-------------------
.. code-block:: python

    await account.remove()

Deploying, undeploying and redeploying an account (API server) via API
----------------------------------------------------------------------
.. code-block:: python

    await account.deploy()
    await account.undeploy()
    await account.redeploy()

Access MetaTrader account via RPC API
=====================================
RPC API let you query the trading terminal state. You should use
RPC API if you develop trading monitoring apps like myfxbook or other
simple trading apps.

Query account information, positions, orders and history via RPC API
--------------------------------------------------------------------
.. code-block:: python

    connection = await account.connect()

    await connection.wait_synchronized()

    # retrieve balance and equity
    print(await connection.get_account_information())
    # retrieve open positions
    print(await connection.get_positions())
    # retrieve a position by id
    print(await connection.get_position(position_id='1234567'))
    # retrieve pending orders
    print(await connection.get_orders())
    # retrieve a pending order by id
    print(await connection.get_order(order_id='1234567'))
    # retrieve history orders by ticket
    print(await connection.get_history_orders_by_ticket(ticket='1234567'))
    # retrieve history orders by position id
    print(await connection.get_history_orders_by_position(position_id='1234567'))
    # retrieve history orders by time range
    print(await connection.get_history_orders_by_time_range(start_time=start_time, end_time=end_time))
    # retrieve history deals by ticket
    print(await connection.get_deals_by_ticket(ticket='1234567'))
    # retrieve history deals by position id
    print(await connection.get_deals_by_position(position_id='1234567'))
    # retrieve history deals by time range
    print(await connection.get_deals_by_time_range(start_time=start_time, end_time=end_time))

Query contract specifications and quotes via RPC API
----------------------------------------------------
.. code-block:: python

    connection = await account.connect()

    await connection.wait_synchronized()

    # first, subscribe to market data
    await connection.subscribe_to_market_data(symbol='GBPUSD')

    # read contract specification
    print(await connection.get_symbol_specification(symbol='GBPUSD'))
    # read current price
    print(await connection.get_symbol_price(symbol='GBPUSD'))

    # unsubscribe from market data when no longer needed
    await connection.unsubscribe_from_market_data(symbol='GBPUSD')

Use real-time streaming API
---------------------------
Real-time streaming API is good for developing trading applications like trade copiers or automated trading strategies.
The API synchronizes the terminal state locally so that you can query local copy of the terminal state really fast.

Synchronizing and reading terminal state
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: python

    account = await api.metatrader_account_api.get_account(account_id='accountId')


    # access local copy of terminal state
    terminalState = connection.terminal_state

    # wait until synchronization completed
    await connection.wait_synchronized()

    print(terminalState.connected)
    print(terminalState.connected_to_broker)
    print(terminalState.account_information)
    print(terminalState.positions)
    print(terminalState.orders)
    # symbol specifications
    print(terminalState.specifications)
    print(terminalState.specification(symbol='EURUSD'))
    print(terminalState.price(symbol='EURUSD'))

    # access history storage
    historyStorage = connection.history_storage

    # both orderSynchronizationFinished and dealSynchronizationFinished
    # should be true once history synchronization have finished
    print(historyStorage.order_synchronization_finished)
    print(historyStorage.deal_synchronization_finished)

Overriding local history storage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default history is stored in memory only. You can override history storage to save trade history to a persistent storage like MongoDB database.

.. code-block:: python

    from metaapi_cloud_sdk import HistoryStorage

    class MongodbHistoryStorage(HistoryStorage):
        # implement the abstract methods, see MemoryHistoryStorage for sample
        # implementation

    historyStorage = MongodbHistoryStorage()

    # Note: if you will not specify history storage, then in-memory storage
    # will be used (instance of MemoryHistoryStorage)
    connection = await account.connect(history_storage=historyStorage)

    # access history storage
    historyStorage = connection.history_storage;

    # invoke other methods provided by your history storage implementation
    print(await historyStorage.yourMethod())

Receiving synchronization events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
You can override SynchronizationListener in order to receive synchronization event notifications, such as account/position/order/history updates or symbol quote updates.

.. code-block:: python

    from metaapi_cloud_sdk import SynchronizationListener

    # receive synchronization event notifications
    # first, implement your listener
    class MySynchronizationListener(SynchronizationListener):
        # override abstract methods you want to receive notifications for

    # now add the listener
    listener = MySynchronizationListener()
    connection.add_synchronization_listener(listener=listener)

    # remove the listener when no longer needed
    connection.remove_synchronization_listener(listener=listener)

Retrieve contract specifications and quotes via streaming API
-------------------------------------------------------------
.. code-block:: python

    connection = await account.connect()

    await connection.wait_synchronized()

    # first, subscribe to market data
    await connection.subscribe_to_market_data(symbol='GBPUSD')

    # read contract specification
    print(terminalState.specification(symbol='EURUSD'))

    # read current price
    print(terminalState.price(symbol='EURUSD'))

    # unsubscribe from market data when no longer needed
    await connection.unsubscribe_from_market_data(symbol='GBPUSD')

Execute trades (both RPC and streaming APIs)
--------------------------------------------
.. code-block:: python

    connection = await account.connect()

    await connection.wait_synchronized()

    # trade
    print(await connection.create_market_buy_order(symbol='GBPUSD', volume=0.07, stop_loss=0.9, take_profit=2.0,
        options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.create_market_sell_order(symbol='GBPUSD', volume=0.07, stop_loss=2.0, take_profit=0.9,
        options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.create_limit_buy_order(symbol='GBPUSD', volume=0.07, open_price=1.0, stop_loss=0.9,
        take_profit=2.0, options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.create_limit_sell_order(symbol='GBPUSD', volume=0.07, open_price=1.5, stop_loss=2.0,
        take_profit=0.9, options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.create_stop_buy_order(symbol='GBPUSD', volume=0.07, open_price=1.5, stop_loss=2.0,
        take_profit=0.9, options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.create_stop_sell_order(symbol='GBPUSD', volume=0.07, open_price=1.0, stop_loss=2.0,
        take_profit=0.9, options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.create_stop_limit_buy_order(symbol='GBPUSD', volume=0.07, open_price=1.5,
        stop_limit_price=1.4, stop_loss=0.9, take_profit=2.0, options={'comment': 'comment',
        'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.create_stop_limit_sell_order(symbol='GBPUSD', volume=0.07, open_price=1.0,
        stop_limit_price=1.1, stop_loss=2.0, take_profit=0.9, options={'comment': 'comment',
        'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print(await connection.modify_position(position_id='46870472', stop_loss=2.0, take_profit=0.9))
    print(await connection.close_position_partially(position_id='46870472', volume=0.9))
    print(await connection.close_position(position_id='46870472'))
    print(await connection.close_by(position_id='46870472', opposite_position_id='46870482'))
    print(await connection.close_positions_by_symbol(symbol='EURUSD'))
    print(await connection.modify_order(order_id='46870472', open_price=1.0, stop_loss=2.0, take_profit=0.9))
    print(await connection.cancel_order(order_id='46870472'))

    # if you need to, check the extra result information in stringCode and numericCode properties of the response
    result = await connection.create_market_buy_order(symbol='GBPUSD', volume=0.07, stop_loss=0.9, take_profit=2.0,
        options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'}))
    print('Trade successful, result code is ' + result['stringCode'])

    # catch and output exception
    try:
        await connection.create_market_buy_order(symbol='GBPUSD', volume=0.07, stop_loss=0.9, take_profit=2.0,
            options={'comment': 'comment', 'clientId': 'TE_GBPUSD_7hyINWqAl'})
    except Exception as err:
        print(api.format_error(err))

Monitoring account connection health and uptime
===============================================
You can monitor account connection health using MetaApiConnection.health_monitor API.

.. code-block:: python

    monitor = connection.health_monitor
    # retrieve server-side app health status
    print(monitor.server_health_status)
    # retrieve detailed connection health status
    print(monitor.health_status)
    # retrieve account connection update measured over last 7 days
    print(monitor.uptime)

Tracking latencies
==================
You can track latencies using MetaApi.latency_monitor API. Client-side latencies include network communication delays, thus the lowest client-side latencies are achieved if you host your app in AWS Ohio region.

.. code-block:: python

    api = MetaApi('token', {'enableLatencyMonitor': True})
    monitor = api.latency_monitor
    # retrieve trade latency stats
    print(monitor.trade_latencies)
    # retrieve update streaming latency stats
    print(monitor.update_latencies)
    # retrieve quote streaming latency stats
    print(monitor.price_latencies)
    # retrieve request latency stats
    print(monitor.request_latencies)

Managing MetaTrader demo accounts via API
=========================================
Please note that not all MT4/MT5 servers allows you to create demo accounts using the method below.

Create a MetaTrader 4 demo account
----------------------------------
.. code-block:: python

    demo_account = await api.metatrader_demo_account_api.create_mt4_demo_account(profile_id=provisioningProfile.id,
        account={
            'balance': 100000,
            'email': 'example@example.com',
            'leverage': 100,
            'serverName': 'Exness-Trial4'
        })

Create a MetaTrader 5 demo account
----------------------------------
.. code-block:: python

    demo_account = await api.metatrader_demo_account_api.create_mt5_demo_account((profile_id=provisioningProfile.id,
        account={
            'balance': 100000,
            'email': 'example@example.com',
            'leverage': 100,
            'serverName': 'ICMarketsSC-Demo'
        })

CopyFactory copy trading API
===========================================

CopyFactory is a powerful trade copying API which makes developing forex
trade copying applications as easy as writing few lines of code.

At this point we have not yet defined a final price for this feature.

Why do we offer CopyFactory API
-------------------------------

We found that developing reliable and flexible trade copier is a task
which requires lots of effort, because developers have to solve a series
of complex technical tasks to create a product.

We decided to share our product as it allows developers to start with a
powerful solution in almost no time, saving on development and
infrastructure maintenance costs.

CopyFactory features
--------------------
Features supported:

- low latency trade copying
- reliable copy trading execution
- connect arbitrary number of strategy providers and subscribers
- subscribe accounts to multiple strategies at once
- select arbitrary copy ratio for each subscription
- configure symbol mapping between strategy providers and subscribers
- apply advanced risk filters on strategy provider side
- override risk filters on subscriber side
- provide multiple strategies from a single account based on magic or symbol filters
- reliable trade copying
- supports manual trading on subscriber accounts while copying trades
- synchronize subscriber account with strategy providers
- monitor trading history
- calculate trade copying commissions for account managers
- support portfolio strategies as trading signal source, i.e. the strategies which include signals of several other strategies (also known as combos on some platforms)

Please note that trade copying to MT5 netting accounts is not supported in the current API version

Configuring trade copying
-------------------------

In order to configure trade copying you need to:

- add MetaApi MetaTrader accounts with CopyFactory as application field value (see above)
- create CopyFactory master and slave accounts and connect them to MetaApi accounts via connectionId field
- create a strategy being copied
- subscribe slave CopyFactory accounts to the strategy

.. code-block:: python

    from metaapi_cloud_sdk import MetaApi, CopyFactory

    token = '...'
    metaapi = MetaApi(token=token)
    copy_factory = CopyFactory(token=token)

    # retrieve MetaApi MetaTrader accounts with CopyFactory as application field value
    master_metaapi_account = await metaapi.metatrader_account_api.get_account(account_id='masterMetaapiAccountId')
    if master_metaapi_account.application != 'CopyFactory'
        raise Exception('Please specify CopyFactory application field value in your MetaApi account in order to use it in CopyFactory API')
    slave_metaapi_account = await metaapi.metatrader_account_api.get_account(account_id='slaveMetaapiAccountId')
    if slave_metaapi_account.application != 'CopyFactory'
        raise Exception('Please specify CopyFactory application field value in your MetaApi account in order to use it in CopyFactory API')

    # create CopyFactory master and slave accounts and connect them to MetaApi accounts via connectionId field
    configuration_api = copy_factory.configuration_api
    master_account_id = configuration_api.generate_account_id()
    slave_account_id = configuration_api.generate_account_id()
    await configuration_api.update_account(id=master_account_id, account={
        'name': 'Demo account',
        'connectionId': master_metaapi_account.id,
        'subscriptions': []
    })

    # create a strategy being copied
    strategy_id = await configuration_api.generate_strategy_id()
    await configuration_api.update_strategy(id=strategy_id['id'], strategy={
        'name': 'Test strategy',
        'description': 'Some useful description about your strategy',
        'positionLifecycle': 'hedging',
        'connectionId': master_metaapi_account.id,
        'maxTradeRisk': 0.1,
        'stopOutRisk': {
            'value': 0.4,
            'startTime': '2020-08-24T00:00:00.000Z'
        },
        'timeSettings': {
            'lifetimeInHours': 192,
            'openingIntervalInMinutes': 5
        }
    })

    # subscribe slave CopyFactory accounts to the strategy
    await configuration_api.update_account(id=slave_account_id, account={
        'name': 'Demo account',
        'connectionId': slave_metaapi_account.id,
        'subscriptions': [
            {
                'strategyId': strategy_id['id'],
                'multiplier': 1
            }
        ]
    })

See in-code documentation for full definition of possible configuration options.

Retrieving trade copying history
--------------------------------

CopyFactory allows you to monitor transactions conducted on trading accounts in real time.

Retrieving trading history on provider side
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    history_api = copy_factory.history_api

    # retrieve list of subscribers
    print(await history_api.get_subscribers())

    # retrieve list of strategies provided
    print(await history_api.get_provided_strategies())

    # retrieve trading history, please note that this method support pagination and limits number of records
    print(await history_api.get_provided_strategies_transactions(time_from=datetime.fromisoformat('2020-08-01'),
        time_till=datetime.fromisoformat('2020-09-01')))


Retrieving trading history on subscriber side
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    history_api = copy_factory.history_api

    # retrieve list of providers
    print(await history_api.get_providers())

    # retrieve list of strategies subscribed to
    print(await history_api.get_strategies_subscribed())

    # retrieve trading history, please note that this method support pagination and limits number of records
    print(await history_api.get_strategies_subscribed_transactions(time_from=datetime.fromisoformat('2020-08-01'),
        time_till=datetime.fromisoformat('2020-09-01')))

Resynchronizing slave accounts to masters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
There is a configurable time limit during which the trades can be opened. Sometimes trades can not open in time due to broker errors or trading session time discrepancy.
You can resynchronize a slave account to place such late trades. Please note that positions which were
closed manually on a slave account will also be reopened during resynchronization.

.. code-block:: python

    account_id = '...' # CopyFactory account id

    # resynchronize all strategies
    await copy_factory.trading_api.resynchronize(account_id=account_id)

    # resynchronize specific strategy
    await copy_factory.trading_api.resynchronize(account_id=account_id, strategy_ids=['ABCD'])

Managing stopouts
^^^^^^^^^^^^^^^^^
A subscription to a strategy can be stopped if the strategy have exceeded allowed risk limit.

.. code-block:: python

    trading_api = copy_factory.trading_api
    account_id = '...' # CopyFactory account id
    strategy_id = '...' # CopyFactory strategy id

    # retrieve list of strategy stopouts
    print(await trading_api.get_stopouts(account_id=account_id))

    # reset a stopout so that subscription can continue
    await trading_api.reset_stopout(account_id=account_id, strategy_id=strategy_id, reason='daily-equity')

Retrieving slave trading logs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    trading_api = copy_factory.trading_api
    account_id = '...' # CopyFactory account id

    # retrieve slave trading log
    print(await trading_api.get_user_log(account_id))

    # retrieve paginated slave trading log by time range
    print(await trading_api.get_user_log(account_id, datetime.fromtimestamp(datetime.now().timestamp() - 24 * 60 * 60), None, 20, 10))

Keywords: MetaTrader API, MetaTrader REST API, MetaTrader websocket API,
MetaTrader 5 API, MetaTrader 5 REST API, MetaTrader 5 websocket API,
MetaTrader 4 API, MetaTrader 4 REST API, MetaTrader 4 websocket API,
MT5 API, MT5 REST API, MT5 websocket API, MT4 API, MT4 REST API,
MT4 websocket API, MetaTrader SDK, MetaTrader SDK, MT4 SDK, MT5 SDK,
MetaTrader 5 SDK, MetaTrader 4 SDK, MetaTrader python SDK, MetaTrader 5
python SDK, MetaTrader 4 python SDK, MT5 python SDK, MT4 python SDK,
FX REST API, Forex REST API, Forex websocket API, FX websocket API, FX
SDK, Forex SDK, FX python SDK, Forex python SDK, Trading API, Forex
API, FX API, Trading SDK, Trading REST API, Trading websocket API,
Trading SDK, Trading python SDK
