from ..domain_client import DomainClient
from .equityTracking_client_model import EquityTrackingClientModel
from ...models import random_id, date
from .... import MetaApi
from ....logger import LoggerManager
from ....clients.metaApi.synchronizationListener import SynchronizationListener
from ....metaApi.streamingMetaApiConnectionInstance import StreamingMetaApiConnectionInstance
from .periodStatisticsListener import PeriodStatisticsListener
from typing import List, Dict
from asyncio import Future
from datetime import datetime
from functools import reduce
import asyncio
import json


class PeriodStatisticsStreamManager:
    """Manager for handling period statistics event listeners."""

    def __init__(self, domain_client: DomainClient, equity_tracking_client: EquityTrackingClientModel,
                 meta_api: MetaApi):
        """Constructs period statistics event listener manager instance.

        Args:
            domain_client: Domain client.
            equity_tracking_client: Equity tracking client.
            meta_api: MetaApi SDK instance.
        """
        self._domainClient = domain_client
        self._equityTrackingClient = equity_tracking_client
        self._metaApi = meta_api
        self._periodStatisticsListeners = {}
        self._accountsByListenerId: Dict[str, str] = {}
        self._periodStatisticsConnections: Dict[str, StreamingMetaApiConnectionInstance] = {}
        self._periodStatisticsCaches = {}
        self._accountSynchronizationFlags: Dict[str, bool] = {}
        self._pendingInitalizationResolves: Dict[str, List[Future]] = {}
        self._retryIntervalInSeconds = 1
        self._logger = LoggerManager.get_logger('PeriodStatisticsStreamManager')

    def get_account_listeners(self, account_id: str) -> Dict[str, PeriodStatisticsListener]:
        """Returns listeners for account.

        Args:
            account_id: Account id to return listeners for.

        Returns:
            Dictionary of account equity chart event listeners.
        """
        if account_id not in self._periodStatisticsListeners:
            self._periodStatisticsListeners[account_id] = {}
        return self._periodStatisticsListeners[account_id]

    async def add_period_statistics_listener(self, listener: PeriodStatisticsListener, account_id: str,
                                             tracker_id: str):
        """Adds a period statistics event listener.

        Args:
            listener: Period statistics event listener.
            account_id: Account id.
            tracker_id: Tracker id.

        Returns:
            Listener id.
        """
        if account_id not in self._periodStatisticsCaches:
            self._periodStatisticsCaches[account_id] = {
                'trackerData': {},
                'record': {},
                'equityAdjustments': {},
                'lastPeriod': None
            }
        cache = self._periodStatisticsCaches[account_id]
        connection: StreamingMetaApiConnectionInstance = None
        retry_interval_in_seconds = self._retryIntervalInSeconds
        equity_tracking_client = self._equityTrackingClient
        listener_id = random_id(10)

        def remove_period_statistics_listener():
            return self.remove_period_statistics_listener(listener_id)

        def get_account_listeners():
            return self.get_account_listeners(account_id)

        pending_initialization_resolves = self._pendingInitalizationResolves
        synchronization_flags = self._accountSynchronizationFlags

        class PeriodStatisticsStreamListener(SynchronizationListener):

            async def on_deals_synchronized(self, instance_index: str, synchronization_id: str):
                if account_id not in synchronization_flags or not synchronization_flags[account_id]:
                    synchronization_flags[account_id] = True
                    for account_listener in get_account_listeners().values():
                        asyncio.create_task(account_listener.on_connected())
                if account_id in pending_initialization_resolves:
                    for promise in pending_initialization_resolves[account_id]:
                        promise.set_result(True)
                        del pending_initialization_resolves[account_id]

            async def on_disconnected(self, instance_index: str):
                if account_id in synchronization_flags and not connection.health_monitor.health_status['synchronized']:
                    synchronization_flags[account_id] = False
                    for account_listener in get_account_listeners().values():
                        asyncio.create_task(account_listener.on_disconnected())

            async def on_symbol_price_updated(self, instance_index: str, price):
                if account_id in pending_initialization_resolves:
                    for promise in pending_initialization_resolves[account_id]:
                        promise.set_result(True)
                        del pending_initialization_resolves[account_id]

                if not cache['lastPeriod']:
                    return

                """Process brokerTime:
                - smaller than tracker startBrokerTime -> ignore
                - bigger than tracker endBrokerTime -> send on_tracker_completed, close connection
                - bigger than period endBrokerTime -> send on_period_statistics_completed
                - normal -> compare to previous data, if different -> send on_period_statistics_updated
                """

                equity = price['equity'] - reduce(lambda a, b: a + b, cache['equityAdjustments'].values(), 0)
                broker_time = price['brokerTime']
                if broker_time > cache['lastPeriod']['endBrokerTime']:
                    asyncio.create_task(listener.on_period_statistics_completed())
                    cache['equityAdjustments'] = {}
                    start_broker_time = cache['lastPeriod']['startBrokerTime']
                    cache['lastPeriod'] = None
                    while True:
                        periods = await equity_tracking_client.get_tracking_statistics(account_id, tracker_id, None, 2)
                        if periods[0]['startBrokerTime'] == start_broker_time:
                            await asyncio.sleep(10)
                        else:
                            cache['lastPeriod'] = periods[0]
                            periods.reverse()
                            asyncio.create_task(listener.on_period_statistics_updated(periods))
                            break
                else:
                    if 'startBrokerTime' in cache['trackerData'] and \
                            broker_time < cache['trackerData']['startBrokerTime']:
                        return

                    if 'endBrokerTime' in cache['trackerData'] and \
                            broker_time > cache['trackerData']['endBrokerTime']:
                        asyncio.create_task(listener.on_tracker_completed())
                        cache['equityAdjustments'] = {}
                        connection.remove_synchronization_listener(self)
                        remove_period_statistics_listener()
                        await connection.close()

                    absolute_drawdown = max(0, cache['lastPeriod']['initialBalance'] - equity)
                    relative_drawdown = absolute_drawdown / cache['lastPeriod']['initialBalance']
                    absolute_profit = max(0, equity - cache['lastPeriod']['initialBalance'])
                    relative_profit = absolute_profit / cache['lastPeriod']['initialBalance']
                    previous_record = json.dumps(cache['record'])
                    if not cache['record']['thresholdExceeded']:
                        if cache['record']['maxAbsoluteDrawdown'] < absolute_drawdown:
                            cache['record']['maxAbsoluteDrawdown'] = absolute_drawdown
                            cache['record']['maxRelativeDrawdown'] = relative_drawdown
                            cache['record']['maxDrawdownTime'] = broker_time
                            if 'relativeDrawdownThreshold' in cache['trackerData'] and \
                                    cache['trackerData']['relativeDrawdownThreshold'] and \
                                    cache['trackerData']['relativeDrawdownThreshold'] < relative_drawdown or \
                                    'absoluteDrawdownThreshold' in cache['trackerData'] and \
                                    cache['trackerData']['absoluteDrawdownThreshold'] and \
                                    cache['trackerData']['absoluteDrawdownThreshold'] < absolute_drawdown:
                                cache['record']['thresholdExceeded'] = True
                                cache['record']['exceededThresholdType'] = 'drawdown'

                        if cache['record']['maxAbsoluteProfit'] < absolute_profit:
                            cache['record']['maxAbsoluteProfit'] = absolute_profit
                            cache['record']['maxRelativeProfit'] = relative_profit
                            cache['record']['maxProfitTime'] = broker_time
                            if 'relativeProfitThreshold' in cache['trackerData'] and \
                                    cache['trackerData']['relativeProfitThreshold'] and \
                                    cache['trackerData']['relativeProfitThreshold'] < relative_profit or \
                                    'absoluteProfitThreshold' in cache['trackerData'] and \
                                    cache['trackerData']['absoluteProfitThreshold'] and \
                                    cache['trackerData']['absoluteProfitThreshold'] < absolute_profit:
                                cache['record']['thresholdExceeded'] = True
                                cache['record']['exceededThresholdType'] = 'profit'
                        if json.dumps(cache['record']) != previous_record:
                            asyncio.create_task(listener.on_period_statistics_updated([{
                                'startBrokerTime': cache['lastPeriod']['startBrokerTime'],
                                'endBrokerTime': cache['lastPeriod']['endBrokerTime'],
                                'initialBalance': cache['lastPeriod']['initialBalance'],
                                'maxAbsoluteDrawdown': cache['record']['maxAbsoluteDrawdown'],
                                'maxAbsoluteProfit': cache['record']['maxAbsoluteProfit'],
                                'maxDrawdownTime': cache['record']['maxDrawdownTime'],
                                'maxProfitTime': cache['record']['maxProfitTime'],
                                'maxRelativeDrawdown': cache['record']['maxRelativeDrawdown'],
                                'maxRelativeProfit': cache['record']['maxRelativeProfit'],
                                'period': cache['lastPeriod']['period'],
                                'exceededThresholdType': cache['record']['exceededThresholdType'],
                                'thresholdExceeded': cache['record']['thresholdExceeded'],
                                'tradeDayCount': cache['record']['tradeDayCount']
                            }]))

            async def on_deal_added(self, instance_index: str, deal):
                if not cache['lastPeriod']:
                    return

                if deal['type'] == 'DEAL_TYPE_BALANCE':
                    cache['equityAdjustments'][deal['id']] = deal['profit']
                ignored_deal_types = ['DEAL_TYPE_BALANCE', 'DEAL_TYPE_CREDIT']
                if deal['type'] not in ignored_deal_types:
                    time_diff = (date(deal['time']).timestamp() - date(deal['brokerTime']).timestamp())
                    start_search_date = datetime.fromtimestamp(date(cache['lastPeriod']['startBrokerTime']).timestamp()
                                                               + time_diff)
                    deals = list(filter(
                        lambda deal_item: deal_item['type'] not in ignored_deal_types,
                        connection.history_storage.get_deals_by_time_range(start_search_date, date(8640000000))))
                    deals.append(deal)
                    traded_days = {}
                    for deal_item in deals:
                        traded_days[deal_item['brokerTime'][0:10]] = True
                    trade_day_count = len(traded_days.keys())
                    if cache['record']['tradeDayCount'] != trade_day_count:
                        cache['record']['tradeDayCount'] = trade_day_count
                        asyncio.create_task(listener.on_period_statistics_updated([{
                            'startBrokerTime': cache['lastPeriod']['startBrokerTime'],
                            'endBrokerTime': cache['lastPeriod']['endBrokerTime'],
                            'initialBalance': cache['lastPeriod']['initialBalance'],
                            'maxAbsoluteDrawdown': cache['record']['maxAbsoluteDrawdown'],
                            'maxAbsoluteProfit': cache['record']['maxAbsoluteProfit'],
                            'maxDrawdownTime': cache['record']['maxDrawdownTime'],
                            'maxProfitTime': cache['record']['maxProfitTime'],
                            'maxRelativeDrawdown': cache['record']['maxRelativeDrawdown'],
                            'maxRelativeProfit': cache['record']['maxRelativeProfit'],
                            'period': cache['lastPeriod']['period'],
                            'exceededThresholdType': cache['record']['exceededThresholdType'],
                            'thresholdExceeded': cache['record']['thresholdExceeded'],
                            'tradeDayCount': cache['record']['tradeDayCount']
                        }]))

        account = await self._metaApi.metatrader_account_api.get_account(account_id)
        tracker = await equity_tracking_client.get_tracker(account_id, tracker_id)
        cache['trackerData'] = tracker
        account_listeners = get_account_listeners()
        account_listeners[listener_id] = listener
        self._accountsByListenerId[listener_id] = account_id
        if account_id not in self._periodStatisticsConnections:
            is_deployed = False
            while not is_deployed:
                try:
                    await account.wait_deployed()
                    is_deployed = True
                except Exception as err:
                    asyncio.create_task(listener.on_error(err))
                    self._logger.error(f'Error wait for account {account_id} to deploy, retrying',
                                       MetaApi.format_error(err))
                    await asyncio.sleep(retry_interval_in_seconds)
                    retry_interval_in_seconds = min(retry_interval_in_seconds * 2, 300)

            retry_interval_in_seconds = self._retryIntervalInSeconds
            connection = account.get_streaming_connection()
            self._periodStatisticsConnections[account_id] = connection
            sync_listener = PeriodStatisticsStreamListener()
            connection.add_synchronization_listener(sync_listener)
            self._periodStatisticsConnections[account_id] = connection

            is_synchronized = False
            while not is_synchronized:
                try:
                    await connection.connect()
                    await connection.wait_synchronized()
                    is_synchronized = True
                except Exception as err:
                    asyncio.create_task(listener.on_error(err))
                    self._logger.error('Error configuring period statistics stream listener ' +
                                       f'for account {account_id}, retrying', MetaApi.format_error(err))
                    await asyncio.sleep(retry_interval_in_seconds)
                    retry_interval_in_seconds = min(retry_interval_in_seconds * 2, 300)

            retry_interval_in_seconds = self._retryIntervalInSeconds
        else:
            connection = self._periodStatisticsConnections[account_id]
            if not connection.health_monitor.health_status['synchronized']:
                if account_id not in self._pendingInitalizationResolves:
                    self._pendingInitalizationResolves[account_id] = []
                initialize_promise = asyncio.Future()
                self._pendingInitalizationResolves[account_id].append(initialize_promise)
                await initialize_promise

        initial_data = []
        while not len(initial_data):
            try:
                initial_data = await equity_tracking_client.get_tracking_statistics(account_id, tracker_id)
                if len(initial_data):
                    last_item = initial_data[0]
                    asyncio.create_task(listener.on_period_statistics_updated(initial_data))
                    cache['lastPeriod'] = {
                        'startBrokerTime': last_item['startBrokerTime'],
                        'endBrokerTime': last_item['endBrokerTime'] if 'endBrokerTime' in last_item else None,
                        'period': last_item['period'],
                        'initialBalance': last_item['initialBalance'],
                        'maxDrawdownTime': last_item['maxDrawdownTime'] if 'maxDrawdownTime' in last_item else None,
                        'maxAbsoluteDrawdown': last_item['maxAbsoluteDrawdown'] if 'maxAbsoluteDrawdown' in
                                                                                   last_item else None,
                        'maxRelativeDrawdown': last_item['maxRelativeDrawdown'] if 'maxRelativeDrawdown' in
                                                                                   last_item else None,
                        'maxProfitTime': last_item['maxProfitTime'] if 'maxProfitTime' in last_item else None,
                        'maxAbsoluteProfit': last_item['maxAbsoluteProfit'] if 'maxAbsoluteProfit' in
                                                                               last_item else None,
                        'maxRelativeProfit': last_item['maxRelativeProfit'] if 'maxRelativeProfit' in
                                                                               last_item else None,
                        'thresholdExceeded': last_item['thresholdExceeded'],
                        'exceededThresholdType': last_item['exceededThresholdType'] if 'exceededThresholdType' in
                                                                                       last_item else None,
                        'tradeDayCount': last_item['tradeDayCount'] if 'tradeDayCount' in last_item else None,
                    }
                    cache['record'] = cache['lastPeriod']
            except Exception as err:
                asyncio.create_task(listener.on_error(err))
                self._logger.error(f'Failed initialize tracking statistics data for account {account_id}',
                                   MetaApi.format_error(err))
                await asyncio.sleep(retry_interval_in_seconds * 1)
                retry_interval_in_seconds = min(retry_interval_in_seconds * 2, 300)
        return listener_id

    def remove_period_statistics_listener(self, listener_id: str):
        """Removes period statistics event listener by id.

        Args:
            listener_id: Listener id.
        """
        if listener_id in self._accountsByListenerId:
            account_id = self._accountsByListenerId[listener_id]
            if listener_id in self._accountsByListenerId:
                del self._accountsByListenerId[listener_id]
            if account_id in self._accountSynchronizationFlags:
                del self._accountSynchronizationFlags[account_id]
            if account_id in self._periodStatisticsListeners:
                del self._periodStatisticsListeners[account_id][listener_id]

            if account_id in self._periodStatisticsConnections and not \
                    len(self._periodStatisticsListeners[account_id].keys()):
                asyncio.create_task(self._periodStatisticsConnections[account_id].close())
                del self._periodStatisticsConnections[account_id]
