from ..clients.metaApi.synchronizationListener import SynchronizationListener
from .models import MetatraderAccountInformation, MetatraderPosition, MetatraderOrder, \
    MetatraderSymbolSpecification, MetatraderSymbolPrice, G1Encoder, G2Encoder
import functools
from typing import List, Dict, Optional
from typing_extensions import TypedDict
import asyncio
from datetime import datetime
from hashlib import md5
from copy import copy
import json


class TerminalStateDict(TypedDict):
    instanceIndex: str or None
    connected: bool
    connectedToBroker: bool
    accountInformation: Optional[dict]
    positions: List[dict]
    orders: List[dict]
    specificationsBySymbol: dict
    pricesBySymbol: dict
    completedOrders: dict
    removedPositions: dict
    ordersInitialized: bool
    positionsInitialized: bool
    lastUpdateTime: float
    initializationCounter: int
    specificationCount: int


class TerminalStateHashes(TypedDict):
    specificationsMd5: str
    positionsMd5: str
    ordersMd5: str


class TerminalState(SynchronizationListener):
    """Responsible for storing a local copy of remote terminal state."""

    def __init__(self):
        """Inits the instance of terminal state class"""
        super().__init__()
        self._stateByInstanceIndex = {}
        self._waitForPriceResolves = {}

    @property
    def connected(self) -> bool:
        """Returns true if MetaApi has connected to MetaTrader terminal.

        Returns:
            Whether MetaApi has connected to MetaTrader terminal.
        """
        return True in list(map(lambda instance: instance['connected'], self._stateByInstanceIndex.values()))

    @property
    def connected_to_broker(self) -> bool:
        """Returns true if MetaApi has connected to MetaTrader terminal and MetaTrader terminal is connected to broker

        Returns:
             Whether MetaApi has connected to MetaTrader terminal and MetaTrader terminal is connected to broker
        """
        return True in list(map(lambda instance: instance['connectedToBroker'], self._stateByInstanceIndex.values()))

    @property
    def account_information(self) -> MetatraderAccountInformation:
        """Returns a local copy of account information.

        Returns:
            Local copy of account information.
        """
        return self._get_best_state()['accountInformation']

    @property
    def positions(self) -> List[MetatraderPosition]:
        """Returns a local copy of MetaTrader positions opened.

        Returns:
            A local copy of MetaTrader positions opened.
        """
        return self._get_best_state()['positions']

    @property
    def orders(self) -> List[MetatraderOrder]:
        """Returns a local copy of MetaTrader orders opened.

        Returns:
            A local copy of MetaTrader orders opened.
        """
        return self._get_best_state()['orders']

    @property
    def specifications(self) -> List[MetatraderSymbolSpecification]:
        """Returns a local copy of symbol specifications available in MetaTrader trading terminal.

        Returns:
             A local copy of symbol specifications available in MetaTrader trading terminal.
        """
        return list(self._get_best_state()['specificationsBySymbol'].values())

    def get_hashes(self, account_type: str) -> TerminalStateHashes:
        """Returns hashes of terminal state data for incremental synchronization.

        Args:
            account_type: Account type.

        Returns:
            Hashes of terminal state data.
        """
        specifications = copy(self.specifications)
        for i in range(len(specifications)):
            specifications[i] = copy(specifications[i])
        specifications = sorted(specifications, key=lambda s: s['symbol'])
        if account_type == 'cloud-g1':
            specification: dict
            for specification in specifications:
                del specification['description']
                for key in list(specification.keys()):
                    if isinstance(specification[key], int) and not isinstance(specification[key], bool) and \
                            key != 'digits':
                        specification[key] = float(specification[key])
        specifications_hash = self._get_hash(specifications, account_type)

        positions = copy(self.positions)
        for i in range(len(positions)):
            positions[i] = copy(positions[i])
        positions = sorted(positions, key=lambda p: p['id'])
        position: dict
        for position in positions:
            del position['profit']
            if 'unrealizedProfit' in position:
                del position['unrealizedProfit']
            if 'realizedProfit' in position:
                del position['realizedProfit']
            if 'currentPrice' in position:
                del position['currentPrice']
            if 'currentTickValue' in position:
                del position['currentTickValue']
            if 'updateSequenceNumber' in position:
                del position['updateSequenceNumber']
            if 'accountCurrencyExchangeRate' in position:
                del position['accountCurrencyExchangeRate']
            if 'comment' in position:
                del position['comment']
            if 'originalComment' in position:
                del position['originalComment']
            if 'clientId' in position:
                del position['clientId']
            if account_type == 'cloud-g1':
                del position['time']
                del position['updateTime']
                for key in list(position.keys()):
                    if isinstance(position[key], int) and not isinstance(position[key], bool) and \
                            key != 'magic':
                        position[key] = float(position[key])
        positions_hash = self._get_hash(positions, account_type)

        orders = copy(self.orders)
        for i in range(len(orders)):
            orders[i] = copy(orders[i])
        orders = sorted(orders, key=lambda p: p['id'])
        order: dict
        for order in orders:
            del order['currentPrice']
            if 'updateSequenceNumber' in order:
                del order['updateSequenceNumber']
            if 'accountCurrencyExchangeRate' in order:
                del order['accountCurrencyExchangeRate']
            if 'comment' in order:
                del order['comment']
            if 'originalComment' in order:
                del order['originalComment']
            if 'clientId' in order:
                del order['clientId']
            if account_type == 'cloud-g1':
                del order['time']
                for key in list(order.keys()):
                    if isinstance(order[key], int) and not isinstance(order[key], bool) and \
                            key != 'magic':
                        order[key] = float(order[key])
        orders_hash = self._get_hash(orders, account_type)

        return {
            'specificationsMd5': specifications_hash,
            'positionsMd5': positions_hash,
            'ordersMd5': orders_hash
        }

    def specification(self, symbol: str) -> MetatraderSymbolSpecification:
        """Returns MetaTrader symbol specification by symbol.

        Args:
            symbol: Symbol (e.g. currency pair or an index).

        Returns:
            MetatraderSymbolSpecification found or undefined if specification for a symbol is not found.
        """
        state = self._get_best_state(symbol, 'specification')
        return state['specificationsBySymbol'][symbol] if \
            (symbol in state['specificationsBySymbol']) else None

    def price(self, symbol: str) -> MetatraderSymbolPrice:
        """Returns MetaTrader symbol price by symbol.

        Args:
            symbol: Symbol (e.g. currency pair or an index).

        Returns:
            MetatraderSymbolPrice found or undefined if price for a symbol is not found.
        """
        state = self._get_best_state(symbol, 'price')
        return state['pricesBySymbol'][symbol] if \
            (symbol in state['pricesBySymbol']) else None

    async def wait_for_price(self, symbol: str, timeout_in_seconds: float = 30):
        """Waits for price to be received.

        Args:
            symbol: Symbol (e.g. currency pair or an index).
            timeout_in_seconds: Timeout in seconds, default is 30.

        Returns:
            A coroutine resolving with price or undefined if price has not been received.
        """
        self._waitForPriceResolves[symbol] = self._waitForPriceResolves[symbol] if symbol in \
            self._waitForPriceResolves else []
        if self.price(symbol) is None:
            future = asyncio.Future()
            self._waitForPriceResolves[symbol].append(future)
            await asyncio.wait_for(future, timeout=timeout_in_seconds)

        return self.price(symbol)

    async def on_connected(self, instance_index: str, replicas: int):
        """Invoked when connection to MetaTrader terminal established.

        Args:
            instance_index: Index of an account instance connected.
            replicas: Number of account replicas launched.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        self._get_state(instance_index)['connected'] = True

    async def on_disconnected(self, instance_index: str):
        """Invoked when connection to MetaTrader terminal terminated.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
             A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['connected'] = False
        state['connectedToBroker'] = False

    async def on_broker_connection_status_changed(self, instance_index: str, connected: bool):
        """Invoked when broker connection status have changed.

        Args:
            instance_index: Index of an account instance connected.
            connected: Is MetaTrader terminal is connected to broker.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        self._get_state(instance_index)['connectedToBroker'] = connected

    async def on_synchronization_started(self, instance_index: str, specifications_updated: bool = True,
                                         positions_updated: bool = True, orders_updated: bool = True):
        """Invoked when MetaTrader terminal state synchronization is started.

        Args:
            instance_index: Index of an account instance connected.
            specifications_updated: Whether specifications are going to be updated during synchronization.
            positions_updated: Whether positions are going to be updated during synchronization.
            orders_updated: Whether orders are going to be updated during synchronization.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['accountInformation'] = None
        state['pricesBySymbol'] = {}
        if positions_updated:
            state['positions'] = []
            state['removedPositions'] = {}
            state['positionsInitialized'] = False
        if orders_updated:
            state['orders'] = []
            state['completedOrders'] = {}
            state['ordersInitialized'] = False
        if specifications_updated:
            state['specificationsBySymbol'] = {}

    async def on_account_information_updated(self, instance_index: str,
                                             account_information: MetatraderAccountInformation):
        """Invoked when MetaTrader position is updated.

        Args:
            instance_index: Index of an account instance connected.
            account_information: Updated MetaTrader position.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['accountInformation'] = account_information
        state['initializationCounter'] = 1

    async def on_positions_replaced(self, instance_index: str, positions: List[MetatraderPosition]):
        """Invoked when the positions are replaced as a result of initial terminal state synchronization.

        Args:
            instance_index: Index of an account instance connected.
            positions: Updated array of positions.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['positions'] = positions

    async def on_positions_synchronized(self, instance_index: str, synchronization_id: str):
        """Invoked when position synchronization finished to indicate progress of an initial terminal state
        synchronization.

        Args:
            instance_index: Index of an account instance connected.
            synchronization_id: Synchronization request id.
        """
        state = self._get_state(instance_index)
        state['removedPositions'] = {}
        state['positionsInitialized'] = True
        state['initializationCounter'] = 2

    async def on_position_updated(self, instance_index: str, position: MetatraderPosition):
        """Invoked when MetaTrader position is updated.

        Args:
            instance_index: Index of an account instance connected.
            position: Updated MetaTrader position.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        is_exists = False
        for i in range(len(state['positions'])):
            if state['positions'][i]['id'] == position['id']:
                state['positions'][i] = position
                is_exists = True
                break
        if (not is_exists) and (position['id'] not in state['removedPositions']):
            state['positions'].append(position)

    async def on_position_removed(self, instance_index: str, position_id: str):
        """Invoked when MetaTrader position is removed.

        Args:
            instance_index: Index of an account instance connected.
            position_id: Removed MetaTrader position id.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        position = next((p for p in state['positions'] if p['id'] == position_id), None)
        if position is None:
            for key in list(state['removedPositions'].keys()):
                e = state['removedPositions'][key]
                if e + 5 * 60 < datetime.now().timestamp():
                    del state['removedPositions'][key]
            state['removedPositions'][position_id] = datetime.now().timestamp()
        else:
            state['positions'] = list(filter(lambda p: p['id'] != position_id, state['positions']))

    async def on_pending_orders_replaced(self, instance_index: str, orders: List[MetatraderOrder]):
        """Invoked when the pending orders are replaced as a result of initial terminal state synchronization.
        This method will be invoked only if server thinks the data was updated, otherwise invocation can be skipped.

        Args:
            instance_index: Index of an account instance connected.
            orders: Updated array of pending orders.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['orders'] = orders

    async def on_pending_orders_synchronized(self, instance_index: str, synchronization_id: str):
        """Invoked when pending order synchronization finished to indicate progress of an initial terminal state
        synchronization.

        Args:
            instance_index: Index of an account instance connected.
            synchronization_id: Synchronization request id.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['completedOrders'] = {}
        state['ordersInitialized'] = True
        state['initializationCounter'] = 3

    async def on_pending_order_updated(self, instance_index: str, order: MetatraderOrder):
        """Invoked when MetaTrader pending order is updated.

        Args:
            instance_index: Index of an account instance connected.
            order: Updated MetaTrader pending order.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        is_exists = False
        for i in range(len(state['orders'])):
            if state['orders'][i]['id'] == order['id']:
                state['orders'][i] = order
                is_exists = True
                break
        if (not is_exists) and (order['id'] not in state['completedOrders']):
            state['orders'].append(order)

    async def on_pending_order_completed(self, instance_index: str, order_id: str):
        """Invoked when MetaTrader pending order is completed (executed or canceled).

        Args:
            instance_index: Index of an account instance connected.
            order_id: Completed MetaTrader order id.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        order = next((p for p in state['orders'] if p['id'] == order_id), None)
        if order is None:
            for key in list(state['completedOrders'].keys()):
                e = state['completedOrders'][key]
                if e + 5 * 60 < datetime.now().timestamp():
                    del state['completedOrders'][key]
            state['completedOrders'][order_id] = datetime.now().timestamp()
        else:
            state['orders'] = list(filter(lambda o: o['id'] != order_id, state['orders']))

    async def on_symbol_specifications_updated(self, instance_index: str,
                                               specifications: List[MetatraderSymbolSpecification],
                                               removed_symbols: List[str]):
        """Invoked when a symbol specifications were updated.

        Args:
            instance_index: Index of an account instance connected.
            specifications: Updated MetaTrader symbol specification.
            removed_symbols: Removed symbols.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        for specification in specifications:
            state['specificationsBySymbol'][specification['symbol']] = specification
        for symbol in removed_symbols:
            if symbol in state['specificationsBySymbol']:
                del state['specificationsBySymbol'][symbol]
        state['specificationCount'] = len(state['specificationsBySymbol'].keys())

    async def on_symbol_prices_updated(self, instance_index: str, prices: List[MetatraderSymbolPrice],
                                       equity: float = None, margin: float = None, free_margin: float = None,
                                       margin_level: float = None, account_currency_exchange_rate: float = None):
        """Invoked when prices for several symbols were updated.

        Args:
            instance_index: Index of an account instance connected.
            prices: Updated MetaTrader symbol prices.
            equity: Account liquidation value.
            margin: Margin used.
            free_margin: Free margin.
            margin_level: Margin level calculated as % of equity/margin.
            account_currency_exchange_rate: Current exchange rate of account currency into USD.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        state = self._get_state(instance_index)
        state['lastUpdateTime'] = max(map(lambda p: p['time'].timestamp(), prices)) if len(prices) else 0
        prices_initialized = False
        if prices:
            for price in prices:
                state['pricesBySymbol'][price['symbol']] = price
                positions = list(filter(lambda p: p['symbol'] == price['symbol'], state['positions']))
                other_positions = list(filter(lambda p: p['symbol'] != price['symbol'], state['positions']))
                orders = list(filter(lambda o: o['symbol'] == price['symbol'], state['orders']))
                prices_initialized = True
                for position in other_positions:
                    if position['symbol'] in state['pricesBySymbol']:
                        p = state['pricesBySymbol'][position['symbol']]
                        if 'unrealizedProfit' not in position:
                            self._update_position_profits(position, p)
                    else:
                        prices_initialized = False
                for position in positions:
                    self._update_position_profits(position, price)
                for order in orders:
                    order['currentPrice'] = price['ask'] if (order['type'] == 'ORDER_TYPE_BUY' or
                                                             order['type'] == 'ORDER_TYPE_BUY_LIMIT' or
                                                             order['type'] == 'ORDER_TYPE_BUY_STOP' or
                                                             order['type'] == 'ORDER_TYPE_BUY_STOP_LIMIT') else \
                        price['bid']
                price_resolves = self._waitForPriceResolves[price['symbol']] if price['symbol'] in \
                    self._waitForPriceResolves else []
                if len(price_resolves):
                    resolve: asyncio.Future
                    for resolve in price_resolves:
                        if not resolve.done():
                            resolve.set_result(True)
                    del self._waitForPriceResolves[price['symbol']]
        if state['accountInformation']:
            if state['positionsInitialized'] and prices_initialized:
                if state['accountInformation']['platform'] == 'mt5':
                    state['accountInformation']['equity'] = equity if equity is not None else \
                        state['accountInformation']['balance'] + \
                        functools.reduce(lambda a, b: a + round((b['unrealizedProfit'] if 'unrealizedProfit' in b and
                                                                 b['unrealizedProfit'] is not None
                                                                 else 0) * 100) / 100 +
                                         round((b['swap'] if 'swap' in b and b['swap'] is not None
                                                else 0) * 100) / 100, state['positions'], 0)
                else:
                    state['accountInformation']['equity'] = equity if equity is not None else \
                        state['accountInformation']['balance'] + \
                        functools.reduce(
                        lambda a, b: a + round((b['swap'] if 'swap' in b and b['swap'] is not None
                                                else 0) * 100) / 100 +
                        round((b['commission'] if 'commission' in b and b['commission'] is not None
                               else 0) * 100) / 100 +
                        round((b['unrealizedProfit'] if 'unrealizedProfit' in b and b['unrealizedProfit'] is not None
                               else 0) * 100) / 100,
                            state['positions'], 0)
                state['accountInformation']['equity'] = round(state['accountInformation']['equity'] * 100) / 100
            else:
                state['accountInformation']['equity'] = equity if equity else (
                    state['accountInformation']['equity'] if 'equity' in state['accountInformation'] else None)
            state['accountInformation']['margin'] = margin if margin else (
                state['accountInformation']['margin'] if 'margin' in state['accountInformation'] else None)
            state['accountInformation']['freeMargin'] = free_margin if free_margin else (
                state['accountInformation']['freeMargin'] if 'freeMargin' in state['accountInformation'] else None)
            state['accountInformation']['marginLevel'] = margin_level if free_margin else (
                state['accountInformation']['marginLevel'] if 'marginLevel' in state['accountInformation'] else None)

    async def on_stream_closed(self, instance_index: str):
        """Invoked when a stream for an instance index is closed.

        Args:
            instance_index: Index of an account instance connected.

        Returns:
            A coroutine which resolves when the asynchronous event is processed.
        """
        if instance_index in self._stateByInstanceIndex:
            del self._stateByInstanceIndex[instance_index]

    def _update_position_profits(self, position: Dict, price: Dict):
        specification = self.specification(position['symbol'])
        if specification:
            multiplier = pow(10, specification['digits'])
            if 'profit' in position:
                position['profit'] = round(position['profit'] * multiplier) / multiplier
            if 'unrealizedProfit' not in position or 'realizedProfit' not in position:
                position['unrealizedProfit'] = (1 if (position['type'] == 'POSITION_TYPE_BUY') else -1) * \
                                               (position['currentPrice'] - position['openPrice']) * \
                                               position['currentTickValue'] * position['volume'] / \
                                               specification['tickSize']
                position['unrealizedProfit'] = round(position['unrealizedProfit'] * multiplier) / multiplier
                position['realizedProfit'] = position['profit'] - position['unrealizedProfit']
            new_position_price = price['bid'] if (position['type'] == 'POSITION_TYPE_BUY') else price['ask']
            is_profitable = (1 if (position['type'] == 'POSITION_TYPE_BUY') else -1) * (new_position_price -
                                                                                        position['openPrice'])
            current_tick_value = price['profitTickValue'] if (is_profitable > 0) else price['lossTickValue']
            unrealized_profit = (1 if (position['type'] == 'POSITION_TYPE_BUY') else -1) * \
                                (new_position_price - position['openPrice']) * current_tick_value * \
                position['volume'] / specification['tickSize']
            unrealized_profit = round(unrealized_profit * multiplier) / multiplier
            position['unrealizedProfit'] = unrealized_profit
            position['profit'] = position['unrealizedProfit'] + position['realizedProfit']
            position['profit'] = round(position['profit'] * multiplier) / multiplier
            position['currentPrice'] = new_position_price
            position['currentTickValue'] = current_tick_value

    def _get_state(self, instance_index: str) -> TerminalStateDict:
        if str(instance_index) not in self._stateByInstanceIndex:
            self._stateByInstanceIndex[str(instance_index)] = self._construct_terminal_state(instance_index)
        return self._stateByInstanceIndex[str(instance_index)]

    def _construct_terminal_state(self, instance_index: str = None) -> TerminalStateDict:
        return {
            'instanceIndex': instance_index,
            'connected': False,
            'connectedToBroker': False,
            'accountInformation': None,
            'positions': [],
            'orders': [],
            'specificationsBySymbol': {},
            'pricesBySymbol': {},
            'completedOrders': {},
            'removedPositions': {},
            'ordersInitialized': False,
            'positionsInitialized': False,
            'lastUpdateTime': 0,
            'initializationCounter': 0,
            'specificationCount': 0
        }

    def _get_best_state(self, symbol: str = None, mode: str = 'default') -> TerminalStateDict:
        result = None
        max_update_time = -1
        max_initialization_counter = -1
        max_specification_count = -1
        for state in self._stateByInstanceIndex.values():
            if max_initialization_counter < state['initializationCounter'] or max_initialization_counter == \
                    state['initializationCounter'] and max_initialization_counter == 3 and \
                    max_update_time < state['lastUpdateTime'] or max_initialization_counter == \
                    state['initializationCounter'] and max_initialization_counter == 0 and \
                    max_specification_count < state['specificationCount']:
                if not symbol or (mode == 'specification' and symbol in state['specificationsBySymbol']) or \
                        (mode == 'price' and symbol in state['pricesBySymbol']):
                    max_update_time = state['lastUpdateTime']
                    max_initialization_counter = state['initializationCounter']
                    max_specification_count = state['specificationCount']
                    result = state
        return result or self._construct_terminal_state()

    def _get_hash(self, obj, account_type: str):
        json_item = ''
        if account_type == 'cloud-g1':
            json_item = json.dumps(obj, cls=G1Encoder, ensure_ascii=False)
        elif account_type == 'cloud-g2':
            json_item = json.dumps(obj, cls=G2Encoder, ensure_ascii=False)
        json_item = json_item.encode('utf8')
        return md5(json_item).hexdigest()
