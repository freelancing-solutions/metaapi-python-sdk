from .terminalState import TerminalState
import pytest
from mock import patch
from asyncio import sleep
from datetime import datetime
state = TerminalState()


@pytest.fixture(autouse=True)
def run_around_tests():
    global state
    state = TerminalState()
    yield


class TestTerminalState:

    @pytest.mark.asyncio
    async def test_return_connection_state(self):
        """Should return connection state."""
        assert not state.connected
        await state.on_connected(1, 1)
        assert state.connected
        await state.on_disconnected(1)
        assert not state.connected

    @pytest.mark.asyncio
    async def test_return_broker_connection_state(self):
        """Should return broker connection state."""
        assert not state.connected_to_broker
        await state.on_broker_connection_status_changed(1, True)
        assert state.connected_to_broker
        await state.on_broker_connection_status_changed(1, False)
        assert not state.connected_to_broker
        await state.on_broker_connection_status_changed(1, True)
        await state.on_disconnected(1)
        assert not state.connected_to_broker

    @pytest.mark.asyncio
    async def test_call_disconnect(self):
        """Should call an on_disconnect if there was no signal for a long time"""
        with patch('lib.metaApi.terminalState.asyncio.sleep', new=lambda x: sleep(x / 50)):
            await state.on_connected(1, 1)
            await state.on_broker_connection_status_changed(1, True)
            await sleep(0.2)
            await state.on_broker_connection_status_changed(1, True)
            await sleep(1.1)
            assert state.connected_to_broker
            assert state.connected
            await sleep(0.2)
            assert not state.connected_to_broker
            assert not state.connected

    @pytest.mark.asyncio
    async def test_return_account_information(self):
        """Should return account information."""
        assert not state.account_information
        await state.on_account_information_updated(1, {'balance': 1000})
        assert state.account_information == {'balance': 1000}

    @pytest.mark.asyncio
    async def test_return_positions(self):
        """Should return positions."""
        assert len(state.positions) == 0
        await state.on_position_updated(1, {'id': '1', 'profit': 10})
        await state.on_position_updated(1, {'id': '2'})
        await state.on_position_updated(1, {'id': '1', 'profit': 11})
        assert len(state.positions) == 2
        await state.on_position_removed(1, '2')
        assert len(state.positions) == 1
        assert state.positions == [{'id': '1', 'profit': 11}]

    @pytest.mark.asyncio
    async def test_return_orders(self):
        """Should return orders."""
        assert len(state.orders) == 0
        await state.on_order_updated(1, {'id': '1', 'openPrice': 10})
        await state.on_order_updated(1, {'id': '2'})
        await state.on_order_updated(1, {'id': '1', 'openPrice': 11})
        assert len(state.orders) == 2
        await state.on_order_completed(1, '2')
        assert len(state.orders) == 1
        assert state.orders == [{'id': '1', 'openPrice': 11}]

    @pytest.mark.asyncio
    async def test_return_specifications(self):
        """Should return specifications."""
        assert len(state.specifications) == 0
        await state.on_symbol_specification_updated(1, {'symbol': 'EURUSD', 'tickSize': 0.00001})
        await state.on_symbol_specification_updated(1, {'symbol': 'GBPUSD'})
        await state.on_symbol_specification_updated(1, {'symbol': 'EURUSD', 'tickSize': 0.0001})
        assert len(state.specifications) == 2
        assert state.specifications == [{'symbol': 'EURUSD', 'tickSize': 0.0001}, {'symbol': 'GBPUSD'}]
        assert state.specification('EURUSD') == {'symbol': 'EURUSD', 'tickSize': 0.0001}

    @pytest.mark.asyncio
    async def test_return_price(self):
        """Should return price."""
        assert not state.price('EURUSD')
        await state.on_symbol_prices_updated(1, [{'time': datetime.fromtimestamp(1000000),
                                                  'symbol': 'EURUSD', 'bid': 1, 'ask': 1.1}])
        await state.on_symbol_prices_updated(1, [{'time': datetime.fromtimestamp(1000000), 'symbol': 'GBPUSD'}])
        await state.on_symbol_prices_updated(1, [{'time': datetime.fromtimestamp(1000000),
                                                  'symbol': 'EURUSD', 'bid': 1, 'ask': 1.2}])
        assert state.price('EURUSD') == {'time': datetime.fromtimestamp(1000000),
                                         'symbol': 'EURUSD', 'bid': 1, 'ask': 1.2}

    @pytest.mark.asyncio
    async def test_update_account_equity_and_position(self):
        """Should update account equity and position profit on price update."""
        await state.on_account_information_updated(1, {'equity': 1000, 'balance': 800})
        await state.on_positions_replaced(1, [{
            'id': '1',
            'symbol': 'EURUSD',
            'type': 'POSITION_TYPE_BUY',
            'currentPrice': 9,
            'currentTickValue': 0.5,
            'openPrice': 8,
            'profit': 100,
            'volume': 2
        }])
        await state.on_position_updated(1, {
            'id': '2',
            'symbol': 'AUDUSD',
            'type': 'POSITION_TYPE_BUY',
            'currentPrice': 9,
            'currentTickValue': 0.5,
            'openPrice': 8,
            'profit': 100,
            'volume': 2
        })
        await state.on_symbol_specification_updated(1, {'symbol': 'EURUSD', 'tickSize': 0.01})
        await state.on_symbol_specification_updated(1, {'symbol': 'AUDUSD', 'tickSize': 0.01})
        await state.on_symbol_prices_updated(1, [
          {
            'time': datetime.now(),
            'symbol': 'EURUSD',
            'profitTickValue': 0.5,
            'lossTickValue': 0.5,
            'bid': 10,
            'ask': 11
          },
          {
            'time': datetime.now(),
            'symbol': 'AUDUSD',
            'profitTickValue': 0.5,
            'lossTickValue': 0.5,
            'bid': 10,
            'ask': 11
          }
        ])
        assert list(map(lambda p: p['profit'], state.positions)) == [200, 200]
        assert list(map(lambda p: p['unrealizedProfit'], state.positions)) == [200, 200]
        assert list(map(lambda p: p['currentPrice'], state.positions)) == [10, 10]
        assert state.account_information['equity'] == 1200

    @pytest.mark.asyncio
    async def test_update_margin_fields(self):
        """Should update margin fields on price update."""
        await state.on_account_information_updated(1, {'equity': 1000, 'balance': 800})
        await state.on_symbol_prices_updated(1, [], 100, 200, 400, 40000)
        assert state.account_information['equity'] == 100
        assert state.account_information['margin'] == 200
        assert state.account_information['freeMargin'] == 400
        assert state.account_information['marginLevel'] == 40000

    @pytest.mark.asyncio
    async def test_update_order_current_price_on_price_update(self):
        """Should update order currentPrice on price update."""

        await state.on_order_updated(1, {
          'id': '1',
          'symbol': 'EURUSD',
          'type': 'ORDER_TYPE_BUY_LIMIT',
          'currentPrice': 9
        })
        await state.on_order_updated(1, {
            'id': '2',
            'symbol': 'AUDUSD',
            'type': 'ORDER_TYPE_SELL_LIMIT',
            'currentPrice': 9
        })
        await state.on_symbol_specification_updated(1, {'symbol': 'EURUSD', 'tickSize': 0.01})
        await state.on_symbol_prices_updated(1, [{
          'time': datetime.now(),
          'symbol': 'EURUSD',
          'profitTickValue': 0.5,
          'lossTickValue': 0.5,
          'bid': 10,
          'ask': 11
        }])
        assert list(map(lambda o: o['currentPrice'], state.orders)) == [11, 9]
