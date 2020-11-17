from ..clients.metaApi.synchronizationListener import SynchronizationListener
from .reservoir.reservoir import Reservoir
from .models import MetatraderSymbolPrice, date
from typing import TypedDict
import asyncio
from datetime import datetime
import functools


class ConnectionHealthStatus(TypedDict):
    """Connection health status."""
    connected: bool
    """Flag indicating successful connection to API server."""
    connectedToBroker: bool
    """Flag indicating successfull connection to broker."""
    quoteStreamingHealthy: bool
    """Flag indicating that quotes are being streamed successfully from the broker."""
    synchronized: bool
    """Flag indicating a successful synchronization."""
    healthy: bool
    """Flag indicating overall connection health status."""
    message: str
    """Health status message."""


class ConnectionHealthMonitor(SynchronizationListener):
    """Tracks connection health status."""

    def __init__(self, connection):
        """Inits the monitor.

        Args:
            connection: MetaApi connection instance.
        """
        super().__init__()
        self._connection = connection

        async def update_quote_health_job():
            while True:
                await asyncio.sleep(1)
                self._update_quote_health_status()

        async def measure_uptime_job():
            while True:
                await asyncio.sleep(1)
                self._measure_uptime()

        self._quotesHealthy = False
        self._offset = 0
        self._minQuoteInterval = 60
        self._uptimeReservoir = Reservoir(24 * 7, 7 * 24 * 60 * 60 * 1000)
        asyncio.create_task(update_quote_health_job())
        asyncio.create_task(measure_uptime_job())

    async def on_symbol_price_updated(self, price: MetatraderSymbolPrice):
        """Invoked when a symbol price was updated.

        Args:
            price: Updated MetaTrader symbol price.
        """
        try:
            broker_timestamp = date(price['brokerTime']).timestamp()
            self._priceUpdatedAt = datetime.now()
            self._offset = self._priceUpdatedAt.timestamp() - broker_timestamp

        except Exception as err:
            print(f'[{datetime.now().isoformat()}] failed to update quote streaming health status on price ' +
                  'update for account ' + self._connection.account.id, err)

    @property
    def health_status(self) -> ConnectionHealthStatus:
        """Returns health status.

        Returns:
            Connection health status.
        """
        status = {
            'connected': self._connection.terminal_state.connected,
            'connectedToBroker': self._connection.terminal_state.connected_to_broker,
            'quoteStreamingHealthy': self._quotesHealthy,
            'synchronized': self._connection.synchronized
        }
        status['healthy'] = status['connected'] and status['connectedToBroker'] and \
            status['quoteStreamingHealthy'] and status['synchronized']
        if status['healthy']:
            message = 'Connection to broker is stable. No health issues detected.'
        else:
            message = 'Connection is not healthy because '
            reasons = []
            if not status['connected']:
                reasons.append('connection to API server is not established or lost')
            if not status['connectedToBroker']:
                reasons.append('connection to broker is not established or lost')
            if not status['synchronized']:
                reasons.append('local terminal state is not synchronized to broker')
            if not status['quoteStreamingHealthy']:
                reasons.append('quotes are not streamed from the broker properly')
            message = message + functools.reduce(lambda a, b: a + ' and ' + b, reasons) + '.'
        status['message'] = message
        return status

    @property
    def uptime(self) -> float:
        """Returns uptime in percents measured over a period of one week.

        Returns:
            Uptime in percents measured over a period of one week.
        """
        return self._uptimeReservoir.get_statistics()['average']

    def _measure_uptime(self):
        try:
            self._uptimeReservoir.push_measurement(100 if (
                self._connection.terminal_state.connected and self._connection.terminal_state.connected_to_broker
                and self._connection.synchronized and self._quotesHealthy) else 0)
        except Exception as err:
            print(f'[{datetime.now().isoformat()}] failed to measure uptime for account ' +
                  self._connection.account.id, err)

    def _update_quote_health_status(self):
        try:
            server_date_time = datetime.fromtimestamp(datetime.now().timestamp() - self._offset)
            server_time = server_date_time.strftime('%H:%M:%S.%f')
            day_of_week = server_date_time.isoweekday()
            days_of_week = {
                '1': 'MONDAY',
                '2': 'TUESDAY',
                '3': 'WEDNESDAY',
                '4': 'THURSDAY',
                '5': 'FRIDAY',
                '6': 'SATURDAY',
                '7': 'SUNDAY'
            }
            in_quote_session = False
            for symbol in self._connection.subscribed_symbols:
                specification = self._connection.terminal_state.specification(symbol) or {}
                quote_sessions_list = (specification['quoteSessions'] if 'quoteSessions' in specification else [])
                quote_sessions = quote_sessions_list[days_of_week[str(day_of_week)]] if \
                    days_of_week[str(day_of_week)] in quote_sessions_list else []
                for session in quote_sessions:
                    if session['from'] <= server_time <= session['to']:
                        in_quote_session = True
            self._quotesHealthy = (not len(self._connection.subscribed_symbols)) or (not in_quote_session) or \
                                  ((datetime.now().timestamp() - self._priceUpdatedAt.timestamp()) <
                                   self._minQuoteInterval)
        except Exception as err:
            print(f'[{datetime.now().isoformat()}] failed to update quote streaming health status for account ' +
                  self._connection.account.id, err)
