from socketio import AsyncServer
from aiohttp import web
from .latencyService import LatencyService
from mock import AsyncMock, MagicMock, patch
import pytest
import asyncio
from asyncio import sleep
from freezegun import freeze_time
connections = []
fake_server = None
token = 'token'
service: LatencyService
start_time = '2020-10-05 10:00:00.000'
client = MagicMock()
test = MagicMock()


class FakeServer:

    def __init__(self):
        self.app = web.Application()
        self.runner = None

    async def start(self, port=8080):
        global sio
        sio = AsyncServer(async_mode='aiohttp')

        @sio.event
        async def connect(sid, environ):
            connections.append(sid)

        sio.attach(self.app, socketio_path='ws')
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, 'localhost', port)
        await site.start()

    async def stop(self):
        await self.runner.cleanup()


@pytest.fixture(autouse=True)
async def run_around_tests():
    def get_account_region(replica_id: str):
        if replica_id == 'accountIdReplica':
            return 'new-york'
        else:
            return 'vint-hill'

    global client
    client = MagicMock()
    client.ensure_subscribe = AsyncMock()
    client.unsubscribe = AsyncMock()
    client.get_url_settings = AsyncMock(return_value={'url': 'http://localhost:8080', 'is_shared_client_api': True})
    client.unsubscribe_account_region = AsyncMock()
    client.get_account_region = get_account_region
    client.account_replicas = {
        'accountId': {
            'vint-hill': 'accountId',
            'new-york': 'accountIdReplica'
        }
    }
    client.accounts_by_replica_id = {
        'accountId': 'accountId',
        'accountIdReplica': 'accountId'
    }
    global fake_server
    fake_server = FakeServer()
    await fake_server.start()
    global service
    service = LatencyService(client, token, 5000)
    yield
    # await client.close()
    await fake_server.stop()
    tasks = [task for task in asyncio.all_tasks() if task is not
             asyncio.tasks.current_task()]
    list(map(lambda task: task.cancel(), tasks))


@pytest.mark.asyncio
async def test_process_on_connected_event():
    """Should process on_connected event."""
    await service.on_connected('accountId:vint-hill:0:ps-mpa-1')
    assert service.get_active_account_instances('accountId') == ['accountId:vint-hill:0:ps-mpa-1']


@pytest.mark.asyncio
async def test_disconnect_connected_instances_with_bigger_ping():
    """Should disconnect connected instances with bigger ping."""
    with freeze_time(start_time) as frozen_datetime:
        async def delayed_tick():
            await asyncio.sleep(0.01)
            frozen_datetime.tick(5)

        asyncio.create_task(delayed_tick())
        await service.on_connected('accountId:new-york:0:ps-mpa-1')
        await service.on_connected('accountId:vint-hill:0:ps-mpa-1')
        assert service.get_active_account_instances('accountId') == \
               ['accountId:new-york:0:ps-mpa-1', 'accountId:vint-hill:0:ps-mpa-1']
        client.unsubscribe.assert_called_with('accountIdReplica')
        client.unsubscribe_account_region.assert_called_with('accountId', 'new-york')


@pytest.mark.asyncio
async def test_disconnect_synchronized_instances_with_bigger_ping():
    """Should disconnect synchronized instances with bigger ping."""
    with freeze_time(start_time) as frozen_datetime:
        async def delayed_tick():
            await asyncio.sleep(0.01)
            frozen_datetime.tick(5)

        asyncio.create_task(delayed_tick())
        await service.on_connected('accountId:new-york:0:ps-mpa-1')
        await service.on_deals_synchronized('accountId:new-york:0:ps-mpa-1')
        await service.on_connected('accountId:vint-hill:0:ps-mpa-1')
        assert service.get_active_account_instances('accountId') == \
            ['accountId:new-york:0:ps-mpa-1', 'accountId:vint-hill:0:ps-mpa-1']
        client.unsubscribe.assert_not_called()
        await service.on_deals_synchronized('accountId:vint-hill:0:ps-mpa-1')
        client.unsubscribe.assert_called_with('accountIdReplica')
        client.unsubscribe_account_region.assert_called_with('accountId', 'new-york')


@pytest.mark.asyncio
async def test_not_double_check_ping_if_two_accounts_connected_at_the_same_time():
    """Should not double check ping if two accounts connected at the same time."""
    asyncio.create_task(service.on_connected('accountId:new-york:0:ps-mpa-1'))
    await service.on_connected('accountId2:new-york:0:ps-mpa-1')
    assert service.get_active_account_instances('accountId') == ['accountId:new-york:0:ps-mpa-1']
    assert service.get_active_account_instances('accountId2') == ['accountId2:new-york:0:ps-mpa-1']
    client.get_url_settings.assert_called_once()


@pytest.mark.asyncio
async def test_deploy_to_better_ping():
    """Should deploy to a better ping if ping stats changed on refresh."""
    with freeze_time() as frozen_datetime:
        async def delayed_tick():
            await asyncio.sleep(0.01)
            frozen_datetime.tick(5)

        service = LatencyService(client, token, 5000)
        asyncio.create_task(delayed_tick())
        await service.on_connected('accountId:vint-hill:0:ps-mpa-1')
        await service.on_deals_synchronized('accountId:vint-hill:0:ps-mpa-1')
        await service.on_connected('accountId:new-york:0:ps-mpa-1')
        await service.on_deals_synchronized('accountId:new-york:0:ps-mpa-1')
        client.unsubscribe.assert_called_with('accountId')
        client.unsubscribe_account_region.assert_called_with('accountId', 'vint-hill')
        service.on_unsubscribe('accountId')
        task = asyncio.create_task(service._refresh_latency_job())
        await sleep(0.01)
        await service._refreshPromisesByRegion['vint-hill']
        asyncio.create_task(delayed_tick())
        await task
        await sleep(0.05)
        client.ensure_subscribe.assert_any_call('accountId', 0)
        client.ensure_subscribe.assert_any_call('accountId', 1)


@pytest.mark.asyncio
async def test_subscribe_replicas_on_disconnected_event():
    """Should subscribe replicas on disconnected event if all replicas offline."""
    await service.on_connected('accountId:vint-hill:0:ps-mpa-1')
    await service.on_deals_synchronized('accountId:vint-hill:0:ps-mpa-1')
    await service.on_connected('accountId:new-york:0:ps-mpa-1')
    await service.on_deals_synchronized('accountId:new-york:0:ps-mpa-1')
    assert service.get_active_account_instances('accountId') == \
           ['accountId:vint-hill:0:ps-mpa-1', 'accountId:new-york:0:ps-mpa-1']
    assert service.get_synchronized_account_instances('accountId') == \
           ['accountId:vint-hill:0:ps-mpa-1', 'accountId:new-york:0:ps-mpa-1']
    service.on_disconnected('accountId:new-york:0:ps-mpa-1')
    assert service.get_active_account_instances('accountId') == ['accountId:vint-hill:0:ps-mpa-1']
    assert service.get_synchronized_account_instances('accountId') == ['accountId:vint-hill:0:ps-mpa-1']
    client.ensure_subscribe.assert_not_called()
    service.on_disconnected('accountId:vint-hill:0:ps-mpa-1')
    assert service.get_active_account_instances('accountId') == []
    assert service.get_synchronized_account_instances('accountId') == []
    client.ensure_subscribe.assert_any_call('accountIdReplica', 0)
    client.ensure_subscribe.assert_any_call('accountIdReplica', 1)


@pytest.mark.asyncio
async def test_mark_accounts_as_disconnected_on_unsubscribe():
    """Should mark accounts as disconnected on unsubscribe."""
    await service.on_connected('accountId:vint-hill:0:ps-mpa-1')
    await service.on_deals_synchronized('accountId:vint-hill:0:ps-mpa-1')
    await service.on_connected('accountId:new-york:0:ps-mpa-1')
    await service.on_deals_synchronized('accountId:new-york:0:ps-mpa-1')
    assert service.get_active_account_instances('accountId') == \
           ['accountId:vint-hill:0:ps-mpa-1', 'accountId:new-york:0:ps-mpa-1']
    assert service.get_synchronized_account_instances('accountId') == \
           ['accountId:vint-hill:0:ps-mpa-1', 'accountId:new-york:0:ps-mpa-1']
    service.on_unsubscribe('accountIdReplica')
    assert service.get_active_account_instances('accountId') == ['accountId:vint-hill:0:ps-mpa-1']
    assert service.get_synchronized_account_instances('accountId') == ['accountId:vint-hill:0:ps-mpa-1']
