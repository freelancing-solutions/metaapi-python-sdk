from .avlTreeReservoir import reservoir
import pytest
from asyncio import sleep
from mock import MagicMock, patch
from freezegun import freeze_time


class TestReservoir:
    @pytest.mark.asyncio
    async def test_accumulate(self):
        """Should be able to accumulate measurements."""
        res = reservoir(3)
        res['pushSome']('test1')
        res['pushSome']('test2')
        assert res['size']() == 2
        assert res['at'](0)['data'] == 'test1'
        assert res['at'](1)['data'] == 'test2'

    @pytest.mark.asyncio
    async def test_randomly_remove(self):
        """Should randomly remove old elements from Reservoir."""
        res = reservoir(3)
        res['pushSome'](5)
        res['pushSome'](4)
        res['pushSome'](3)
        res['pushSome'](2)
        res['pushSome'](1)
        assert res['size']() == 3

    @pytest.mark.asyncio
    async def test_calculate_percentiles(self):
        """Should calculate percentiles when Reservoir has 5 elements."""
        res = reservoir(5)
        data = [5, 1, 3, 2, 4]
        for e in data:
            res['pushSome'](e)

        pers1 = res['getPercentile'](75.13)
        pers2 = res['getPercentile'](75.1)
        pers3 = res['getPercentile'](0.05)
        pers4 = res['getPercentile'](50)
        pers5 = res['getPercentile'](75)

        assert pers1 == 4.0052
        assert pers2 == 4.004
        assert pers3 == 1.002
        assert pers4 == 3
        assert pers5 == 4

    @pytest.mark.asyncio
    async def test_return_percentiles_for_actual_records(self):
        """Should return percentiles for actual records only."""
        with freeze_time() as frozen_datetime:
            res = reservoir(15, 60000)
            for item in [5, 15, 20, 35, 40, 50]:
                res['pushSome'](item)
                frozen_datetime.tick(10.001)
            pers50 = res['getPercentile'](50)
            assert pers50 == 35
