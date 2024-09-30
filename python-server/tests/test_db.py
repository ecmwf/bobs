from pest.db import Database
from pest.db.stats_manager import EvictionCommand
import tempfile
import pytest

# TODO there are some deprecation warnings -- configure this properly
pytest_plugins = ('pytest_asyncio',)

@pytest.mark.asyncio
async def test_open_write_close():
    with tempfile.TemporaryDirectory() as t:
        db = Database(root=t)
        async def read(n):
            result = b""
            async for chunk in db.read(key, 0, n):
                result += chunk
            return result

        key = await db.newkey()
        data = b"1234"
        await db.write(key, 0, data)
        assert await read(2) == data[:2]
        assert await read(4) == data # this is served from the writer's buffer

        await db.close(key)
        assert await read(0) == data # served from reader pages

        # TODO simulate genuine pressure
        await db.dss[key].evict(EvictionCommand.read_pages_hard)
        assert db.dss[key].read_pages == {}
        # this causes new fetching
        assert await read(0) == data # served from reader pages

        await db.evict_all()
