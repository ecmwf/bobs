from pest.db.dataset import Dataset
from pest.db.stats_manager import StatsManager, EvictionCommand
from typing import AsyncIterator
import uuid
import os
import logging

logger = logging.getLogger(__name__)

class Database():
    def __init__(self, host = "local", root = "/tmp/pest") -> None:
        self.host = host
        self.root = root
        self.dss: dict[str, Dataset] = {}
        self.terminating: bool = False
        self.page_stats = StatsManager()

        os.makedirs(self.root, exist_ok=True)
        if not os.access(self.root, os.W_OK):
            raise ValueError(f"root directory not accessible for writing: {self.root}")

    async def newkey(self) -> str:
        while True:
            uu = str(uuid.uuid4())
            key = f"{self.host}-{uu}"
            if key in self.dss:
                continue
            path = f"{self.root}/{uu}"
            dataset = await Dataset.for_path(path, self.page_stats)
            if self.terminating:
                raise ValueError()
            self.dss[key] = dataset
            return key

    async def evict_disk(self, key: str) -> None:
        await self.dss.pop(key).evict_disk()

    async def evict_all(self) -> None:
        self.terminating = True
        keys = list(self.dss.keys())
        for k in keys:
            await self.evict_disk(k)

    async def finalize(self) -> None:
        self.terminating = True
        for v in self.dss.values():
            await v.finalize()

    async def write(self, key: str, position: int, data: bytes) -> int:
        logger.debug(f"save for {key} from {position}")
        ds = self.dss[key]
        if ds.is_closed():
            logger.warning("already closed")
            return -1
        if position < ds.abs_head():
            logger.warning("write from the past")
            return -1
        if position > ds.abs_head():
            logger.warning("write from the future")
            return -1
        await ds.append_all(memoryview(data))
        # TODO call evict check more granularly, in a background task
        await self.check_evict()
        return ds.abs_head()

    async def close(self, key: str) -> None:
        logger.debug(f"close for {key}")
        ds = self.dss[key]
        await ds.close()

    async def read(self, key: str, start: int, end: int) -> AsyncIterator[bytes]:
        ds = self.dss[key]
        async for chunk in ds.read(start, end):
            yield chunk
        # TODO call evict check more granularly, in a background task
        await self.check_evict()

    async def check_evict(self):
        commands = self.page_stats.get_pressure_levels()
        for v in self.dss.values():
            for e in commands:
                await v.evict(e)
        if EvictionCommand.write_fds in commands:
            logger.error("not clearing write fds")
            # raise NotImplementedError
