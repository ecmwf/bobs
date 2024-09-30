"""
Holds single dataset and its pages

Handles writes, closes and flushes within the dataset
"""

import time
import asyncio
import os
from typing import AsyncIterator
from typing_extensions import Self
from dataclasses import dataclass, field
import logging
import async_files
from pest.util import assert_never
from pest.db.stats_manager import StatsManager, EvictionCommand
from pest.config import Config

logger = logging.getLogger(__name__)
page_size = Config.page_size()

# TODO we may want to put capacity here equal to page size, but that makes `extend` unusable
# overall we may want to microbenchmark the _append_head
new_page = lambda : bytearray()

@dataclass
class Dataset:
    # writing
    write_fd: async_files.fileobj.FileObj|None
    path: str
    page_stats: StatsManager # belongs to stats section but because no default has to be here
    write: bytearray = field(default_factory=new_page)
    write_idx: int = 0

    # reading
    read_pages: dict[int, bytes] = field(default_factory=dict)
    read_fds: dict[int, async_files.fileobj.FileObj] = field(default_factory=dict)
    read_in_progress: set[int] = field(default_factory=set) # primitive lock
    terminating: bool = False

    # stats
    last_write: int = -1
    last_read: tuple[int, int] = (-1, -1) # NOTE this flat structure works best for 1 reader (possibly with retries)

    def is_closed(self):
        return self.write_fd is None

    def abs_head(self):
        return self.write_idx * page_size + len(self.write)

    @classmethod
    async def for_path(cls, path: str, page_stats: StatsManager) -> Self:
        fd = await async_files.FileIO(path, 'wb')()
        page_stats.inc_write_fd()
        return cls(write_fd=fd, path=path, page_stats=page_stats)

    def _append_head(self, c: memoryview) -> memoryview:
        # TODO validate memoryview does not realloc but only slices
        capacity = page_size - len(self.write)
        self.write.extend(c[:capacity])
        return c[capacity:]

    async def append_all(self, c: memoryview) -> None:
        tail = c
        while True:
            tail = self._append_head(tail)
            if not tail:
                return
            await self.advance_page()

    async def advance_page(self) -> None:
        # TODO run in the background? Would need to ensure that the _previous_ advance has been awaited
        # TODO lock
        # contention start
        self.page_stats.inc_read_page()
        self.read_pages[self.write_idx] = bytes(self.write) # TODO is this cast or realloc?
        flush = self.write
        self.write_idx += 1
        self.write = new_page()
        self.last_write = int(time.time())
        # contention end
        
        if not self.write_fd:
            raise ValueError
        await self.write_fd.write(flush)

    async def close(self) -> None:
        if len(self.write) > 0:
            await self.advance_page()
        if not self.write_fd:
            raise ValueError
        await self.write_fd.close()
        self.page_stats.dec_write_fd()
        self.write_fd = None

    async def fetch(self, page_idx: int) -> bytes:
        while page_idx in self.read_in_progress:
            await asyncio.sleep(0.1)
        self.read_in_progress.add(page_idx)
        reader = self.read_fds.get(page_idx, None)
        
        if reader is None:
            reader = await async_files.FileIO(self.path, 'rb')()
            await reader.seek(page_idx * page_size)
            logger.debug(f"seek to {page_idx}")
            if self.terminating:
                raise ValueError()
            self.page_stats.inc_read_fd()
            self.read_fds[page_idx] = reader
        # logger.debug(f"disk read of {page_idx}")
        data = await reader.read(page_size)
        if page_idx+1 not in self.read_fds:
            if self.terminating:
                raise ValueError()
            self.read_fds[page_idx+1] = self.read_fds.pop(page_idx)
        else:
            self.page_stats.dec_read_fd()
        self.page_stats.inc_read_page()
        self.read_pages[page_idx] = data
        self.read_in_progress.remove(page_idx)
        return data

    async def read(self, start: int, end: int) -> AsyncIterator[bytes]:
        page_idx = start // page_size
        rel_start = start % page_size
        exp_waiting = 0.1
        while end == 0 or page_idx * page_size < end:
            if end == 0:
                if page_idx < self.write_idx:
                    rel_end = page_size
                else:
                    if self.is_closed():
                        return
                    rel_end = len(self.write)
                    if rel_start == rel_end:
                        logger.debug(f"waiting for {exp_waiting}")
                        await asyncio.sleep(exp_waiting)
                        exp_waiting *= 2
                        continue
                    else:
                        exp_waiting = 0.1
            else:
                rel_end = page_size if (page_idx+1) * page_size <= end else end % page_size

            if page_idx in self.read_pages:
                source = self.read_pages[page_idx]
            elif page_idx == self.write_idx:
                source = bytes(self.write)
            else:
                source = await self.fetch(page_idx)

            yield source[rel_start:rel_end]
            page_idx += 1
            rel_start = 0
            self.last_read = (page_idx, int(time.time()))

    async def evict_disk(self) -> None:
        if not self.is_closed():
            raise ValueError()
        # TODO lock/fd check
        os.remove(self.path)

    async def finalize(self):
        if self.write_fd is not None:
            logger.error(f"dataset at {self.path} was *not* closed => purging")
            await self.evict_disk()
        self.terminating = True
        for fd in self.read_fds.values():
            await fd.close()

    async def evict(self, command: EvictionCommand):
        logger.debug(f"eviction with {command=}")
        if command == EvictionCommand.read_pages_soft:
            if self.last_read[0] == -1:
                logger.debug(f"soft evicted no pages")
                return
            else:
                hits = 0
                for i in range(self.last_read[0]):
                    if i in self.read_pages:
                        page = self.read_pages.pop(i)
                        del page
                        hits += 1
                logger.debug(f"soft evicted {hits} pages")
                self.page_stats.dec_read_page(hits)
        elif command == EvictionCommand.read_pages_hard:
            hits = len(self.read_pages)
            self.read_pages = {}
            logger.debug(f"evicted {hits} pages")
            self.page_stats.dec_read_page(hits)
        elif command == EvictionCommand.read_fds:
            hits = 0
            readers = list(self.read_fds.items())
            for page_idx, reader in readers:
                if page_idx in self.read_in_progress:
                    logger.debug(f"skipped evicting reader {page_idx}")
                    continue
                logger.debug(f"evicting reader {page_idx}")
                hits += 1
                await self.read_fds.pop(page_idx).close()
            self.page_stats.dec_read_fd(hits)
            logger.debug(f"evicted {hits} readers")
        elif command == EvictionCommand.write_fds:
            pass
        else:
            assert_never(command)
