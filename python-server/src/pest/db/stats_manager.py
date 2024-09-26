"""
Keeps stats about total page count
"""

from enum import Enum, auto

class EvictionCommand(int, Enum):
    read_pages_soft = auto()
    read_pages_hard = auto()
    read_fds = auto()
    write_fds = auto()
    

class StatsManager():
    write_fds: int = 0
    read_pages: int = 0
    read_fds: int = 0

    # TODO config
    page_threshold_soft: int = 10
    page_threshold_hard: int = 20
    fds_threshold: int = 5

    # overhead methods in case we need atomics, locks, etc
    def inc_read_page(self):
        self.read_pages += 1

    def inc_read_fd(self):
        self.read_fds += 1

    def inc_write_fd(self):
        self.write_fds += 1

    def dec_read_page(self, by: int = 1):
        self.read_pages -= by

    def dec_read_fd(self, by: int = 1):
        self.read_fds -= by

    def dec_write_fd(self, by: int = 1):
        self.write_fds -= by

    # TODO probably move also the access stats from Dataset in here, have them in a prio heap, issue targeted evictions

    def get_pressure_levels(self) -> set[EvictionCommand]:
        rv = set()
        if self.read_pages > self.page_threshold_hard:
            rv.add(EvictionCommand.read_pages_hard)
        elif self.read_pages > self.page_threshold_soft:
            rv.add(EvictionCommand.read_pages_soft)
        if self.read_fds + self.write_fds > self.fds_threshold:
            rv.add(EvictionCommand.read_fds)
        if self.write_fds > self.fds_threshold:
            rv.add(EvictionCommand.write_fds)
        return rv
