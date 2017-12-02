import sys
import gzip
from functools import partial

import attr

from .base import Collector


@attr.s
class FileCollector(Collector):

    path = attr.ib(default='-')
    format = attr.ib(default='text')
    interval = attr.ib(default=None)

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        # paths
        if self.path=='-':
            self._opener = lambda: sys.stdout
        elif self.path.endswith('.gz'):
            self._opener = partial(gzip.open, self.path, mode='at')
        else:
            self._opener = partial(open, self.path, mode='at')

        if self.format=='text':
            self.event_stream = self.event_stream.map(
                lambda ev: str(ev)+'\n')
        elif self.format=='json':
            self.event_stream = self.event_stream.map(
                lambda ev: str(ev.json())+'\n')
        else:
            raise NotImplementedError(f'format={self.format!r}')
        # construct data_stream
        if self.interval:
            self._data_stream = \
                self.event_stream.timed_window(interval=self.interval)
        else:
            # ensure downstream receives lists rather than elements
            self._data_stream = \
                self.event_stream.partition(1)
        self._data_stream.sink(self.write)

    def write(self, data):
        # TODO: Use aiofiles for non-blocking IO here
        file = self._opener()
        try:
            for datum in data:
                file.write(datum)
            file.flush()
        finally:
            if self.path!='-':
                file.close()
