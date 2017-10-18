import sys

import attr

from .base import Collector


@attr.s
class FileCollector(Collector):

    path = attr.ib(default='-')
    format = attr.ib(default='text')
    interval = attr.ib(default=None)

    def __attrs_post_init__(self):
        if self.format=='text':
            self.source_stream = self.source_stream.map(
                lambda ev: str(ev)+'\n')
        elif self.format=='json':
            self.source_stream = self.source_stream.map(
                lambda ev: str(ev.json())+'\n')
        else:
            raise NotImplementedError(f'format={self.format!r}')
        if self.interval:
            self.source_stream = \
                self.source_stream.timed_window(interval=self.interval)
        else:
            # ensure downstream receives lists rather than elements
            self.source_stream = \
                self.source_stream.partition(1)
        self.source_stream.sink(self.write)

    def write(self, data):
        file = sys.stdout if self.path=='-' else open(self.path, 'at')
        try:
            for datum in data:
                file.write(datum)
            file.flush()
        finally:
            if self.path!='-':
                file.close()
