import sys

import attr

from .base import Collector


@attr.s
class FileCollector(Collector):

    file = attr.ib(default=sys.stdout)
    format = attr.ib(default='text')
    interval = attr.ib(default='1s')

    def __attrs_post_init__(self):
        print(self.__class__.__name__)
        if self.format=='text':
            self.source_stream = self.source_stream.map(
                lambda ev: str(ev)+'\n')
        elif self.format=='json':
            self.source_stream = self.source_stream.map(
                lambda ev: str(ev.json())+'\n')
        else:
            raise NotImplementedError(f'format={self.format!r}')
        self.source_stream.sink(self.write)

    def write(self, data):
        self.file.write(data)
        self.file.flush()
