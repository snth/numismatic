import sys
import gzip
from functools import partial

import attr

from ..orderbooks import OrderBook
from .base import Collector


@attr.s
class OrderBookCollector(Collector):

    path = attr.ib(default='-')
    format = attr.ib(default='text')
    interval = attr.ib(default=None)
    order_book = attr.ib(default=attr.Factory(OrderBook))

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        if self.path=='-':
            self._opener = lambda: sys.stdout
        elif self.path.endswith('.gz'):
            self._opener = partial(gzip.open, self.path, mode='at')
        else:
            self._opener = partial(open, self.path, mode='at')

        self.source_stream = (self.source_stream
                              .map(self.order_book.update)
                              .map(lambda ob: (ob.mid_price,
                                               ob.best_bid,
                                               ob.best_ask))
                              )
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
        # TODO: Use aiofiles for non-blocking IO here
        file = self._opener()
        try:
            for datum in data:
                file.write(datum)
            file.flush()
        finally:
            if self.path!='-':
                file.close()
