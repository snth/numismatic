import attr

from ..orderbooks import OrderBook


@attr.s
class OrderBookCollector:

    order_book = attr.ib(default=attr.Factory(OrderBook))

    def __attrs_post_init__(self):
        self.sourc_stream = (self.source_stream
                             .map(self.order_book.update)
                             .map(lambda ob: print(ob.mid_price(),
                                                   ob.best_bid(),
                                                   ob.best_ask()))
                             )
