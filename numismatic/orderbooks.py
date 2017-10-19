from functools import partial
from itertools import chain
from collections import defaultdict

import attr

from .libs.events import LimitOrder, CancelOrder


@attr.s
class OrderBook:

    orders = attr.ib(default=attr.Factory(dict))
    levels = attr.ib(default=attr.Factory(partial(defaultdict, list)))
    bids = attr.ib(default=attr.Factory(list))
    asks = attr.ib(default=attr.Factory(list))

    def update(self, order):
        if isinstance(order, LimitOrder):
            self.orders[order.id] = order
            level = self.levels[order.price]
            level.append(order.id)
            side = self.bids if order.volume>0 else self.asks
            side.append(order.price)
        elif isinstance(order, CancelOrder):
            order = self.orders[order.id]
            del self.orders[order.id]
            level = self.levels[order.price]
            level.remove(order.id)
            if not level:
                del self.levels[order.price]
            side = self.bids if order.volume>0 else self.asks
            side.remove(order.price)
        else:
            raise NotImplementedError(f'order={order}')

    def best_bid(self):
        return max(self.bids) if self.bids else float('nan')


    def best_ask(self):
        return min(self.asks) if self.asks else float('nan')


if __name__=='__main__':
    import time
    prices = [1, 10, 2, 2, 9, 9]
    ob = OrderBook()
    for i, p in enumerate(prices):
        # Treat orders below 5 as bids and above as asks
        o = LimitOrder('test', 'BTCUSD', time.time(), p, 
                       (1 if p<5 else -1)*10*p , i)
        ob.update(o)
        print('\n', ob.best_bid(), ob.best_ask())
        print(ob)
    for i, p in enumerate(prices):
        c = CancelOrder('test', 'BTCUSD', time.time(), i)
        ob.update(c)
        print('\n', ob.best_bid(), ob.best_ask())
        print(ob)
