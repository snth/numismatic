from functools import partial
from itertools import chain
from collections import defaultdict
import bisect

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
            if not level:
                # a new level so need to update bids or asks
                side = self.bids if order.volume>0 else self.asks
                price = -order.price if order.volume>0 else order.price
                position = bisect.bisect(side, price)
                side.insert(position, price)
            level.append(order.id)
        elif isinstance(order, CancelOrder):
            order = self.orders[order.id]
            del self.orders[order.id]
            level = self.levels[order.price]
            level.remove(order.id)
            if not level:
                del self.levels[order.price]
                side = self.bids if order.volume>0 else self.asks
                price = -order.price if order.volume>0 else order.price
                position = bisect.bisect(side, price)
                side.pop(position-1)
        else:
            raise NotImplementedError(type(order))
        return self

    def best_bid(self):
        return -self.bids[0] if self.bids else float('nan')


    def best_ask(self):
        return self.asks[0] if self.asks else float('nan')

    def mid_price(self):
        return (self.best_bid()+self.best_ask())/2


if __name__=='__main__':
    import time
    import random
    random.seed(5)
    prices = [random.randint(-5, 5) for i in range(10)]
    print(prices)
    mid_price = 0
    ob = OrderBook()
    for i, p in enumerate(prices):
        # Treat orders below 5 as bids and above as asks
        o = LimitOrder('test', 'BTCUSD', time.time(), p, 
                       (1 if p<mid_price else -1)*10*i , i)
        print(o)
        ob.update(o)
        print(ob)
        print(ob.best_bid(), ob.best_ask())
        print()
    for i, p in enumerate(prices):
        o = CancelOrder('test', 'BTCUSD', time.time(), i)
        print(o)
        ob.update(o)
        print(ob)
        print(ob.best_bid(), ob.best_ask())
        print()
    print(prices)
