import attr

from .libs.events import LimitOrder, CancelOrder
from .libs.queue import PriorityQueue


@attr.s
class OrderBook:

    orders = attr.ib(default=attr.Factory(dict))
    bids = attr.ib(default=attr.Factory(PriorityQueue))
    asks = attr.ib(default=attr.Factory(PriorityQueue))

    def update(self, order):
        if isinstance(order, LimitOrder) and abs(order.volume>0):
            side = self.bids if order.volume>0 else self.asks
            price = -order.price if order.volume>0 else order.price
            side.add(order.id, price)
            self.orders[order.id] = order
        elif isinstance(order, CancelOrder):
            order = self.orders[order.id]
            side = self.bids if order.volume>0 else self.asks
            side.remove(order.id)
            del self.orders[order.id]
        elif isinstance(order, LimitOrder) and order.volume==0:
            # When dealing with aggregated Level 2 data, a zero volume order
            # indicates that that level can be removed.
            order = self.orders[order.id]
            del self.orders[order.id]
            side = self.bids if order.price<=self.best_bid else self.asks
            side.remove(order.id)
        else:
            raise NotImplementedError(type(order))
        return self

    def best_bid(self):
        return self.orders[self.bids.peek()].price if self.bids else \
            float('nan')

    def best_ask(self):
        return self.orders[self.asks.peek()].price if self.asks else \
            float('nan')

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
