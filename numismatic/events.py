import time
import attr


@attr.s(slots=True)
class Heartbeat:
    exchange = attr.ib()
    symbol = attr.ib()
    timestamp = attr.ib(default=attr.Factory(time.time))


@attr.s(slots=True)
class Trade:
    exchange = attr.ib()
    symbol = attr.ib()
    timestamp = attr.ib()
    price = attr.ib()
    volume = attr.ib()
    id = attr.ib(default=None)
