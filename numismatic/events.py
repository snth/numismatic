import time
import json     # TOOD: use a faster json library if necessary

import attr


@attr.s
class Event:

    def json(self):
        return json.dumps(attr.asdict(self))


@attr.s(slots=True)
class Heartbeat(Event):
    exchange = attr.ib()
    symbol = attr.ib()
    timestamp = attr.ib(default=attr.Factory(time.time))


@attr.s(slots=True)
class Trade(Event):
    exchange = attr.ib()
    symbol = attr.ib()
    timestamp = attr.ib()
    price = attr.ib()
    volume = attr.ib()
    id = attr.ib(default=None)

@attr.s(slots=True)
class LimitOrder(Event):
    exchange = attr.ib()
    symbol = attr.ib()
    timestamp = attr.ib()
    price = attr.ib()
    volume = attr.ib()
    id = attr.ib()

@attr.s(slots=True)
class CancelOrder(Event):
    exchange = attr.ib()
    symbol = attr.ib()
    timestamp = attr.ib()
    id = attr.ib()
