import time
import json     # TOOD: use a faster json library if necessary
from enum import Enum
import math

import attr


class OrderType(str, Enum):
    BUY = 'BUY'
    SELL = 'SELL'
    CANCEL = 'CANCEL'
    TRADE = 'TRADE'


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
    exchange = attr.ib(convert=str)
    symbol = attr.ib(convert=str)
    price = attr.ib(convert=float)
    volume = attr.ib(convert=float)
    type = attr.ib(convert=OrderType, default='TRADE')
    timestamp = attr.ib(convert=float, default=attr.Factory(time.time))
    id = attr.ib(convert=str, default='')

@attr.s(slots=True)
class Order(Event):
    exchange = attr.ib(convert=str)
    symbol = attr.ib(convert=str)
    price = attr.ib(convert=float, default=math.nan)
    volume = attr.ib(convert=float, default=math.nan)
    type = attr.ib(convert=OrderType, default='TRADE')
    timestamp = attr.ib(convert=float, default=attr.Factory(time.time))
    id = attr.ib(convert=str, default='')

    @id.validator
    def _validate_id(self, attribute, value):
        if not value:
            self.id = self.price
