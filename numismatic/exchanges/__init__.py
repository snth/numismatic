from .base import Exchange
from .bitfinex import BitfinexExchange
from .gdax import GDAXExchange
from .luno import LunoExchange

__all__ = ["Exchange", "BitfinexExchange", "GDAXExchange", "LunoExchange"]


def exchange_factory(cls, exchange_name, *args, **kwargs):
    if not isinstance(exchange_name, str):
        raise TypeError(f'"exchange_name" must be a str. '
                        'Not {type(exchange_name)}.')
    exchange_name = exchange_name.lower()
    subclasses = {subcls.__name__.lower()[:-len('Exchange')]:subcls 
                    for subcls in cls.__subclasses__()}

    subclass = subclasses[exchange_name]
    exchange = subclass(*args, **kwargs)
    return exchange


setattr(Exchange, 'factory', classmethod(exchange_factory))
