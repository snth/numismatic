from .base import Exchange
from .bitfinex import BitfinexExchange
from .gdax import GDAXExchange
from .luno import LunoExchange

__all__ = ["Exchange", "BitfinexExchange", "GDAXExchange", "LunoExchange"]


from ..libs.utils import make_subclass_factory

setattr(Exchange, 'factory', make_subclass_factory('Exchange'))
