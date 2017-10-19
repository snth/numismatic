from .base import Exchange
from .bitfinex import BitfinexExchange
from .gdax import GDAXExchange
from .luno import LunoExchange

__all__ = ["Exchange", "BitfinexExchange", "GDAXExchange", "LunoExchange"]


from ..libs.utils import make_get_subclasses, subclass_factory

setattr(Exchange, '_get_subclasses', make_get_subclasses('Exchange'))
setattr(Exchange, 'factory', subclass_factory)
