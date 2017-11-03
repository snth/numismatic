from .base import Feed
from .bitfinex import BitfinexFeed
from .bravenewcoin import BraveNewCoinFeed
from .cryptocompare import CryptoCompareFeed
from .gdax import GDAXFeed
from .luno import LunoFeed
from .poloniex import PoloniexFeed

from ..libs.utils import make_get_subclasses, subclass_factory

setattr(Feed, '_get_subclasses', make_get_subclasses('Feed'))
setattr(Feed, 'factory', subclass_factory)

__all__ = ["Feed"].extend(Feed._get_subclasses().keys())
