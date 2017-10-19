from .base import Feed
from .bravenewcoin import BraveNewCoinFeed
from .cryptocompare import CryptoCompareFeed
from .luno import LunoFeed

__all__ = ["Feed", "CryptoCompare", "Luno", "BraveNewCoin"]


from ..libs.utils import make_get_subclasses, subclass_factory

setattr(Feed, '_get_subclasses', make_get_subclasses('Feed'))
setattr(Feed, 'factory', subclass_factory)
