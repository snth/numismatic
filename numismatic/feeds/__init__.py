from .base import Feed
from .bravenewcoin import BraveNewCoinFeed
from .cryptocompare import CryptoCompareFeed
from .luno import LunoFeed

__all__ = ["Feed", "CryptoCompare", "Luno", "BraveNewCoin"]


from ..libs.utils import make_subclass_factory

setattr(Feed, 'factory', make_subclass_factory('Feed'))
