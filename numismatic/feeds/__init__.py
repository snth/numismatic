from .base import Feed
from .bravenewcoin import BraveNewCoin
from .cryptocompare import CryptoCompare
from .luno import Luno

__all__ = ["Feed", "CryptoCompare", "Luno", "BraveNewCoin"]


def feed_factory(cls, feed_name, *args, **kwargs):
    if not isinstance(feed_name, str):
        raise TypeError(f'"feed_name" must be a str. '
                        'Not {type(feed_name)}.')
    feed_name = feed_name.lower()
    subclasses = {subcls.__name__.lower():subcls for subcls in 
                    cls.__subclasses__()}
    subclass = subclasses[feed_name]
    feed = subclass(*args, **kwargs)
    return feed


setattr(Feed, 'factory', classmethod(feed_factory))
