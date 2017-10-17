import logging
import abc

from ..libs.requesters import Requester

logger = logging.getLogger(__name__)


class Feed(abc.ABC):
    "Feed Base class"

    @classmethod
    def factory(cls, feed_name, *args, **kwargs):
        if not isinstance(feed_name, str):
            raise TypeError(f'"feed_name" must be a str. '
                            'Not {type(feed_name)}.')
        feed_name = feed_name.lower()
        subclasses = {subcls.__name__.lower():subcls for subcls in 
                      cls.__subclasses__()}

        subclass = subclasses[feed_name]
        feed = subclass(*args, **kwargs)
        return feed

    def __init__(self, requester='base', cache_dir=None):
        # TODO: Use attrs here
        if not isinstance(requester, Requester):
            requester = Requester.factory(requester, cache_dir=cache_dir)
        self.requester = requester
        self.cache_dir = cache_dir

    def _make_request(self, api_url, params=None, headers=None):
        response = self.requester.get(api_url, params=params, headers=headers)
        data = response.json()
        return data

    @abc.abstractmethod
    def get_list(self):
        return

    @abc.abstractmethod
    def get_info(self, assets):
        return

    @abc.abstractmethod
    def get_prices(self, assets, currencies):
        return
