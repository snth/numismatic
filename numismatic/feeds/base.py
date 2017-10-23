import logging
import abc

from ..libs.requesters import Requester

logger = logging.getLogger(__name__)


class Feed(abc.ABC):
    "Feed Base class"

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
