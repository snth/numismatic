import logging
import abc

from ..libs.requesters import Requester

logger = logging.getLogger(__name__)


class Feed(abc.ABC):
    "Feed Base class"

    def __getattr__(self, attr):
        if self.rest_api is not None and hasattr(self.rest_api, attr):
            return getattr(self.rest_api, attr)
        elif self.ws_api is not None and hasattr(self.ws_api, attr):
            return getattr(self.ws_api, attr)
        else:
            raise AttributeError

    @abc.abstractmethod
    def get_list(self):
        return

    @abc.abstractmethod
    def get_info(self, assets):
        return

    @abc.abstractmethod
    def get_prices(self, assets, currencies):
        return


class RestApi(abc.ABC):

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
