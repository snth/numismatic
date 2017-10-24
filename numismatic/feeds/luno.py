from itertools import product
import logging

from .base import Feed, RestApi


logger = logging.getLogger(__name__)


class LunoRestApi(RestApi):

    api_url = 'https://api.mybitx.com/api/1/'

    def get_tickers(self):
        api_url = f'{self.api_url}/tickers'
        data = self._make_request(api_url)
        return data['tickers']

class LunoFeed(Feed):

    def __init__(self, requester='basic', cache_dir=None):
        self.rest_api = LunoRestApi(requester=requester, cache_dir=cache_dir)
        self.websocket_api = None

    def get_list(self):
        tickers = self.rest_api.get_tickers()
        return [ticker['pair'] for ticker in tickers]

    def get_info(self, assets):
        raise NotImplementedError('Not available for this feed.') 

    def get_prices(self, assets, currencies):
        assets = assets.upper().split(',')
        currencies = currencies.upper().split(',')
        tickers = self.rest_api.get_tickers()
        pairs = {f'{asset}{currency}' for asset, currency in 
                 product(assets, currencies)}
        return [ticker for ticker in tickers if ticker['pair'] in pairs]
