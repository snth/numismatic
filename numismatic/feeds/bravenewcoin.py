import logging

from .base import Feed, RestApi
from ..libs.config import get_config


logger = logging.getLogger(__name__)


class BraveNewCoinFeed(Feed):

    def __init__(self, requester='basic', cache_dir=None, api_key_id=None,
                 api_key_secret=None):
        self.rest_api = BraveNewCoinRestApi(
            requester=requester, cache_dir=cache_dir, api_key_id=api_key_id,
            api_key_secret=api_key_secret)
        self.ws_api = None

    def get_list(self): 
        digital_currencies = self.rest_api.get_digital_currency_symbols()
        response = [key 
                    for json_dict in digital_currencies
                    for key, value in json_dict.items()]
        return response

    def get_info(self, assets):
        raise NotImplementedError('Not available for this feed.') 

    def get_prices(self, assets, currencies):
        """Not the most efficient method as BNC does not
        allow a list for input. So we iterate over each
        asset and currency and make separate calls"""
        response = []
        for asset in assets.split(','):
            for currency in currencies.split(','):
                data = self.rest_api.get_ticker(coin=asset, show=currency)
                response.append({'asset':asset, 'currency':currency, 
                                 'price': float(data['last_price'])})
        return response


class BraveNewCoinRestApi(RestApi):

    api_url = 'https://bravenewcoin-v1.p.mashape.com/'

    def __init__(self, requester='basic', cache_dir=None, api_key_id=None,
                 api_key_secret=None):
        # Should the requester and cache_dir not also be settable in config?
        super().__init__(requester=requester, cache_dir=cache_dir)
        
        config = get_config('bravenewcoin')
        self.api_key_secret = config.get('api_key_secret', None)
        assert self.api_key_secret

        self.headers = {'X-Mashape-Key': f'{self.api_key_secret}'}

    def get_digital_currency_symbols(self): 
        api_url = f'{self.api_url}/digital-currency-symbols'
        data = self._make_request(api_url, headers=self.headers)
        return data['digital_currencies']

    def get_ticker(self, coin, show):
        api_url = f'{self.api_url}/ticker/'
        params = dict(coin=coin, show=show)
        data = self._make_request(api_url, params=params,
                                    headers=self.headers)
        return data if data['success'] else None


if __name__ == '__main__':
    bnc = BraveNewCoinFeed()
    print(bnc.get_list())
    print(bnc.get_prices('BTC', 'USD'))
    print(bnc.get_ticker('BTC', 'USD'))
