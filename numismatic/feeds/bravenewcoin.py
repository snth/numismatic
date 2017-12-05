import logging

import attr

from .base import Feed, RestClient
from ..config import config_item_getter


logger = logging.getLogger(__name__)


@attr.s
class BraveNewCoinRestClient(RestClient):

    api_url = 'https://bravenewcoin-v1.p.mashape.com/'

    api_key_id = attr.ib(default=attr.Factory(
        config_item_getter('BraveNewCoinFeed', 'api_key_id', 'X-Mashape-Key')))
    api_key_secret = attr.ib(default=attr.Factory(
        config_item_getter('BraveNewCoinFeed', 'api_key_secret')), repr=False)

    @property
    def headers(self):
        return {self.api_key_id: self.api_key_secret}

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


class BraveNewCoinFeed(Feed):

    _rest_client_class = BraveNewCoinRestClient

    def get_list(self, **kwargs): 
        digital_currencies = self.rest_client.get_digital_currency_symbols()
        response = [key 
                    for json_dict in digital_currencies
                    for key, value in json_dict.items()]
        return response

    def get_info(self, assets, **kwargs):
        raise NotImplementedError('Not available for this feed.') 

    def get_prices(self, assets, currencies, **kwargs):
        '''Latest prices for assets in given currencies
        
        Not the most efficient method as BNC does not
        allow a list for input. So we iterate over each
        asset and currency and make separate calls'''
        assets = self._validate_parameter('assets', assets)
        currencies = self._validate_parameter('currencies', currencies)
        response = []
        for asset in assets:
            for currency in currencies:
                data = self.rest_client.get_ticker(coin=asset, show=currency)
                response.append({'asset':asset, 'currency':currency, 
                                 'price': float(data['last_price'])})
        return response


if __name__ == '__main__':
    bnc = BraveNewCoinFeed()
    print(bnc.get_list())
    print(bnc.get_prices('BTC', 'USD'))
    print(bnc.get_ticker('BTC', 'USD'))
