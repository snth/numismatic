from itertools import product

from .base import Feed


class Luno(Feed):

    api_url = 'https://api.mybitx.com/api/1/'

    def get_list(self):
        api_url = f'{self.api_url}/tickers'
        data = self._make_request(api_url)
        tickers = data['tickers']
        return [ticker['pair'] for ticker in tickers]

    def get_info(self, assets):
        raise NotImplementedError('Not available for this feed.') 

    def get_prices(self, assets, currencies):
        api_url = f'{self.api_url}/tickers'
        data = self._make_request(api_url)
        tickers = data['tickers']
        assets = assets.upper().split(',')
        currencies = currencies.upper().split(',')
        pairs = {f'{asset}{currency}' for asset, currency in 
                 product(assets, currencies)}
        return [ticker for ticker in tickers if ticker['pair'] in pairs]
