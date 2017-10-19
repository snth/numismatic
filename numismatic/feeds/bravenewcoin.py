from itertools import product
import attr
from .base import Feed
from ..libs.config import get_config

class BraveNewCoin(Feed):

    api_url = 'https://bravenewcoin-v1.p.mashape.com/'

    def __init__(self, requester='basic', cache_dir=None, api_key_id=None,
                 api_key_secret=None):
        # Should the requester and cache_dir not also be settable in config?
        super().__init__(requester=requester, cache_dir=cache_dir)
        all__config = get_config()
        config = (all_config['bravenewcoin'] if 'bravenewcoin' in all_config
                else {})
        self.api_key_id = (config.get('api_key_id', '') if api_key_id is None
                            else api_key_id)
        self.api_key_secret = (config.get('api_key_secret', '') if
                                api_key_secret is None else api_key_secret)
        self.headers = {f'{self.api_key_id}': f'{self.api_key_secret}'}

    def get_list(self): 
        api_url = f'{self.api_url}/digital-currency-symbols'
        data = self._make_request(api_url, headers=self.headers)
        response = []

        response = [key 
                    for json_dict in data['digital_currencies']
                    for key, value in json_dict.items()]

        return response

    #TODO implement this
    def get_info(self, assets):
        raise NotImplementedError('Not implemented yet') 

    def get_prices(self, assets, currencies):
        """Not the most efficient method as BNC does not
        allow a list for input. So we iterate over each
        asset and currency and make separate calls"""
        response = []
        for asset in assets.split(','):
            for currency in currencies.split(','):
                api_url = f'{self.api_url}/ticker/'
                params = dict(coin=asset, show=currency)
                data = self._make_request(api_url, params=params,
                                          headers=self.headers)
                if not data['success']:
                    continue

                response.append({'asset':asset, 'currency':currency, 
                                 'price': float(data['last_price'])})
            
        return response
