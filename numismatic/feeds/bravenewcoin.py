from itertools import product
import attr
from .base import Feed
from ..libs.config import get_config

class BraveNewCoin(Feed):
    config = get_config()['bravenewcoin']
    api_key_id = (config.get('api_key_id', '') if config else '')
    api_key_secret = (config.get('api_key_secret', '') if config else '')
    api_url = 'https://bravenewcoin-v1.p.mashape.com/'

    def __init__(self, requester='basic', cache_dir=None):
      super().__init__(requester=requester, cache_dir=cache_dir)
      self.headers = {f'{self.api_key_id}': f'{self.api_key_secret}'}

    def get_list(self): 
      api_url = f'{self.api_url}/digital-currency-symbols'
      data = self._make_request(api_url, headers=self.headers)
      response = []

      for json_dict in data['digital_currencies']:
        for key, value in json_dict.items():
          response.insert(0, key)

      return response

    #TODO implement this
    def get_info(self, assets):
      raise NotImplementedError('Not implemented yet') 

    #TODO implement this
    def get_prices(self, assets, currencies):
      raise NotImplementedError('Not implemented yet')
