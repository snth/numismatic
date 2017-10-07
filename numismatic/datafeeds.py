import logging
from itertools import product, chain
import math
import time
from datetime import datetime, timedelta
from dateutil.parser import parse
import abc

from pathlib import Path
import asyncio

from .requesters import Requester

logger = logging.getLogger(__name__)


# TODO: move the utility functions to lib module
def make_list_str(items):
    if isinstance(items, str):
        items = items.split(',')
    return ','.join(items)

def date_range(start_date, end_date, delta):
    dates = []

    while end_date > start_date:
        dates.insert(0, end_date)
        end_date -= delta
    
    dates.insert(0, start_date)
    return dates


def to_datetime(datelike, origin=None):
    if datelike is None:
        return datetime.now()
    elif isinstance(datelike, str):
        return parse(datelike)
    else:
        raise TypeError(f'{datelike}')


def _validate_dates(start_date, end_date, freq):
    freqmap = dict(d='days', h='hours', m='minutes', s='seconds',
                    ms='milliseconds', us='microseconds')
    freqstr = freqmap[freq]
    end_date = to_datetime(end_date)
    if isinstance(start_date, int):
        start_date = end_date + timedelta(**{freqstr:start_date})
    else:
        start_date = to_datetime(start_date)
    interval_time = timedelta(**{freqstr:1})
    intervals = math.ceil((end_date-start_date)/interval_time)
    return start_date, end_date, freqstr, intervals


class Datafeed(abc.ABC):
    "Base class"

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

    def _make_request(self, api_url, params=None):
        response = self.requester.get(api_url, params=params)
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


class Luno(Datafeed):

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


class CryptoCompare(Datafeed):
    '''Low level API for CryptoCompare.com

    TODO:
      * This should use the json api to automatically generate the methods
    '''

    base_url = 'https://www.cryptocompare.com/api/data/'
    api_url = 'https://min-api.cryptocompare.com/data'
    _interval_limit = 2000

    def __init__(self, requester='basic', cache_dir=None):
        super().__init__(requester=requester, cache_dir=cache_dir)

    def get_list(self):
        api_url = f'{self.base_url}/coinlist'
        coinlist = self._make_request(api_url)
        return coinlist.keys()

    def get_info(self, assets):
        api_url = f'{self.base_url}/coinlist'
        coinlist = self._make_request(api_url)
        assets = assets.upper().split(',')
        assets_info = [coinlist[a] for a in assets]
        return assets_info

    def get_prices(self, assets, currencies):
        assets = assets.upper().split(',')
        currencies = currencies.upper().split(',')
        # FIXME: SHouldn't use caching
        data = self.get_latest_price_multi(assets, currencies)
        prices = [{'asset':asset, 'currency':currency, 'price':price}
                  for asset, asset_prices in data.items()
                  for currency, price in asset_prices.items()]
        return prices

    def get_historical_data(self, asset, currency, freq='d', end_date=None,
                            start_date=-30, exchange=None):
        asset = asset.upper()
        currency = currency.upper()
        start_date, end_date, freqstr, intervals = \
            _validate_dates(start_date, end_date, freq)
        limit = min(intervals, self._interval_limit)
        dates = date_range(start_date, end_date, timedelta(**{freqstr:limit}))

        data = []
        for start, end in zip(dates[:-1], dates[1:]):
            toTs = math.ceil(end.timestamp())
            limit = math.ceil((end-start)/timedelta(**{freqstr:1}))
            logger.debug(f'Getting {asset}/{currency} for {limit}{freqstr} to {end}')
            time.sleep(1/4)   # max 4 requests per second
            if freq.startswith('m'):
                chunk = self.get_historical_minute(
                    fsym=asset, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            elif freq.startswith('h'):
                chunk = self.get_historical_hour(
                    fsym=asset, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            elif freq.startswith('d'):
                chunk = self.get_historical_day(
                    fsym=asset, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            else:
                raise NotImplementedError(f'freq={freq}')
            data.extend(chunk)
        return data

    def get_latest_price(self, fsym, tsyms):
        api_url = f'{self.api_url}/price'
        tsyms = make_list_str(tsyms)
        query_str = f'{api_call}?fsym={fsym}&tsyms={tsyms}'
        return self._make_request(api_url, query_str)

    def get_latest_price_multi(self, fsyms, tsyms):
        api_url = f'{self.api_url}/pricemulti'
        params = dict(fsyms=make_list_str(fsyms), tsyms=make_list_str(tsyms))
        return self._make_request(api_url, params)

    def get_historical_price(self, fsym, tsyms, ts, markets=None):
        api_url = f'{self.api_url}/pricehistorical'
        tsyms = make_list_str(tsyms)
        params = dict(fsym=fsym, tsyms=tsyms, ts=ts, markets=markets)
        return self._make_request(api_url, params)

    def get_historical_day(self, fsym, tsym, e=None, limit=30, toTs=None,
                           allData=False):
        api_url = f'{self.api_url}/histoday'
        params = dict(fsym=fsym, tsym=tsym, e=e, limit=limit, toTs=toTs)
        return self._make_request(api_url, params)

    def get_historical_hour(self, fsym, tsym, e=None, limit=30, toTs=None):
        api_url = f'{self.api_url}/histohour'
        params = dict(fsym=fsym, tsym=tsym, e=e, limit=limit, toTs=toTs)
        return self._make_request(api_url, params)

    def get_historical_minute(self, fsym, tsym, e=None, limit=30, toTs=None):
        api_url = f'{self.api_url}/histominute'
        params = dict(fsym=fsym, tsym=tsym, e=e, limit=limit, toTs=toTs)
        return self._make_request(api_url, params)

    def _make_request(self, api_url, params=None):
        data = super()._make_request(api_url, params)
        if 'Data' in data:
            data = data['Data']
        return data


if __name__=='__main__':
    luno = Luno()
    print(luno.get_list())
    print(luno.get_prices('XBT', 'ZAR'))
    cc = CryptoCompare()
    print(cc.get_list())
    print(cc.get_info('BTC,ETH'))
    print(cc.get_prices('BTC,ETH', 'USD,EUR'))
    print(cc.get_historical_data('BTC,ETH', 'USD,EUR'))
