import logging
from itertools import product, chain
import math
import time
from datetime import datetime, timedelta
from dateutil.parser import parse

from pathlib import Path
import asyncio

from .requesters import Requester

logger = logging.getLogger(__name__)


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

class CoinCollector:
    "Base class"

    @classmethod
    def factory(cls, collector_name, *args, **kwargs):
        if not isinstance(collector_name, str):
            raise TypeError(f'"collector_name" must be a str. '
                            'Not {type(collector_name)}.')
        collector_name = collector_name.lower()
        if collector_name=='cryptocompare':
            collector = CryptoCompare(*args, **kwargs)
        elif collector_name=='bfxdata':
            collector = BFXData(*args, **kwargs)
        elif collector_name=='gdax':
            collector = GDAX(*args, **kwargs)
        else:
            raise NotImplementedError(f'collector_name: {collector_name}')
        return collector

    def __init__(self, requester='base', cache_dir=None):
        # TODO: Use attrs here
        if not isinstance(requester, Requester):
            requester = Requester.factory(requester, cache_dir=cache_dir)
        self.requester = requester
        self.cache_dir = cache_dir

    def _make_request(self, api_url, params=None):
        response = self.requester.get(api_url, params=params)
        return response

    @staticmethod
    def to_datetime(datelike, origin=None):
        if datelike is None:
            return datetime.now()
        elif isinstance(datelike, str):
            return parse(datelike)
        else:
            raise TypeError(f'{datelike}')

    @staticmethod
    def _validate_dates(start_date, end_date, freq):
        freqmap = dict(d='days', h='hours', m='minutes', s='seconds',
                       ms='milliseconds', us='microseconds')
        freqstr = freqmap[freq]
        end_date = CoinCollector.to_datetime(end_date)
        if isinstance(start_date, int):
            start_date = end_date + timedelta(**{freqstr:start_date})
        else:
            start_date = CoinCollector.to_datetime(start_date)
        interval_time = timedelta(**{freqstr:1})
        intervals = math.ceil((end_date-start_date)/interval_time)
        return start_date, end_date, freqstr, intervals


class CryptoCompare(CoinCollector):
    '''Low level API for CryptoCompare.com

    TODO:
      * This should use the json api to automatically generate the methods
    '''

    base_url = 'https://www.cryptocompare.com/api/data/'
    api_url = 'https://min-api.cryptocompare.com/data'
    _interval_limit = 2000

    def __init__(self, requester='basic', cache_dir=None):
        super().__init__(requester=requester, cache_dir=cache_dir)

    def get_prices(self, coins, currencies):
        coins = coins.upper().split(',')
        currencies = currencies.upper().split(',')
        # FIXME: SHouldn't use caching
        data = self.get_latest_price_multi(coins, currencies)
        prices = [{'coin':coin, 'currency':currency, 'price':price}
                  for coin, coin_prices in data.items()
                  for currency, price in coin_prices.items()]
        return prices

    def get_historical_data(self, coin, currency, freq='d', end_date=None,
                            start_date=-30, exchange=None):
        coin = coin.upper()
        currency = currency.upper()
        start_date, end_date, freqstr, intervals = \
            self._validate_dates(start_date, end_date, freq)
        limit = min(intervals, self._interval_limit)
        dates = date_range(start_date, end_date, timedelta(**{freqstr:limit}))

        data = []
        for start, end in zip(dates[:-1], dates[1:]):
            toTs = math.ceil(end.timestamp())
            limit = math.ceil((end-start)/timedelta(**{freqstr:1}))
            logger.debug(f'Getting {coin}/{currency} for {limit}{freqstr} to {end}')
            time.sleep(1/4)   # max 4 requests per second
            if freq.startswith('m'):
                chunk = self.get_historical_minute(
                    fsym=coin, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            elif freq.startswith('h'):
                chunk = self.get_historical_hour(
                    fsym=coin, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            elif freq.startswith('d'):
                chunk = self.get_historical_day(
                    fsym=coin, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            else:
                raise NotImplementedError(f'freq={freq}')
            data.extend(chunk)
        return data

    def get_list(self):
        api_url = f'{self.base_url}/coinlist'
        return self._make_request(api_url)

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
        response = super()._make_request(api_url, params)
        data = response.json()
        if 'Data' in data:
            data = data['Data']
        return data
