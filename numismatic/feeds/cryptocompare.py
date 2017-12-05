import math
import logging
import abc
import time
from datetime import timedelta
from dateutil.parser import parse
from itertools import product

import attr

from .base import Feed, RestClient
from ..events import PriceUpdate, Ticker
from ..libs.utils import date_range, make_list_str, to_datetime, \
    dates_and_frequencies


logger = logging.getLogger(__name__)


EXCHANGES = ('BTC38,BTCC,BTCE,BTER,Bit2C,Bitfinex,Bitstamp,Bittrex,CCEDK,'
             'Cexio,Coinbase,Coinfloor,Coinse,Coinsetter,Cryptopia,Cryptsy,'
             'Gatecoin,Gemini,HitBTC,Huobi,itBit,Kraken,LakeBTC,LocalBitcoins,'
             'MonetaGo,OKCoin,Poloniex,Yacuna,Yunbi,Yobit,Korbit,BitBay,'
             'BTCMarkets,QuadrigaCX,CoinCheck,BitSquare,Vaultoro,'
             'MercadoBitcoin,Unocoin,Bitso,BTCXIndia,Paymium,TheRockTrading,'
             'bitFlyer,Quoine,Luno,EtherDelta,Liqui,bitFlyerFX,BitMarket,'
             'LiveCoin,Coinone,Tidex,Bleutrade,EthexIndia'
             ).split(',')

@attr.s
class CryptoCompareRestClient(RestClient):
    '''Low level API for CryptoCompare.com

    TODO:
      * This should use the json api to automatically generate the methods
    '''

    exchange = 'CCCAGG'
    base_url = 'https://www.cryptocompare.com/api/data/'
    api_url = 'https://min-api.cryptocompare.com/data'

    def get_coinlist(self):
        api_url = f'{self.base_url}/coinlist'
        coinlist = self._make_request(api_url)
        return coinlist

    def get_price(self, fsym, tsyms, e=None):
        api_url = f'{self.api_url}/price'
        tsyms = make_list_str(tsyms)
        query_str = f'{api_call}?fsym={fsym}&tsyms={tsyms}'
        return self._make_request(api_url, query_str)

    def get_price_multi(self, fsyms, tsyms, e=None):
        api_url = f'{self.api_url}/pricemulti'
        params = dict(fsyms=make_list_str(fsyms), tsyms=make_list_str(tsyms),
                      e=e)
        return self._make_request(api_url, params)

    def get_price_multi_full(self, fsyms, tsyms, e=None, raw=False):
        api_url = f'{self.api_url}/pricemultifull'
        params = dict(fsyms=make_list_str(fsyms), tsyms=make_list_str(tsyms),
                      e=e)
        return self._make_request(api_url, params, raw=raw)

    def get_price_historical(self, fsym, tsyms, ts, markets=None):
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

    def _make_request(self, api_url, params=None, raw=False):
        data = super()._make_request(api_url, params, raw=raw)
        if 'Data' in data and not raw:
            data = data['Data']
        return data

    @staticmethod
    def parse_price(msg):
        if isinstance(msg, dict) and \
                set(msg)=={'exchange', 'asset', 'currency', 'price'}:
            event = PriceUpdate(exchange=msg['exchange'],
                                asset=msg['asset'],
                                currency=msg['currency'],
                                price=msg['price'])
            return event

    @staticmethod
    def parse_ticker(msg):
        if isinstance(msg, dict) and \
                {'PRICE', 'VOLUME24HOUR', 'VOLUME24HOURTO', 'OPEN24HOUR',
                 'HIGH24HOUR', 'LOW24HOUR'} <= set(msg):
            event = Ticker(exchange=msg['MARKET'],
                           asset=msg['FROMSYMBOL'],
                           currency = msg['TOSYMBOL'],
                           price=msg['PRICE'],
                           volume_24h=msg['VOLUME24HOUR'],
                           value_24h=msg['VOLUME24HOURTO'],
                           open_24h=msg['OPEN24HOUR'],
                           high_24h=msg['HIGH24HOUR'],
                           low_24h=msg['LOW24HOUR'],)
            return event


@attr.s
class CryptoCompareFeed(Feed):

    _interval_limit = 2000
    _rest_client_class = CryptoCompareRestClient

    def get_list(self, **kwargs):
        return self.rest_client.get_coinlist()
        return coinlist.keys()

    def get_info(self, assets, **kwargs):
        assets = self._validate_parameter('assets', assets)
        coinlist = self.rest_client.get_coinlist()
        assets_info = [coinlist[a] for a in assets]
        return assets_info

    def get_prices(self, assets, currencies, raw=False, exchange=None,
                   **kwargs):
        assets = self._validate_parameter('assets', assets)
        currencies = self._validate_parameter('currencies', currencies)
        exchange = exchange if exchange else self.rest_client.exchange
        # FIXME: SHouldn't use caching
        data = self.rest_client.get_price_multi(fsyms=assets, tsyms=currencies,
                                                e=exchange)
        if data:
            prices = [{'exchange':exchange, 'asset':asset, 'currency':currency,
                       'price':price, }
                      for asset, asset_prices in data.items()
                      for currency, price in asset_prices.items()]
        else:
            prices = []
        if not raw:
            prices = [self.rest_client.parse_price(msg) for msg in prices]
        return prices

    def get_tickers(self, assets, currencies, raw=False, exchange=None,
                    **kwargs):
        assets = self._validate_parameter('assets', assets)
        currencies = self._validate_parameter('currencies', currencies)
        # FIXME: SHouldn't use caching
        data = self.rest_client.get_price_multi_full(fsyms=assets,
                                                     tsyms=currencies,
                                                     e=exchange)
        if data:
            tickers = [msg if raw else self.rest_client.parse_ticker(msg)
                       for asset, asset_updates in data['RAW'].items()
                       for currency, msg in asset_updates.items()]
        else:
            tickers = []
        return tickers

    def get_historical_data(self, assets, currencies, freq='d', end_date=None,
                            start_date=-30, exchange=None):
        assets = self._validate_parameter('assets', assets)
        currencies = self._validate_parameter('currencies', currencies)
        start_date, end_date, freqstr, intervals = \
            dates_and_frequencies(start_date, end_date, freq)
        limit = min(intervals, self._interval_limit)
        dates = date_range(start_date, end_date, **{freqstr:limit})

        data = []
        for asset, currency, (start, end) in \
                product(assets, currencies, zip(dates[:-1], dates[1:])):
            asset = asset.upper()
            currency = currency.upper()
            toTs = math.ceil(end.timestamp())
            limit = math.ceil((end-start)/timedelta(**{freqstr:1}))
            logger.debug(f'Getting {asset}/{currency} for {limit}{freqstr} to {end}')
            time.sleep(1/4)   # max 4 requests per second
            if freq.startswith('m'):
                chunk = self.rest_client.get_historical_minute(
                    fsym=asset, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            elif freq.startswith('h'):
                chunk = self.rest_client.get_historical_hour(
                    fsym=asset, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            elif freq.startswith('d'):
                chunk = self.rest_client.get_historical_day(
                    fsym=asset, tsym=currency, e=exchange, limit=limit,
                    toTs=toTs)
            else:
                raise NotImplementedError(f'freq={freq}')
            annotated_chunk = [{**{'asset':asset, 'currency':currency},
                                **item} for item in chunk]
            data.extend(annotated_chunk)
        return data
