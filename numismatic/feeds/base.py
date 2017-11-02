import logging
import time
import asyncio
import abc
from pathlib import Path
import gzip
from itertools import product
from functools import partial
import json

from streamz import Stream
import attr
import websockets

from ..requesters import Requester
from ..config import ConfigMixin

logger = logging.getLogger(__name__)


STOP_HANDLERS = object()        # sentinel to signal end of handler processing

# TODO:
#   * Websocket Client vs Subscription --> clarify and unify
#   * Rename symbol to pair
#  Terms
#   * Asset: BTC
#   * Currency: USD
#   * Pair: BTC--USD
#   * Market: Bitfinex--BTC--USD
#   * Channel: trades or orders
#   * Subscription: Trades--Bitfinex--BTC--USD


@attr.s
class Subscription:
    exchange = attr.ib()
    symbol = attr.ib()
    channel = attr.ib()
    client = attr.ib()
    listener = attr.ib()
    channel_info = attr.ib(default=attr.Factory(dict))
    raw_stream = attr.ib(default=attr.Factory(Stream))
    event_stream = attr.ib(default=attr.Factory(Stream))
    handlers = attr.ib(default=attr.Factory(list))

    @property
    def market_name(self):
        return f'{self.exchange}--{self.symbol}--{self.channel}'


@attr.s
class Feed(abc.ABC, ConfigMixin):
    "Feed Base class"

    _rest_client_class = None
    _websocket_client_class = None

    rest_client = attr.ib(default=None)
    websocket_client = attr.ib(default=None)
    cache_dir = attr.ib(default=None)
    requester = attr.ib(default='base')
    websocket = attr.ib(default=None)

    @rest_client.validator
    def _rest_client_validator(self, attribute, value):
        self.rest_client = None if self._rest_client_class is None else \
            self._rest_client_class(cache_dir=self.cache_dir,
                                    requester=self.requester)

    @websocket_client.validator
    def _websocket_client_validator(self, attribute, value):
        self.websocket_client = None if self._websocket_client_class is None \
            else self._websocket_client_class(websocket=self.websocket)
        
    @staticmethod
    def get_symbol(asset, currency):
        return f'{asset}{currency}'

    @abc.abstractmethod
    def get_list(self):
        return

    @abc.abstractmethod
    def get_info(self, assets):
        return

    @abc.abstractmethod
    def get_prices(self, assets, currencies, raw=False):
        return

    @abc.abstractmethod
    def get_tickers(self, assets, currencies, raw=False):
        return

    @classmethod
    def subscribe(cls, assets, currencies, channels):
        assets = cls._validate_parameter('assets', assets)
        currencies = cls._validate_parameter('currencies', currencies)
        channels = cls._validate_parameter('channels', channels)
        subscriptions = {}
        for symbol, channel in product(cls._get_pairs(assets, currencies),
                                       channels):
            if cls._websocket_client_class is not None:
                # Creates a new websocket_client for each subscription
                # FIXME: Allow subscriptions on the same socket
                websocket_client = cls._websocket_client_class()
                subscription = websocket_client.listen(symbol, channel)
            else:
                raise ValueError('websocket_client is None')
            subscriptions[subscription.market_name] = subscription
        return subscriptions

    @classmethod
    def _validate_parameter(cls, parameter, value):
        if not value:
            # value = self.config[parameter]
            value = cls.get_config_item(parameter)
        value_str = value if isinstance(value, str) else ','.join(value)
        return value_str.upper().split(',')

    def __getattr__(self, attr):
        if self.rest_client is not None and hasattr(self.rest_client, attr):
            return getattr(self.rest_client, attr)
        elif self.websocket_client is not None and hasattr(self.websocket_client, attr):
            return getattr(self.websocket_client, attr)
        else:
            raise AttributeError

    @classmethod
    def _get_pairs(cls, assets, currencies):
        for asset, currency in product(assets, currencies):
            yield cls.get_symbol(asset, currency)


@attr.s
class RestClient(abc.ABC):

    cache_dir = attr.ib(default=None)
    requester = attr.ib(default='base')

    @requester.validator
    def __requester_validator(self, attribute, value):
        if isinstance(value, str):
            requester = Requester.factory(value, cache_dir=self.cache_dir)
            setattr(self, attribute.name, requester)
        elif not isinstance(value, Requester):
            raise ValueError(f'{attribute.name}: {value}')

    def _make_request(self, api_url, params=None, headers=None, raw=False):
        response = self.requester.get(api_url, params=params, headers=headers)
        if not raw:
            data = response.json()
        else:
            data = response
        return data


@attr.s
class WebsocketClient(abc.ABC):
    '''Base class for WebsocketClient feeds'''
    # FIXME: Is this really an ABC? What abstractmethods are there?

    exchange = None
    websocket_url = None
    websocket = attr.ib(default=None)

    # FIXME: Should this not be named subscribe?
    def listen(self, symbol, channel=None, websocket_url=None):
        symbol = symbol.upper()
        # set up the subscription
        channel_info = {'channel': channel}
        subscription = Subscription(exchange=self.exchange, 
                                    symbol=symbol,
                                    channel=channel,
                                    channel_info=channel_info, 
                                    client=self,
                                    listener=None,
                                    handlers=self._get_handlers(),
                                    )
        # FIXME: find a better way to do this
        subscription.listener = self._listener(subscription)
        subscription.raw_stream.sink(
            partial(self.__handle_packet, subscription=subscription))
        return subscription

    async def _listener(self, subscription):
        if self.websocket is None:
            self.websocket = await self._connect(subscription)
        await self._subscribe(subscription)
        while True:
            try:
                packet = await self.websocket.recv()
                subscription.raw_stream.emit(packet)
            except asyncio.CancelledError:
                ## unsubscribe
                confirmation = \
                    await asyncio.shield(self._unsubscribe(subscription))
            except Exception as ex:
                logger.error(ex)
                logger.error(packet)
                raise

    async def _connect(self, subscription):
        websocket_url = subscription.client.websocket_url
        logger.info(f'Connecting to {websocket_url!r} ...')
        self.websocket = await websockets.connect(websocket_url)
        if hasattr(self, 'on_connect'):
            await self.on_connect(self.websocket)
        return self.websocket

    async def _subscribe(self, subscription):
        pass

    async def _unsubscribe(self, subscription):
        pass

    @classmethod
    def _get_handlers(cls):
        return [getattr(cls, attr) for attr in dir(cls)
                if callable(getattr(cls, attr)) 
                and attr.startswith('handle_')]
             

    @staticmethod
    def __handle_packet(packet, subscription):
        # most of the time we get json so only decode that once
        try:
            msg = json.loads(packet)
        except:
            msg = packet
            raise
        if not msg:
            return
        for handler in subscription.handlers:
            result = handler(msg, subscription)
            if result is STOP_HANDLERS:
                break
