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
from websockets.client import WebSocketClientProtocol

from ..requesters import Requester
from ..config import ConfigMixin
from ..events import Event

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
    channel_info = attr.ib(default=attr.Factory(dict))
    raw_stream = attr.ib(default=attr.Factory(Stream))
    event_stream = attr.ib(default=attr.Factory(Stream))
    handlers = attr.ib(default=attr.Factory(list))

    @property
    def market_name(self):
        return f'{self.exchange}--{self.symbol}--{self.channel}'

    async def start(self):
        logger.info(f'Starting Subscription {self.market_name!r} ...')
        await self.client._subscribe(self)

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
            else self._websocket_client_class()
        
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

    def subscribe(self, assets, currencies, channels, exchange=None,
                  interval=1.0):
        assets = self._validate_parameter('assets', assets)
        currencies = self._validate_parameter('currencies', currencies)
        channels = self._validate_parameter('channels', channels)
        subscriptions = {}
        for symbol, channel in product(self._get_pairs(assets, currencies),
                                       channels):
            if self._websocket_client_class is not None:
                if self.websocket_client is None:
                    self.websocket_client = self._websocket_client_class()
                subscription = self.websocket_client.listen(symbol, channel)
            elif self._rest_client_class is not None:
                channel_method = getattr(self, f'get_{channel.lower()}')
                rest_client = self._rest_client_class()
                subscription = rest_client.listen(symbol, channel_method,
                                                  interval=interval,
                                                  exchange=exchange)
            else:
                raise ValueError('No listen() method found.')
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
    subscriptions = attr.ib(default=attr.Factory(list), repr=False)

    def listen(self, symbol, channel, interval=1.0, exchange=None):
        exchange = exchange if exchange else self.exchange
        channel_name = f'{exchange}--{symbol}--{channel.__name__}'
        logger.info(f'Subscribing to {channel_name} ...')
        # timer

        # FIXME: get the symbol properly
        # application
        def _get_raw_channel():
            messages = channel(symbol[:3], symbol[3:], exchange=exchange,
                               raw=True)
            packet = '\n'.join(json.dumps(msg) for msg in messages)
            return packet

        channel_info = {'channel': channel.__name__}
        subscription = Subscription(exchange=exchange,
                                    symbol=symbol,
                                    channel='ticker',
                                    channel_info=channel_info, 
                                    client=self,
                                    handlers=self._get_handlers(),
                                    )
        self.subscriptions.append(subscription)
        asyncio.ensure_future(
            self._listener(subscription, interval=interval,
                           callback=_get_raw_channel))
        asyncio.ensure_future(subscription.start())
        logger.info(f'Subscribed to {channel_name} ...')
        return subscription

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
            # FIXME: The RestClient handlers are actually message parsers
            #        more than handlers which is different from how the
            #        WebsocketClients do things. This should be unified and
            #        one approach chosen.
            result = handler(msg)
            if isinstance(result, Event):
                # FIXME: should this raw_stream now rather sit on the
                # WebsocketClient instead of the Subscription?
                subscription.raw_stream.emit(packet)
                subscription.event_stream.emit(result)
                break
            elif result is STOP_HANDLERS:
                break

    @classmethod
    def _get_handlers(cls):
        return [getattr(cls, attr) for attr in dir(cls)
                if callable(getattr(cls, attr)) 
                and attr.startswith('parse_')]

    async def _listener(self, subscription, interval, callback):
        while True:
            try:
                # FIXME: This should use an async requester as below
                packet = callback()
                self.__handle_packet(packet, subscription)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                ## unsubscribe from all subscriptions
                confirmations = await asyncio.gather(
                    asyncio.shield(self._unsubscribe(subscription)) 
                    for subscription in self.subscriptions)
            except Exception as ex:
                logger.error(ex)
                logger.error(packet)
                raise

    async def _subscribe(self, subscription):
        pass

    async def _unsubscribe(self, subscription):
        pass

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

    exchange = attr.ib(default=None)
    websocket_url = attr.ib(default=None)
    websocket = attr.ib(default=None, repr=False)
    subscriptions = attr.ib(default=attr.Factory(list), repr=False)

    def __attrs_post_init__(self):
        if self.exchange is None:
            self.exchange = self.__class__.exchange
        if self.websocket_url is None:
            self.websocket_url = self.__class__.websocket_url
        asyncio.ensure_future(self._connect())
        asyncio.ensure_future(self._listener())

    async def _connect(self):
        '''
            Connects to websocket. Uses a future to ensure that only one
            connection at a time will happen
        '''
        if self.websocket is None or \
                isinstance(self.websocket, WebSocketClientProtocol) and \
                not self.websocket.open:
            logger.info(f'Connecting to {self.websocket_url!r} ...')
            self.websocket = \
                asyncio.ensure_future(websockets.connect(self.websocket_url))
        if isinstance(self.websocket, asyncio.Future):
            self.websocket = await self.websocket
        return self.websocket

    # FIXME: Should this not be named subscribe?
    def listen(self, symbol, channel=None):
        symbol = symbol.upper()
        # set up the subscription
        channel_info = {'channel': channel}
        subscription = Subscription(exchange=self.exchange, 
                                    symbol=symbol,
                                    channel=channel,
                                    channel_info=channel_info, 
                                    client=self,
                                    handlers=self._get_handlers(),
                                    )
        self.subscriptions.append(subscription)
        asyncio.ensure_future(subscription.start())
        return subscription

    async def _listener(self):
        await self._connect()
        while True:
            try:
                packet = await self.websocket.recv()
                self.__handle_packet(packet)
            except websockets.exceptions.ConnectionClosed:
                await self._connect()
            except asyncio.CancelledError:
                ## unsubscribe from all subscriptions
                confirmations = await asyncio.gather(
                    asyncio.shield(self._unsubscribe(subscription)) 
                    for subscription in self.subscriptions)
            except Exception as ex:
                logger.error(ex)
                logger.error(packet)
                raise

    async def _subscribe(self, subscription):
        await self._connect()

    async def _unsubscribe(self, subscription):
        pass

    def __handle_packet(self, packet):
        # most of the time we get json so only decode that once
        try:
            msg = json.loads(packet)
        except:
            msg = packet
            raise
        if not msg:
            return
        for subscription in self.subscriptions:
            for handler in subscription.handlers:
                result = handler(msg, subscription)
                if result is STOP_HANDLERS:
                    # FIXME: should this raw_stream now rather sit on the
                    # WebsocketClient instead of the Subscription?
                    subscription.raw_stream.emit(packet)
                    break

    @classmethod
    def _get_handlers(cls):
        return [getattr(cls, attr) for attr in dir(cls)
                if callable(getattr(cls, attr)) 
                and attr.startswith('handle_')]
