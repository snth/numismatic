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


LIBRARY_NAME = 'numismatic'
STOP_HANDLERS = object()        # sentinel to signal end of handler processing


@attr.s
class Subscription:
    market_name = attr.ib()
    exchange = attr.ib()
    symbol = attr.ib()
    channel = attr.ib()
    client = attr.ib()
    listener = attr.ib()
    channel_info = attr.ib(default=attr.Factory(dict))
    raw_stream = attr.ib(default=attr.Factory(Stream))
    event_stream = attr.ib(default=attr.Factory(Stream))
    handlers = attr.ib(default=attr.Factory(list))


@attr.s
class Feed(abc.ABC, ConfigMixin):
    "Feed Base class"

    rest_client = attr.ib(default=None)
    websocket_client = attr.ib(default=None)
        
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
    def get_prices(self, assets, currencies):
        return

    def subscribe(self, assets, currencies, channels):
        assets = self._validate_parameter('assets', assets)
        currencies = self._validate_parameter('currencies', currencies)
        channels = self._validate_parameter('channels', channels)
        subscriptions = {}
        for symbol, channel in product(self._get_pairs(assets, currencies),
                                       channels):
            if self.websocket_client is not None:
                subscription = self.websocket_client.listen(symbol, channel)
            else:
                raise ValueError('websocket_client is None')
            subscriptions[subscription.market_name] = subscription
        return subscriptions

    def _validate_parameter(self, parameter, value):
        if not value:
            # value = self.config[parameter]
            value = self.get_config_item(parameter)
        return value.split(',') if isinstance(value, str) else value

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

    def _make_request(self, api_url, params=None, headers=None):
        response = self.requester.get(api_url, params=params, headers=headers)
        data = response.json()
        return data


@attr.s
class WebsocketClient(abc.ABC):
    '''Base class for WebsocketClient feeds'''

    exchange = None
    websocket_url = None
    websocket = attr.ib(default=None)

    def listen(self, symbol, channel=None, websocket_url=None):
        symbol = symbol.upper()
        # set up the subscription
        market_name = f'{self.exchange}--{symbol}--{channel}'
        channel_info = {'channel': channel}
        subscription = Subscription(market_name=market_name,
                                    exchange=self.exchange, 
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
