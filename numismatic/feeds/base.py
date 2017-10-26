import logging
import time
import asyncio
import abc
from pathlib import Path
import gzip
from itertools import product
import json

from streamz import Stream
import attr
import websockets

from ..libs.requesters import Requester

logger = logging.getLogger(__name__)


LIBRARY_NAME = 'numismatic'


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
class Feed(abc.ABC):
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
        assets = ','.join(assets).split(',')
        currencies = ','.join(currencies).split(',')
        channels = ','.join(channels).split(',')
        subscriptions = {}
        for symbol, channel in product(self._get_pairs(assets, currencies),
                                       channels):
            if self.websocket_client is not None:
                subscription = self.websocket_client.listen(symbol, channel)
            else:
                raise NotImplementedError
            subscriptions[subscription.market_name] = subscription
        return subscriptions


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
    wss_url = None
    websocket = attr.ib(default=None)

    def listen(self, symbol, channel=None, wss_url=None):
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
        return subscription

    async def _listener(self, subscription):
        if self.websocket is None:
            self.websocket = await self._connect(subscription)
        await self._subscribe(subscription)
        while True:
            try:
                packet = await self.websocket.recv()
                msg = self.__handle_packet(packet, subscription)
            except asyncio.CancelledError:
                ## unsubscribe
                confirmation = \
                    await asyncio.shield(self._unsubscribe(subscription))
            except Exception as ex:
                logger.error(ex)
                logger.error(packet)
                raise

    async def _connect(self, subscription):
        wss_url = subscription.client.wss_url
        logger.info(f'Connecting to {wss_url!r} ...')
        self.websocket = await websockets.connect(wss_url)
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
        # record the raw packets on the raw_stream
        subscription.raw_stream.emit(packet)
        try:
            msg = json.loads(packet)
        except:
            msg = packet
            raise
        if not msg:
            return
        # FIXME: the code below should be moved to _handle_message
        # most of the time we get json so only decode that once
        for handler in subscription.handlers:
            handler(msg, subscription)
