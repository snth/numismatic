import logging
import time
import asyncio
import abc
from pathlib import Path
import gzip

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
    channel_info = attr.ib(default=attr.Factor(dict))
    raw_stream = attr.ib(default=attr.Factory(Stream))
    event_stream = attr.ib(default=attr.Factory(Stream))
    handlers = attr.ib(default=attr.Factory(list))


@attr.s
class Feed(abc.ABC):
    "Feed Base class"

    rest_client = attr.ib(default=None)
    websocket_client = attr.ib(default=None)
        
    @abc.abstractmethod
    def get_list(self):
        return

    @abc.abstractmethod
    def get_info(self, assets):
        return

    @abc.abstractmethod
    def get_prices(self, assets, currencies):
        return

    def __getattr__(self, attr):
        if self.rest_client is not None and hasattr(self.rest_client, attr):
            return getattr(self.rest_client, attr)
        elif self.websocket_client is not None and hasattr(self.websocket_client, attr):
            return getattr(self.websocket_client, attr)
        else:
            raise AttributeError


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
    # TODO: Write to a separate stream
    websocket = attr.ib(default=None)
    subscriptions = attr.ib(default=attr.Factory(dict))

    @classmethod
    def get_handlers(cls):
        return [getattr(cls, attr) for attr in dir(cls)
                if callable(getattr(cls, attr)) 
                and attr.startswith('handle_')]

    async def _connect(self, wss_url=None):
        if wss_url is None:
            wss_url = self.wss_url
        logger.info(f'Connecting to {wss_url!r} ...')
        self.websocket = await websockets.connect(wss_url)
        if hasattr(self, 'on_connect'):
            await self.on_connect(ws)
        return self.websocket

    @abc.abstractmethod
    async def _subscribe(self, symbol, channel=None, wss_url=None):
        # connect
        ws = await self._connect(wss_url)
        channel_info = {'channel': channel}
        # set up the subscription
        market_name = f'{self.exchange_name}--{symbol}--{channel}'
        subscription = Subscription(market_name=market_name,
                                    exchang=self.exchange, symbol=symbol,
                                    channel_info=channel_info,
                                    handlers=self.get_handlers())
        self.subscriptions[market_name] = subscription
        return subscription

    async def _unsubscribe(self, subscription):
        del self.subscriptions[subscription.market_name]
        return True

    async def listen(self, symbol, channel=None, wss_url=None):
        symbol = symbol.upper()
        subscription = await self._subscribe(symbol,  channel, wss_url)
        while True:
            try:
                packet = await ws.recv()
                msg = self._handle_packet(packet, subscription)
            except asyncio.CancelledError:
                ## unsubscribe
                confirmation = \
                    await asyncio.shield(self._unsubscribe(subscription))
            except Exception as ex:
                logger.error(ex)
                logger.error(packet)
                raise
             

    @abc.abstractmethod
    def _handle_packet(self, packet, subscription):
        # record the raw packets on the raw_stream
        subscription.raw_stream.emit(packet)
        try:
            msg = json.loads(packet)
        except:
            msg = packet
        if not msg:
            return
        # most of the time we get json so only decode that once
        for handler in subscription.handlers:
            handler(msg, subscription)
