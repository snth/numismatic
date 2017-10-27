from itertools import product
import logging
import json
import time

import attr
import websockets

from ..events import Heartbeat, Trade, Order
from .base import Feed, RestClient, WebsocketClient, STOP_HANDLERS
from ..config import config


logger = logging.getLogger(__name__)
config = config['Luno']


class LunoFeed(Feed):

    def __init__(self, **kwargs):
        self.rest_client = LunoRestClient(**{a.name:kwargs[a.name] 
                                       for a in attr.fields(LunoRestClient) 
                                       if a.name in kwargs})
        self.websocket_client = LunoWebsocketClient(**{a.name:kwargs[a.name] for a 
                                          in attr.fields(LunoWebsocketClient)
                                          if a.name in kwargs})

    def get_list(self):
        tickers = self.rest_client.get_tickers()
        return [ticker['pair'] for ticker in tickers]

    def get_info(self, assets):
        raise NotImplementedError('Not available for this feed.') 

    def get_prices(self, assets, currencies):
        assets = assets.upper().split(',')
        currencies = currencies.upper().split(',')
        tickers = self.rest_client.get_tickers()
        pairs = {f'{asset}{currency}' for asset, currency in 
                 product(assets, currencies)}
        return [ticker for ticker in tickers if ticker['pair'] in pairs]


class LunoRestClient(RestClient):

    api_url = 'https://api.mybitx.com/api/1/'

    def get_tickers(self):
        api_url = f'{self.api_url}/tickers'
        data = self._make_request(api_url)
        return data['tickers']


@attr.s
class LunoWebsocketClient(WebsocketClient):
    '''Websocket client for the Luno Exchange

    '''
    exchange = 'Luno'
    websocket_url = 'wss://ws.luno.com/api/1/stream'

    api_key_id = attr.ib(default=None)
    api_key_secret = attr.ib(default=None, repr=False)

    @api_key_id.validator
    def __validate_api_key_id(self, attribute, value):
        self.api_key_id = value if value else config.get('api_key_id', '')

    @api_key_secret.validator
    def __validate_api_key_secret(self, attribute, value):
        self.api_key_secret = value if value else \
            config.get('api_key_secret', '')

    async def _connect(self, subscription):
        websocket_url = f'{self.websocket_url}/{subscription.symbol}'
        logger.info(f'Connecting to {websocket_url!r} ...')
        self.websocket = await websockets.connect(websocket_url)
        if hasattr(self, 'on_connect'):
            await self.on_connect(self.websocket)
        return self.websocket

    async def _subscribe(self, subscription):
        await super()._subscribe(subscription)
        credentials = dict(api_key_id=self.api_key_id,
                           api_key_secret=self.api_key_secret)
        await self.websocket.send(json.dumps(credentials))
        subscription.handlers = [self._handle_order_book]

    @staticmethod
    def _handle_order_book(msg, subscription):
        if 'asks' in msg:
            for order in msg['asks']:
                order_ev = Order(exchange=subscription.exchange,
                                 symbol=subscription.symbol,
                                 price=order['price'],
                                 volume=order['volume'],
                                 type='SELL',
                                 id=order['id'], 
                                 )
                subscription.event_stream.emit(order_ev)
        if 'bids' in msg:
            for order in msg['bids']:
                order_ev = Order(exchange=subscription.exchange,
                                 symbol=subscription.symbol,
                                 price=order['price'],
                                 volume=order['volume'],
                                 type='BUY',
                                 id=order['id'], 
                                 )
                subscription.event_stream.emit(order_ev)
        if 'asks' in msg and 'bids' in msg:
            # restore normal handlers
            subscription.handlers = subscription.client._get_handlers()
            return STOP_HANDLERS

    @staticmethod
    def handle_trades(msg, subscription):
        # TODO: Implement handling of sequence numbers for detecting missing
        #       events
        timestamp = float(msg['timestamp'])/1000
        if 'trade_updates' in msg and msg['trade_updates']:
            for trade in msg['trade_updates']:
                volume = float(trade['base'])
                value = float(trade['counter'])
                price = value/volume
                trade_ev = Trade(exchange=subscription.exchange,
                                 symbol=subscription.symbol,
                                 price=price,
                                 volume=volume,
                                 type='TRADE',
                                 timestamp=timestamp,
                                 id=trade['order_id'],
                                 )
                subscription.event_stream.emit(trade_ev)
            # need to process further handlers so no STOP_HANDLERS

    @staticmethod
    def handle_creates(msg, subscription):
        # TODO: Implement handling of sequence numbers for detecting missing
        #       events
        timestamp = float(msg['timestamp'])/1000
        if 'create_update' in msg and msg['create_update']:
            order = msg['create_update']
            order_ev = Order(exchange=subscription.exchange,
                             symbol=subscription.symbol,
                             price=order['price'],
                             volume=order['volume'],
                             type='BUY' if order['type']=='BID' else 'SELL',
                             timestamp=timestamp,
                             id=order['order_id'],
                             )
            subscription.event_stream.emit(order_ev)
            # need to process further handlers so no STOP_HANDLERS

    @staticmethod
    def handle_deletes(msg, subscription):
        # TODO: Implement handling of sequence numbers for detecting missing
        #       events
        timestamp = float(msg['timestamp'])/1000
        if 'delete_update' in msg and msg['delete_update']:
            order = msg['delete_update']
            cancel_ev = Order(exchange=subscription.exchange,
                              symbol=subscription.symbol,
                              timestamp=timestamp,
                              type='CANCEL',
                              id=order['order_id'],
                              )
            subscription.event_stream.emit(cancel_ev)
            # need to process further handlers so no STOP_HANDLERS


if __name__=='__main__':
    # Simple example of how these should be used
    # Test with: python -m numismatic.exchanges.luno
    from configparser import ConfigParser
    from pathlib import Path
    import asyncio
    from streamz import Stream
    logging.basicConfig(level=logging.INFO)
    config = ConfigParser()
    config.read(Path.home() / '.coinrc')
    print(list(config.keys()))
    api_key_id = config['Luno'].get('api_key_id', '')
    api_key_secret = config['Luno'].get('api_key_secret', '') 

    output_stream = Stream()
    printer = output_stream.map(print)

    luno = LunoWebsocketClient(output_stream=output_stream, api_key_id=api_key_id,
                            api_key_secret=api_key_secret)
    luno_btc = luno.listen('XBTZAR')

    loop = asyncio.get_event_loop()
    future = asyncio.wait([luno_btc], timeout=15)
    completed, pending = loop.run_until_complete(future)
    for task in pending:
        task.cancel()

