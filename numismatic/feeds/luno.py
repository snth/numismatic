from itertools import product
import logging
import asyncio
import json
import time

import attr
import websockets

from ..libs.events import Heartbeat, Trade, LimitOrder, CancelOrder
from .base import Feed, RestClient, WebsocketClient


logger = logging.getLogger(__name__)


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
    wss_url = 'wss://ws.luno.com/api/1/stream'
    exchange = 'Luno'

    api_key_id = attr.ib(default=None)
    api_key_secret = attr.ib(default=None, repr=False)


    async def listen(self, symbol):
        symbol = symbol.upper()
        await super().listen(symbol)
        ws = await self._subscribe(symbol)
        while True:
            try:
                packet = await ws.recv()
                msg = self._handle_packet(packet, symbol)
            except asyncio.CancelledError:
                ## unsubscribe
                confirmation = await self._unsubscribe(ws, symbol)
            except Exception as ex:
                logger.error(ex)
                logger.error(packet)
                raise

    async def _subscribe(self, symbol):
        wss_url = f'{self.wss_url}/{symbol}'
        logger.info(f'Connecting to {wss_url} ...')
        ws = await websockets.connect(wss_url)
        credentials = dict(api_key_id=self.api_key_id,
                           api_key_secret=self.api_key_secret)
        await ws.send(json.dumps(credentials))
        packet = await ws.recv()
        self._handle_order_book(packet, symbol)
        return ws

    @classmethod
    async def _unsubscribe(cls, ws, symbol):
        return True

    def _handle_order_book(self, packet, symbol):
        timestamp = time.time()
        super()._handle_packet(packet, symbol)
        order_book = json.loads(packet)
        if 'asks' in order_book:
            sign = -1
            for order in order_book['asks']:
                id = order['id']
                volume = float(order['volume'])
                price = sign * float(order['price'])
                order_ev = LimitOrder(exchange=self.exchange, symbol=symbol,
                                    timestamp=timestamp, price=price,
                                    volume=volume, id=id)
                self.output_stream.emit(order_ev)
        if 'bids' in order_book:
            sign = 1
            for order in order_book['bids']:
                id = order['id']
                volume = float(order['volume'])
                price = sign * float(order['price'])
                order_ev = LimitOrder(exchange=self.exchange, symbol=symbol,
                                    timestamp=timestamp, price=price,
                                    volume=volume, id=id)
                self.output_stream.emit(order_ev)
        return order_book

    def _handle_packet(self, packet, symbol):
        super()._handle_packet(packet, symbol)
        msg = json.loads(packet)
        if not msg:
            # sometimes we receive empty packets
            return msg
        # TODO: Implement handling of sequence numbers for detecting missing
        #       events
        timestamp = float(msg['timestamp'])/1000
        if 'trade_updates' in msg and msg['trade_updates']:
            for trade in msg['trade_updates']:
                volume = float(trade['base'])
                value = float(trade['counter'])
                price = value/volume
                id = trade['order_id']
                trade_ev = Trade(exchange=self.exchange, symbol=symbol,
                                 timestamp=timestamp, price=price,
                                 volume=volume, id=id)
                self.output_stream.emit(trade_ev)
        if 'create_update' in msg and msg['create_update']:
            order = msg['create_update']
            sign = 1 if order['type']=='BID' else -1
            id = order['order_id']
            volume = float(order['volume'])
            price = sign * float(order['price'])
            order_ev = LimitOrder(exchange=self.exchange, symbol=symbol,
                                  timestamp=timestamp, price=price,
                                  volume=volume, id=id)
            self.output_stream.emit(order_ev)
        if 'delete_update' in msg and msg['delete_update']:
            delete_update = msg['delete_update']
            id = delete_update['order_id']
            cancel_ev = CancelOrder(exchange=self.exchange, symbol=symbol,
                                    timestamp=timestamp, id=id)
            self.output_stream.emit(cancel_ev)
        return msg


if __name__=='__main__':
    # Simple example of how these should be used
    # Test with: python -m numismatic.exchanges.luno
    from configparser import ConfigParser
    from pathlib import Path
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

