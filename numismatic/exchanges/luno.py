import logging
import asyncio
import json
import time
import gzip

from streamz import Stream
import attr
import websockets

from numismatic.exchanges import Exchange
from numismatic.libs.events import Heartbeat, Trade, LimitOrder, CancelOrder

logger = logging.getLogger(__name__)

@attr.s
class LunoExchange(Exchange):
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
