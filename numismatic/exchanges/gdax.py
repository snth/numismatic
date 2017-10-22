import logging
import asyncio
import json
import time
from datetime import datetime

from streamz import Stream
import attr
import websockets

from .base import Exchange
from ..libs.events import Heartbeat, Trade, LimitOrder, CancelOrder, \
    MarketDepthUpdate

logger = logging.getLogger(__name__)


@attr.s
class GDAXExchange(Exchange):
    '''Websocket client for the GDAX Exchange

    This currently opens a separate socket for every symbol that we listen to.
    This could probably be handled by having just one socket.
    '''

    wss_url = 'wss://ws-feed.gdax.com'
    exchange = 'GDAX'


    @classmethod
    async def _connect(cls):
        logger.info(f'Connecting to {cls.wss_url!r} ...')
        ws = await websockets.connect(cls.wss_url)
        # packet = await ws.recv()
        # connection_status = json.loads(packet)
        # logger.info(connection_status)
        return ws


    async def listen(self, symbol, channel='ticker'):
        ws = await self._connect()
        await super().listen(symbol)
        channel_info = await self._subscribe(ws, symbol,  channel)
        while True:
            try:
                packet = await ws.recv()
                msg = self._handle_packet(packet, symbol)
            except asyncio.CancelledError:
                ## unsubscribe
                confirmation = await self._unsubscribe(ws, channel_info)

    async def _subscribe(self, ws, symbol, channel):
        msg = dict(type='subscribe', product_ids=symbol.split(','),
                   channels=channel.split(','))
        packet = json.dumps(msg)
        logger.info(packet)
        await ws.send(packet)
        while True:
            packet = await ws.recv()
            msg = self._handle_packet(packet, symbol)
            if isinstance(msg, dict) and 'type' in msg and \
                    msg['type']=='subscriptions':
                channel_info = msg
                logger.info(channel_info)
                break
        return channel_info 

    async def _unsubscribe(self, ws, channel_info):
        symbols = {symbol for channel in channel_info['channels'] 
                   for symbol in channel['product_ids']}
        channels = [channel['name'] for channel in channel_info['channels']]
        msg = dict(type='unsubscribe', product_ids=list(symbols),
                   channels=channels)
        packet = json.dumps(msg)
        logger.info(msg)
        await ws.send(msg)
        while True:
            packet = await ws.recv()
            msg = self._handle_packet(packet, 'N/A')
            break
        return msg

    def _handle_packet(self, packet, symbol):
        super()._handle_packet(packet, symbol)
        msg = json.loads(packet)
        if not isinstance(msg, dict):
            raise TypeError('msg: {msg}'.format(msg=msg))
        if 'product_id' in msg:
            symbol = msg['product_id'].replace('-', '')
        if 'time' in msg:
            dt = datetime.strptime(msg['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
            timestamp = dt.timestamp()
        else:
            timestamp = time.time()

        if 'type' in msg and msg['type']=='heartbeat':
            event = Heartbeat(exchange=self.exchange, symbol=symbol,
                              timestamp=timestamp)
            self.output_stream.emit(event)
        elif 'type' in msg and msg['type']=='ticker' and 'trade_id' in msg:
            sign = -1 if ('side' in msg and msg['side']=='sell') else 1
            price = msg['price']
            volume = sign * msg['last_size'] if 'last_size' in msg else 0
            trade_id = msg['trade_id']
            event = Trade(exchange=self.exchange, symbol=symbol, 
                          timestamp=timestamp, price=price, volume=volume,
                          id=trade_id)
            self.output_stream.emit(event)
        elif 'type' in msg and msg['type']=='snapshot':
            for price, volume in msg['bids']:
                event = MarketDepthUpdate(exchange=self.exchange,
                                          symbol=symbol, timestamp=timestamp,
                                          price=float(price),
                                          volume=float(volume))
                self.output_stream.emit(event)
            for price, volume in msg['asks']:
                event = MarketDepthUpdate(exchange=self.exchange,
                                          symbol=symbol, timestamp=timestamp,
                                          price=float(price), 
                                          volume=-float(volume))
                self.output_stream.emit(event)
        elif 'type' in msg and msg['type']=='l2update':
            for side, price, size in msg['changes']:
                volume = float(size) if side=='buy' else -float(size)
                event = MarketDepthUpdate(exchange=self.exchange,
                                          symbol=symbol, timestamp=timestamp,
                                          price=float(price), 
                                          volume=volume)
                self.output_stream.emit(event)
        elif isinstance(msg, dict):
            event = msg
            self.output_stream.emit(event)
        else:
            raise NotImplementedError(msg)
        return event


if __name__=='__main__':
    # Simple example of how these should be used
    # Test with: python -m numismatic.exchanges.bitfinex
    logging.basicConfig(level=logging.INFO)
    from streamz import Stream
    output_stream = Stream()
    printer = output_stream.map(print)

    bfx = GDAXExchange(output_stream=output_stream)
    bfx_btc = bfx.listen('BTC-USD', 'level2')

    loop = asyncio.get_event_loop()
    future = asyncio.wait([bfx_btc], timeout=15)
    completed, pending = loop.run_until_complete(future)
    for task in pending:
        task.cancel()
