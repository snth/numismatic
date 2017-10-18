import logging
import asyncio
import json
import time
import gzip

from streamz import Stream
import attr
import websockets

from .base import Exchange
from ..libs.events import Heartbeat, Trade, LimitOrder, CancelOrder

logger = logging.getLogger(__name__)


@attr.s
class BitfinexExchange(Exchange):
    '''Websocket client for the Bitfinex Exchange

    This currently opens a separate socket for every symbol that we listen to.
    This could probably be handled by having just one socket.
    '''

    wss_url = 'wss://api.bitfinex.com/ws/2'
    exchange = 'Bitfinex'


    @classmethod
    async def _connect(cls):
        logger.info(f'Connecting to {cls.wss_url!r} ...')
        ws = await websockets.connect(cls.wss_url)
        packet = await ws.recv()
        connection_status = json.loads(packet)
        logger.info(connection_status)
        return ws

    @staticmethod
    async def _ping_pong(ws):
        'Simple ping pong for testing the connection'
        # try ping-pong
        msg = json.dumps({'event':'ping'})
        await ws.send(msg)
        pong = await ws.recv()
        return pong

    async def listen(self, symbol, channel='trades'):
        ws = await self._connect()
        await super().listen(symbol)
        channel_info = await self._subscribe(ws, symbol,  channel)
        while True:
            try:
                packet = await ws.recv()
                msg = self._handle_packet(packet, symbol)
            except asyncio.CancelledError:
                ## unsubscribe
                confirmation = \
                    await asyncio.shield(self._unsubscribe(ws, channel_info))

    async def _subscribe(self, ws, symbol, channel):
        msg = json.dumps(dict(event='subscribe', channel=channel,
                                symbol=symbol))
        logger.info(msg)
        await ws.send(msg)
        while True:
            packet = await ws.recv()
            msg = self._handle_packet(packet, symbol)
            if isinstance(msg, dict) and 'event' in msg and \
                    msg['event']=='subscribed':
                channel_info = msg
                logger.info(channel_info)
                break
        return channel_info 

    async def _unsubscribe(self, ws, channel_info):
        msg = json.dumps(dict(event='unsubscribe', chanId=channel_info['chanId']))
        logger.info(msg)
        await ws.send(msg)
        while True:
            packet = await ws.recv()
            msg = self._handle_packet(packet, channel_info['pair'])
            if isinstance(msg, dict) and 'event' in msg and \
                    msg['event']=='unsubscribed':
                confirmation = msg
                logger.info(confirmation)
                break
        return confirmation

    def _handle_packet(self, packet, symbol):
        super()._handle_packet(packet, symbol)
        msg = json.loads(packet)
        if isinstance(msg, dict) and 'event' in msg:
            pass
        elif isinstance(msg, list):
            if len(msg) in {2,3} and msg[1]=='hb':
                msg = Heartbeat(exchange=self.exchange, symbol=symbol,
                                timestamp=time.time())
                self.output_stream.emit(msg)
            elif len(msg)==3:
                try:
                    channel_id, trade_type, (trade_id, timestamp, volume, price) = msg
                except TypeError as e:
                    logger.error(e)
                    logger.error(msg)
                    raise
                # FIXME: validate the channel_id below
                msg = Trade(exchange=self.exchange, symbol=symbol, 
                            timestamp=timestamp/1000, price=price, volume=volume,
                            id=trade_id)
                self.output_stream.emit(msg)
            elif len(msg)==2 and isinstance(msg[1], list):
                # snapshot
                for (trade_id, timestamp, volume, price) in reversed(msg[1]):
                    msg = Trade(exchange=self.exchange, symbol=symbol, 
                                timestamp=timestamp/1000, price=price, 
                                volume=volume, id=trade_id)
                    self.output_stream.emit(msg)
            else:
                msg = None
        else:
            raise NotImplementedError(msg)
        return msg


if __name__=='__main__':
    # Simple example of how these should be used
    # Test with: python -m numismatic.exchanges.bitfinex
    logging.basicConfig(level=logging.INFO)
    from streamz import Stream
    output_stream = Stream()
    printer = output_stream.map(print)

    bfx = BitfinexExchange(output_stream=output_stream)
    bfx_btc = bfx.listen('BTCUSD', 'trades')

    loop = asyncio.get_event_loop()
    future = asyncio.wait([bfx_btc], timeout=15)
    completed, pending = loop.run_until_complete(future)
    for task in pending:
        task.cancel()
