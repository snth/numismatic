import logging
import websockets
import asyncio
import json
import time
import attr

from .events import Heartbeat, Trade

logger = logging.getLogger(__name__)


@attr.s
class BitfinexExchange:
    '''Websocket client for the Bitfinex Exchange

    This currently opens a separate socket for every symbol that we listen to.
    This could probably be handled by having just one socket.
    '''

    wss_url = 'wss://api.bitfinex.com/ws/2'
    exchange = 'Bitfinex'

    output_stream = attr.ib()


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
            # try ping-pong
            msg = json.dumps({'event':'ping'})
            await ws.send(msg)
            pong = await ws.recv()
            return pong

    async def listen(self, symbol, channel='trades'):
        ws = await self._connect()
        channel_info = await self._subscribe(ws, symbol,  channel)
        while True:
            try:
                packet = await ws.recv()
                msg = self._handle_packet(packet, symbol)
            except asyncio.CancelledError:
                ## unsubscribe
                confirmation = await self._unsubscribe(ws, channel_info)

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

