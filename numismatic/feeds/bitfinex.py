import logging
import time
import json

from streamz import Stream
import attr
import websockets

from .base import Feed, WebsocketClient
from ..libs.events import Heartbeat, Trade, LimitOrder, CancelOrder

logger = logging.getLogger(__name__)


class BitfinexFeed(Feed):

    def __init__(self, **kwargs):
        self.rest_client = None
        self.websocket_client = BitfinexWebsocketClient(**{a.name:kwargs[a.name] for a in 
                                              attr.fields(BitfinexWebsocketClient)
                                              if a.name in kwargs})

    def get_list(self):
        raise NotImplemented()

    def get_info(self, assets):
        raise NotImplemented()
 
    def get_prices(self, assets, currencies):
        raise NotImplemented()       


@attr.s
class BitfinexWebsocketClient(WebsocketClient):
    '''Websocket client for the Bitfinex WebsocketClient

    This currently opens a separate socket for every symbol that we listen to.
    This could probably be handled by having just one socket.
    '''

    wss_url = 'wss://api.bitfinex.com/ws/2'
    exchange = 'Bitfinex'

    @classmethod
    async def on_connect(cls, ws):
        packet = await ws.recv()
        connection_status = json.loads(packet)
        logger.info(connection_status)
        return ws

    async def _subscribe(self, symbol, channel='trades', wss_url=None):
        ws, channel_info = await super()._subscribe(symbol, channel, wss_url)
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
        return ws, channel_info

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

    @staticmethod
    async def _ping_pong(ws):
        'Simple ping pong for testing the connection'
        # try ping-pong
        msg = json.dumps({'event':'ping'})
        await ws.send(msg)
        pong = await ws.recv()
        return pong


if __name__=='__main__':
    # Simple example of how these should be used
    # Test with: python -m numismatic.feeds.bitfinex
    logging.basicConfig(level=logging.INFO)
    import asyncio
    from streamz import Stream
    output_stream = Stream()
    printer = output_stream.map(print)

    bfx = BitfinexWebsocketClient(output_stream=output_stream)
    bfx_btc = bfx.listen('BTCUSD', 'trades')

    loop = asyncio.get_event_loop()
    future = asyncio.wait([bfx_btc], timeout=15)
    completed, pending = loop.run_until_complete(future)
    for task in pending:
        task.cancel()
