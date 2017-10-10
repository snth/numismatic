import logging
import asyncio
import json
import time
import abc
from pathlib import Path
from streamz import Stream

import attr
import websockets

from .events import Heartbeat, Trade


logger = logging.getLogger(__name__)

LIBRARY_NAME = 'numismatic'


@attr.s
class Exchange(abc.ABC):
    '''Base class for Exchanges'''
    output_stream = attr.ib()
    raw_stream = attr.ib(default=None)

    @abc.abstractmethod
    async def listen(self, symbol):
        print(self.raw_stream)
        if self.raw_stream is not None:
            if self.raw_stream is True:
                from appdirs import user_cache_dir
                self.raw_stream = user_cache_dir(LIBRARY_NAME)
            filename = f'{self.exchange}_{symbol}_{{date}}.json'
            raw_stream_path = str(Path(self.raw_stream) / filename)
            print(raw_stream_path)

            def write_to_file(packet):
                date = time.strftime('%Y%m%d')
                path = raw_stream_path.format(date=date)
                print(f'Writing to {path} ...')
                with open(path, 'at') as f:
                    f.write(packet)

            self.raw_stream = Stream()
            self.raw_stream.map(write_to_file)
             

    @abc.abstractmethod
    def _handle_packet(self, packet, symbol):
        # record the raw packets on the raw_stream
        if self.raw_stream is not None:
            self.raw_stream.emit(packet)


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
        initial_order_book = json.loads(packet)
        # FIXME: Do something with the initial_order_book
        logger.debug(initial_order_book)
        super()._handle_packet(packet, symbol)
        return ws

    @classmethod
    async def _unsubscribe(cls, ws, symbol):
        return True

    def _handle_packet(self, packet, symbol):
        super()._handle_packet(packet, symbol)
        msg = json.loads(packet)
        self.output_stream.emit(msg)
        # FIXME: handle the packets properly
        return msg



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


if __name__=='__main__':
    # Simple example of how these should be used
    logging.basicConfig(level=logging.INFO)
    from streamz import Stream
    output_stream = Stream()
    printer = output_stream.map(print)

    bfx = BitfinexExchange(output_stream=output_stream)
    bfx_btc = bfx.listen('BTCUSD', 'trades')
    luno = LunoExchange(output_stream=output_stream)
    luno_btc = luno.listen('XBTZAR')

    loop = asyncio.get_event_loop()
    future = asyncio.wait([luno_btc], timeout=15)
    completed, pending = loop.run_until_complete(future)
    for task in pending:
        task.cancel()
