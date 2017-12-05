import logging
import time
import json

from streamz import Stream
import attr
import websockets

from .base import Feed, WebsocketClient, STOP_HANDLERS
from ..events import Heartbeat, Trade

logger = logging.getLogger(__name__)


@attr.s
class BitfinexWebsocketClient(WebsocketClient):
    '''Websocket client for the Bitfinex WebsocketClient'''

    exchange = 'Bitfinex'
    websocket_url = 'wss://api.bitfinex.com/ws/2'

    @classmethod
    def handle_connect(cls, msg, subscription):
        connection_status = msg
        logger.debug(connection_status)
        subscription.handlers.remove(cls.handle_connect)
        logger.info(f'Removed {cls.__name__}.handle_connect()')
        return STOP_HANDLERS

    async def _subscribe(self, subscription):
        await super()._subscribe(subscription)
        # install only the handle_subscribed handler
        # it needs to go through the main __handle_packet so the raw_stream is
        # updated.
        subscription.handlers = [self.__handle_subscribed]
        msg = json.dumps(dict(event='subscribe', channel=subscription.channel, 
                              symbol=subscription.symbol))
        logger.info(msg)
        await self.websocket.send(msg)
        return subscription

    @staticmethod
    def __handle_subscribed(msg, subscription):
        if isinstance(msg, dict) and 'event' in msg and \
                msg['event']=='subscribed' and \
                msg['pair']==subscription.symbol:
            subscription.channel_info = msg
            # TODO: Make the following a debug log message rather
            logger.info(subscription.channel_info)
            # install the proper handlers
            subscription.handlers = subscription.client._get_handlers()
            # stop processing other handlers
            return STOP_HANDLERS

    async def _unsubscribe(self, subscription):
        msg = json.dumps(dict(event='unsubscribe', 
                              chanId=subscription.channel_info['chanId']))
        logger.info(msg)
        await self.websocket.send(msg)

    @staticmethod
    def __handle_unsubscribed(msg, subscription):
        if isinstance(msg, dict) and 'event' in msg and \
                msg['event']=='unsubscribed' and \
                msg['chanId']==subscription.channel_info['chanId']:
            confirmation = msg
            logger.info(confirmation)
            # disable all handlers
            subscription.handlers = []
            # stop processing other handlers
            return STOP_HANDLERS

    async def _ping_pong(self):
        'Simple ping pong for testing the connection'
        # try ping-pong
        msg = json.dumps({'event':'ping'})
        await self.websocket.send(msg)
        pong = await self.websocket.recv()
        return pong

    @staticmethod
    def handle_heartbeat(msg, subscription):
        if isinstance(msg, list) and \
                msg[0]==subscription.channel_info['chanId'] and \
                msg[1]=='hb':
            msg = Heartbeat(exchange=subscription.exchange,
                            asset=subscription.asset,
                            currency=subscription.currency,
                            timestamp=time.time())
            subscription.event_stream.emit(msg)
            # stop processing other handlers
            return STOP_HANDLERS

    @staticmethod
    def handle_trade(msg, subscription):
        if isinstance(msg, list) and \
                msg[0]==subscription.channel_info['chanId'] and \
                msg[1]=='tu':
                # TODO: There is also the 'te' trade type? What for?
            try:
                channel_id, trade_type, (trade_id, timestamp, volume, price) = msg
            except TypeError as e:
                # for debugging
                logger.error(e)
                logger.error(msg)
                raise
            # FIXME: validate the channel_id below
            msg = Trade(exchange=subscription.exchange, 
                        asset=subscription.asset, 
                        currency=subscription.currency, 
                        price=price,
                        volume=volume,
                        timestamp=timestamp/1000,
                        id=trade_id)
            subscription.event_stream.emit(msg)
            # stop processing other handlers
            return STOP_HANDLERS

    @staticmethod
    def handle_snapshot(msg, subscription):
        if isinstance(msg, list) and \
                msg[0]==subscription.channel_info['chanId'] and \
                isinstance(msg[1], list):
            # snapshot
            for (trade_id, timestamp, volume, price) in reversed(msg[1]):
                msg = Trade(exchange=subscription.exchange,
                            asset=subscription.asset, 
                            currency=subscription.currency, 
                            price=price, 
                            volume=volume,
                            timestamp=timestamp/1000,
                            id=trade_id)
                subscription.event_stream.emit(msg)
            # stop processing other handlers
            return STOP_HANDLERS


class BitfinexFeed(Feed):

    _websocket_client_class = BitfinexWebsocketClient

    def get_list(self, **kwargs):
        raise NotImplemented()

    def get_info(self, assets, **kwargs):
        raise NotImplemented()
 
    def get_prices(self, assets, currencies, **kwargs):
        raise NotImplemented()       
 
    def get_tickers(self, assets, currencies, **kwargs):
        raise NotImplemented()       


if __name__=='__main__':
    # Simple example of how these should be used
    # Test with: python -m numismatic.feeds.bitfinex
    logging.basicConfig(level=logging.INFO)
    import asyncio
    from streamz import Stream
    output_stream = Stream()
    printer = output_stream.map(print)

    bfx = BitfinexWebsocketClient(output_stream=output_stream)
    bfx_btc = bfx.subscribe('BTCUSD', 'trades')

    loop = asyncio.get_event_loop()
    future = asyncio.wait([bfx_btc], timeout=15)
    completed, pending = loop.run_until_complete(future)
    for task in pending:
        task.cancel()
