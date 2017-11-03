import logging
import json
import time
from datetime import datetime

from streamz import Stream
import attr
import websockets

from .base import Feed, WebsocketClient, STOP_HANDLERS
from ..events import Heartbeat, Trade, Order

logger = logging.getLogger(__name__)

class PoloniexWebsocketClient(WebsocketClient):
    '''
        Poloniex Websocket client. This uses an
        undocumented websocket_url. The alternative
        is using Poloniex's WAMP protocol. 
    '''
    exchange = 'Poloniex'
    websocket_url = 'wss://api2.poloniex.com/'

    async def _subscribe(self, subscription):
        await super()._subscribe(subscription)

        # Poloniex requires format USDT_<currency symbol>
        # when comparing a USD to a coin
        channel = subscription.channel
        if subscription.symbol[-4:] == '-USD':
            channel = f'USDT_{subscription.symbol[:3]}'

        connection_message = dict(command='subscribe', channel=channel)
        subscription.handlers = subscription.client._get_handlers()

        packet = json.dumps(connection_message)
        logger.info(packet)
        await self.websocket.send(packet)

        return subscription

    @staticmethod
    def handle_trades_and_orders(msg, subscription):
        '''
            Will handle both trades and orders. Why
            not separate them? Because Poloniex returns
            one message with both information embedded
        '''

        PoloniexWebsocketClient._handle_orderbook(msg, subscription)
        PoloniexWebsocketClient._handle_trade(msg, subscription)
        return STOP_HANDLERS

    @staticmethod
    def _handle_trade(msg, subscription):
        '''
            Poloniex trade format:
            ["t","9394200",1,"5545.00000000","0.00009541",1508060546]
            which is a Trade entry (t) and is defined as 
            [trade, tradeId, 0/1 (sell/buy), price, amount, timestamp]
        '''
        if len(msg) != 3:
            return
        if len(msg[2]) != 2:
            return

        # Only interested in trade messages
        if msg[2][1][0] != 't':
           return
        trade_msg = msg[2][1]
        
        event_msg = Trade(exchange=subscription.exchange,
                    symbol=subscription.symbol, 
                    price=float(trade_msg[3]),
                    volume=float(trade_msg[4]),
                    type='SELL' if trade_msg[2] == 0 else 'BUY',
                    timestamp=trade_msg[5],
                    id=trade_msg[1],) #  id comes from the original message


        subscription.event_stream.emit(event_msg)
    
    @staticmethod
    def _handle_orderbook(msg, subscription):
        '''
            Poloniex order book format:
             [148,394056638,[["o",0,"0.07615527","0.34317849"]]]
             which is [currency pair, id, o (orderbook), 0/1 (remove/modify)
             price, quantity]
        '''
        if len(msg) != 3:
            return
        if len(msg[2]) != 2:
            return

        # Only interested in order book messages
        if msg[2][0][0] != 'o':
           return

        orderbook_msg = msg[2][0]

        event_msg = Order(
            exchange=subscription.exchange,
            symbol=subscription.symbol,
            price=orderbook_msg[2],
            volume=float(orderbook_msg[3]),
            # The orderbook type can either be modify or remove
            # for modify, the amount will replace any previous
            # value. I am using buy because I can't find anyway
            # to distuingish and the documentation uses bid
            type='CANCEL' if orderbook_msg[1] == 0 else 'BUY',
            id=msg[1],
        )

        subscription.event_stream.emit(event_msg)

class PoloniexFeed(Feed):

    _websocket_client_class = PoloniexWebsocketClient
  
    @staticmethod
    def get_symbol(asset, currency):
        return f'{asset}-{currency}'

    def get_list(self):
        raise NotImplemented()

    def get_info(self, assets):
        raise NotImplemented()

    def get_prices(self, assets, currencies):
        raise NotImplemented() 