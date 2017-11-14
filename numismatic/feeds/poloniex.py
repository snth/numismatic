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

CHANNEL_ID_MAP = {
    'trollbox': 1001,
    'ticker': 1002,
    'footer': 1003,
    'heartbeat': 1010
}

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
        subscription.handlers = subscription.client._get_handlers()
        connection_message = dict(command='subscribe', channel=subscription.symbol)

        packet = json.dumps(connection_message)
        logger.info(packet)
        await PoloniexWebsocketClient.websocket.send(packet)

        return subscription

    @staticmethod
    def handle_message(msg, subscription):
        channel_id = msg[0]

        if channel_id == CHANNEL_ID_MAP['heartbeat']:
            pass  # do nothing for now
        elif channel_id == CHANNEL_ID_MAP['ticker']:
            pass  # do nothing for now
        else:
            PoloniexWebsocketClient._trades_and_orders(msg, subscription)

    @staticmethod
    def _trades_and_orders(msg, subscription):
        '''
            Will handle both trades and orders. Why
            not separate them? Because Poloniex returns
            one message with both information embedded
        '''
        seq = msg[1]
        for data in msg[2]:
            msg_type = data[0]

            if msg_type == 'i':
                PoloniexWebsocketClient._orderbook(seq, data[1], subscription)
            elif msg_type == 'o':
                PoloniexWebsocketClient._orderbook_removemodify(seq, data, subscription)
            elif msg_type == 't':
                PoloniexWebsocketClient._trade(seq, data, subscription)

        return STOP_HANDLERS

    @staticmethod
    def _orderbook(seq, market_info, subscription):
        '''
            Poloniex order book, this will provide a snapshot
            of the order book
            TODO Put format here
        '''
        for key, value in market_info.items():
            if key == 'orderBook':
                for ask_price, volume in value[0].items():
                    event = Order(
                        exchange=subscription.exchange,
                        symbol=subscription.symbol,
                        price=ask_price,
                        volume=volume,
                        type='ASK',
                        sequence=seq,
                    )
                    subscription.event_stream.emit(event)

                for bid_price, volume in value[1].items():
                    event = Order(
                        exchange=subscription.exchange,
                        symbol=subscription.symbol,
                        price=bid_price,
                        volume=volume,
                        type='BID',
                        sequence=seq,
                    )
                    subscription.event_stream.emit(event)

    @staticmethod
    def _trade(seq, data, subscription):
        '''
            Poloniex trade format:
            ["t","9394200",1,"5545.00000000","0.00009541",1508060546]
            which is a Trade entry (t) and is defined as 
            [trade, tradeId, 0/1 (sell/buy), price, amount, timestamp]
        '''

        event = Trade(exchange=subscription.exchange,
                    symbol=subscription.symbol, 
                    price=data[3],
                    volume=data[4],
                    type='SELL' if data[2] == 0 else 'BUY',
                    timestamp=data[5],
                    sequence=seq,
                    id=data[1],
        )

        subscription.event_stream.emit(event)
    
    @staticmethod
    def _orderbook_removemodify(seq, data, subscription):
        '''
            Poloniex order book format:
             [148,394056638,[["o",0,"0.07615527","0.34317849"]]]
             which is [currency pair, id, o (orderbook), 0/1 (remove/modify)
             price, quantity]
        '''
        if data[3] == '0.00000000':
            event = Order(
                exchange=subscription.exchange,
                symbol=subscription.symbol,
                price=data[2],
                volume=data[3],
                type='CANCEL',
                sequence=seq,
            )
        else:
            # Below is an OrderModify,
            # but represented as an order
            event = Order(
                exchange=subscription.exchange,
                symbol=subscription.symbol,
                price=data[2],
                volume=data[3],
                type='ASK' if data[1] == 0 else 'BID',
                sequence=seq,
            )

        subscription.event_stream.emit(event)

class PoloniexFeed(Feed):
    _websocket_client_class = PoloniexWebsocketClient
  
    @staticmethod
    def get_symbol(asset, currency):
        # Poloniex requires format USDT_<currency symbol>
        # for USD currency.
        currency = currency.replace('USD','USDT',1)
        return f'{currency}_{asset}'

    def get_list(self):
        raise NotImplemented()

    def get_info(self, assets):
        raise NotImplemented()

    def get_prices(self, assets, currencies):
        raise NotImplemented() 
    
    def get_tickers(self, assets, currencies, raw=False):
        raise NotImplemented()
