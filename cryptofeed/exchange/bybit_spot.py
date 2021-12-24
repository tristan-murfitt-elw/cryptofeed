'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import logging
from decimal import Decimal
from functools import partial
from typing import Dict, List, Callable, Tuple

from sortedcontainers import SortedDict as sd
from yapic import json

from cryptofeed.connection import AsyncConnection, WSAsyncConn
from cryptofeed.defines import BID, ASK, BUY, BYBIT_SPOT, L2_BOOK, SELL, TRADES, TICKER
from cryptofeed.feed import Feed
from cryptofeed.standards import timestamp_normalize
from cryptofeed.exchange.bybit import Bybit


LOG = logging.getLogger('feedhandler')


class BybitSpot(Feed):
    id = BYBIT_SPOT
    symbol_endpoint = 'https://api.bybit.com/spot/v1/symbols'

    @classmethod
    def _parse_symbol_data(cls, data: dict, symbol_separator: str) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)
        for symbol in data['result']:
            ret[symbol['name']] = symbol['name']
        return ret, info

    def __init__(self, **kwargs):
        super().__init__('wss://stream.bybit.com/spot/quote/ws/v2', **kwargs)

    def __reset(self, quote=None):
        if quote is None:
            self.l2_book = {}
        else:
            rem = [symbol for symbol in self.l2_book if quote in symbol]
            for symbol in rem:
                del self.l2_book[symbol]

    async def message_handler(self, msg: str, conn, timestamp: float):

        msg = json.loads(msg, parse_float=Decimal)

        if "event" in msg:
            if msg['event'] == 'sub':
                LOG.debug("%s: Subscription success %s", self.id, msg)
            else:
                LOG.error("%s: Error from exchange %s", self.id, msg)
        elif "topic" not in msg:
            LOG.warning("%s: Invalid message (not topic) %s", self.id, msg)
        elif "trade" in msg["topic"]:
            await self._trade(msg, timestamp)
        elif "depth" in msg["topic"]:
            await self._book(msg, timestamp)
        elif "bookTicker" in msg["topic"]:
            await self._ticker(msg, timestamp)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def subscribe(self, connection: AsyncConnection, quote: str = None):
        self.__reset(quote=quote)

        for chan in self.subscription:
            for pair in self.subscription[chan]:
                await connection.write(json.dumps(
                    {
                        "topic": chan,
                        "event": "sub",
                        "params": {
                            "binary": False,
                            "symbol": self.exchange_symbol_to_std_symbol(pair),
                        }
                    }
                ))

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {
            "topic": "bookTicker",
            "params": {
                "symbol": "BTCUSDT",
                "binary": "false",
                "symbolName": "BTCUSDT"
            },
            "data": {
                "symbol": "BTCUSDT",
                "bidPrice": "9797.79",
                "bidQty": "0.177976",
                "askPrice": "9799",
                "askQty": "0.65",
                "time": 1582001830346
            }
        }
        """
        pair = self.exchange_symbol_to_std_symbol(msg['params']['symbol'])
        extra_fields = {
            'bbo': self.get_book_bbo(pair),
            'best_bid_size': Decimal(msg['data']['bidQty']),
            'best_ask_size': Decimal(msg['data']['askQty']),
        }
        await self.callback(TICKER, feed=self.id,
                            symbol=pair,
                            bid=msg['data']['bidPrice'],
                            ask=msg['data']['askPrice'],
                            timestamp=timestamp_normalize(self.id, msg['data']['time']),
                            receipt_timestamp=timestamp,
                            **extra_fields)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "topic": "trade",
            "params": {
                "symbol": "BTCUSDT",
                "binary": "false",
                "symbolName": "BTCUSDT"
            },
            "data": {
                "v": "564265886622695424",
                "t": 1582001735462,
                "p": "9787.5",
                "q": "0.195009",
                "m": true
            }
        }
        """
        await self.callback(TRADES,
                            feed=self.id,
                            symbol=self.exchange_symbol_to_std_symbol(msg['params']['symbol']),
                            order_id=msg['data']['v'],
                            side=BUY if msg['data']['m'] else SELL,
                            amount=Decimal(msg['data']['q']),
                            price=Decimal(msg['data']['p']),
                            timestamp=timestamp_normalize(self.id, msg['data']['t']),
                            receipt_timestamp=timestamp
                            )

    async def _book(self, msg: dict, timestamp: float):
        """
        {
            "topic": "depth",
            "params": {
                "symbol": "BTCUSDT",
                "binary": "false",
                "symbolName": "BTCUSDT"
            },
            "data": {
                "s": "BTCUSDT",
                "t": 1582001376853,
                "v": "13850022_2",
                "b": [
                    [
                        "9780.79",
                        "0.01"
                    ],
                    ...
                ]
                "a": [
                    [
                        "9781.21",
                        "0.042842"
                    ],
                    ...
                ]
            }
        }
        """
        pair = self.exchange_symbol_to_std_symbol(msg['params']['symbol'])
        data = msg['data']
        delta = {BID: [], ASK: []}

        bids = sd({})
        asks = sd({})
        for level in data['b']:
            bids[Decimal(level[0])] = Decimal(level[1])
        for level in data['a']:
            asks[Decimal(level[0])] = Decimal(level[1])
        self.l2_book[pair] = {BID: bids, ASK: asks}
        forced = True

        # timestamp is in microseconds
        ts = timestamp_normalize(self.id, data['t'])
        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, forced, delta, ts, timestamp)
