'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import logging
import time
import asyncio
from decimal import Decimal
from typing import Dict, Tuple

from sortedcontainers import SortedDict as sd
from yapic import json

from cryptofeed.connection import AsyncConnection
from cryptofeed.defines import BID, ASK, BUY, FUNDING, KRAKEN_FUTURES, L2_BOOK, OPEN_INTEREST, UNDERLYING_INDEX, SELL, TICKER, TRADES
from cryptofeed.exceptions import MissingSequenceNumber
from cryptofeed.feed import Feed
from cryptofeed.standards import feed_to_exchange, timestamp_normalize


LOG = logging.getLogger('feedhandler')


class KrakenFutures(Feed):
    id = KRAKEN_FUTURES
    api = 'https://futures.kraken.com/derivatives/api'
    symbol_endpoint = 'https://futures.kraken.com/derivatives/api/v3/instruments'
    index_product_type = 'Real Time Index'
    index_product_prefix = 'in'

    @classmethod
    def _parse_symbol_data(cls, data: dict, symbol_separator: str) -> Tuple[Dict, Dict]:
        _kraken_futures_product_type = {
            'FI': 'Inverse Futures',
            'FV': 'Vanilla Futures',
            'PI': 'Perpetual Inverse Futures',
            'PV': 'Perpetual Vanilla Futures',
            'IN': cls.index_product_type,
            'RR': 'Reference Rate',
        }
        ret = {}
        info = defaultdict(dict)

        data = data['instruments']
        for entry in data:
            normalized = entry['symbol'].upper().replace("_", "-")
            symbol = normalized[3:6] + symbol_separator + normalized[6:9]
            normalized = normalized.replace(normalized[3:9], symbol)
            normalized = normalized.replace('XBT', 'BTC')
            
            if entry['tradeable']:
                info['tick_size'][normalized] = entry['tickSize']
                info['contract_size'][normalized] = entry['contractSize']
                info['underlying'][normalized] = entry['underlying']
                info['product_type'][normalized] = _kraken_futures_product_type[normalized[:2]]
            else:
                if entry['symbol'][:2] == cls.index_product_prefix: # Index
                    normalized = f'.{normalized}'
                    info['product_type'][normalized] = _kraken_futures_product_type[normalized[1:3]]
                else:
                    continue
            ret[normalized] = entry['symbol']
        return ret, info

    @classmethod
    def product_type(cls, symbol: str):
        return cls.info()['product_type'][symbol]

    def __init__(self, **kwargs):
        super().__init__('wss://futures.kraken.com/ws/v1', **kwargs)
        self.__reset()

    def __reset(self):
        self.open_interest = {}
        self.l2_book = {}
        self.seq_no = {}

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        for chan in self.subscription:
            if chan == feed_to_exchange(self.id, UNDERLYING_INDEX):
                asyncio.create_task(self._index_price(self.subscription[chan]))
            else:
                await conn.write(json.dumps(
                    {
                        "event": "subscribe",
                        "feed": chan,
                        "product_ids": self.subscription[chan]
                    }
                ))

    async def _trade(self, msg: dict, pair: str, timestamp: float):
        """
        {
            "feed": "trade",
            "product_id": "PI_XBTUSD",
            "uid": "b5a1c239-7987-4207-96bf-02355a3263cf",
            "side": "sell",
            "type": "fill",
            "seq": 85423,
            "time": 1565342712903,
            "qty": 1135.0,
            "price": 11735.0
        }
        """
        await self.callback(TRADES, feed=self.id,
                            symbol=pair,
                            side=BUY if msg['side'] == 'buy' else SELL,
                            amount=Decimal(msg['qty']),
                            price=Decimal(msg['price']),
                            order_id=msg['uid'],
                            timestamp=timestamp_normalize(self.id, msg['time']),
                            receipt_timestamp=timestamp)

    async def _ticker(self, msg: dict, pair: str, timestamp: float):
        """
        {
            "feed": "ticker_lite",
            "product_id": "PI_XBTUSD",
            "bid": 11726.5,
            "ask": 11732.5,
            "change": 0.0,
            "premium": -0.1,
            "volume": "7.0541503E7",
            "tag": "perpetual",
            "pair": "XBT:USD",
            "dtm": -18117,
            "maturityTime": 0
        }
        """
        extra_fields = {
            'bbo': self.get_book_bbo(pair),
            'last': Decimal(msg.get('last', 0)),
            'volume': Decimal(msg.get('volume', 0)),
            'bid_size': Decimal(msg.get('bid_size', 0)),
            'ask_size': Decimal(msg.get('ask_size', 0)),
            'mark_price': Decimal(msg.get('markPrice', 0)),
        }
        await self.callback(TICKER, feed=self.id,
                            symbol=pair,
                            bid=msg['bid'],
                            ask=msg['ask'],
                            timestamp=timestamp,
                            receipt_timestamp=timestamp,
                            **extra_fields)

    async def _book_snapshot(self, msg: dict, pair: str, timestamp: float):
        """
        {
            "feed": "book_snapshot",
            "product_id": "PI_XBTUSD",
            "timestamp": 1565342712774,
            "seq": 30007298,
            "bids": [
                {
                    "price": 11735.0,
                    "qty": 50000.0
                },
                ...
            ],
            "asks": [
                {
                    "price": 11739.0,
                    "qty": 47410.0
                },
                ...
            ],
            "tickSize": null
        }
        """
        self.l2_book[pair] = {
            BID: sd({Decimal(update['price']): Decimal(update['qty']) for update in msg['bids']}),
            ASK: sd({Decimal(update['price']): Decimal(update['qty']) for update in msg['asks']})
        }
        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, True, None, timestamp, timestamp)

    async def _book(self, msg: dict, pair: str, timestamp: float):
        """
        Message is received for every book update:
        {
            "feed": "book",
            "product_id": "PI_XBTUSD",
            "side": "sell",
            "seq": 30007489,
            "price": 11741.5,
            "qty": 10000.0,
            "timestamp": 1565342713929
        }
        """
        if pair in self.seq_no and self.seq_no[pair] + 1 != msg['seq']:
            raise MissingSequenceNumber
        self.seq_no[pair] = msg['seq']

        delta = {BID: [], ASK: []}
        s = BID if msg['side'] == 'buy' else ASK
        price = Decimal(msg['price'])
        amount = Decimal(msg['qty'])

        if amount == 0:
            delta[s].append((price, 0))
            del self.l2_book[pair][s][price]
        else:
            delta[s].append((price, amount))
            self.l2_book[pair][s][price] = amount

        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, False, delta, timestamp, timestamp)

    async def _funding(self, msg: dict, pair: str, timestamp: float):
        if msg['tag'] == 'perpetual':
            await self.callback(FUNDING,
                                feed=self.id,
                                symbol=pair,
                                timestamp=timestamp_normalize(self.id, msg['time']),
                                receipt_timestamp=timestamp,
                                tag=msg['tag'],
                                rate=msg['funding_rate'],
                                rate_prediction=msg.get('funding_rate_prediction', None),
                                relative_rate=msg['relative_funding_rate'],
                                relative_rate_prediction=msg.get('relative_funding_rate_prediction', None),
                                next_rate_timestamp=timestamp_normalize(self.id, msg['next_funding_rate_time']))
        else:
            await self.callback(FUNDING,
                                feed=self.id,
                                symbol=pair,
                                timestamp=timestamp_normalize(self.id, msg['time']),
                                receipt_timestamp=timestamp,
                                tag=msg['tag'],
                                premium=msg['premium'],
                                maturity_timestamp=timestamp_normalize(self.id, msg['maturityTime']))

        oi = msg['openInterest']
        if pair in self.open_interest and oi == self.open_interest[pair]:
            return
        self.open_interest[pair] = oi
        await self.callback(OPEN_INTEREST,
                            feed=self.id,
                            symbol=pair,
                            open_interest=msg['openInterest'],
                            timestamp=timestamp_normalize(self.id, msg['time']),
                            receipt_timestamp=timestamp
                            )

    async def _index_price(self, pairs: list):
        # Continously poll list of tickers from the REST API to get most recent index price
        last_update = {}

        while True:
            # Fetch all tickers
            end_point = f"{self.api}/v3/tickers"
            data = await self.http_conn.read(end_point)
            data = json.loads(data, parse_float=Decimal)
            timestamp = time.time()
            for ticker in data['tickers']:
                if ticker['symbol'] not in pairs or self.product_type(self.exchange_symbol_to_std_symbol(ticker['symbol'])) != self.index_product_type:
                    continue
                
                # Check if values have changed
                if ticker == last_update.get(ticker['symbol']):
                    continue

                await self.callback(UNDERLYING_INDEX,
                                            feed=self.id,
                                            symbol=self.exchange_symbol_to_std_symbol(ticker['symbol']),
                                            timestamp=ticker['lastTime'].timestamp(),
                                            receipt_timestamp=timestamp,
                                            price=Decimal(ticker['last'])
                                            )
                last_update[ticker['symbol']] = ticker
            await asyncio.sleep(1)

    async def message_handler(self, msg: str, conn, timestamp: float):

        msg = json.loads(msg, parse_float=Decimal)

        if 'event' in msg:
            if msg['event'] == 'info':
                return
            elif msg['event'] == 'subscribed':
                return
            else:
                LOG.warning("%s: Invalid message type %s", self.id, msg)
        else:
            # As per Kraken support: websocket product_id is uppercase version of the REST API symbols
            pair = self.exchange_symbol_to_std_symbol(msg['product_id'].lower())
            if msg['feed'] == 'trade':
                await self._trade(msg, pair, timestamp)
            elif msg['feed'] == 'trade_snapshot':
                return
            elif msg['feed'] == 'ticker_lite':
                await self._ticker(msg, pair, timestamp)
            elif msg['feed'] == 'ticker':
                await self._funding(msg, pair, timestamp)
            elif msg['feed'] == 'book_snapshot':
                await self._book_snapshot(msg, pair, timestamp)
            elif msg['feed'] == 'book':
                await self._book(msg, pair, timestamp)
            else:
                LOG.warning("%s: Invalid message type %s", self.id, msg)
