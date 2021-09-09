'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging
import time
from typing import List, Tuple, Callable, Dict, List

from yapic import json

from cryptofeed.connection import AsyncConnection, HTTPPoll
from cryptofeed.defines import BINANCE_FUTURES, OPEN_INTEREST, FUNDING, UNDERLYING_INDEX, INDEX_PREFIX
from cryptofeed.exchange.binance import Binance
from cryptofeed.standards import timestamp_normalize

LOG = logging.getLogger('feedhandler')


class BinanceFutures(Binance):
    id = BINANCE_FUTURES
    valid_depths = [5, 10, 20, 50, 100, 500, 1000]
    symbol_endpoint = ['https://fapi.binance.com/fapi/v1/exchangeInfo', 'https://fapi.binance.com/fapi/v1/premiumIndex']

    @classmethod
    def _parse_symbol_data(cls, data: List, symbol_separator: str) -> Tuple[Dict, Dict]:
        ret = {}
        for symbols_result in data:
            if cls._is_index_symbols_result(symbols_result):
                for index in symbols_result:
                    # Index symbol
                    exchange_symbol = cls._translate_index_symbol(index['symbol'], False)
                    ret[exchange_symbol] = exchange_symbol
            else:
                # Regular symbols
                base, info = super()._parse_symbol_data(symbols_result, symbol_separator)
                add = {}
                for symbol, orig in base.items():
                    if "_" in orig:
                        continue
                    add[f"{symbol}{symbol_separator}PINDEX"] = f"p{orig}"
                ret.update(add)
                ret.update(base)
        return ret, info

    @classmethod
    def _is_index_symbols_result(cls, symbols_result) -> bool:
        """
        Returns true if the entry is contains index results. In the Binance API, index data is returned as an array of dictionaries,
        while regular symbols are returned as part of a dictionary of general market information.
        """
        return isinstance(symbols_result, list)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # overwrite values previously set by the super class Binance
        self.ws_endpoint = 'wss://fstream.binance.com'
        self.rest_endpoint = 'https://fapi.binance.com/fapi/v1'
        self.address = self._address()

    def _check_update_id(self, pair: str, msg: dict) -> Tuple[bool, bool]:
        skip_update = False
        forced = not self.forced[pair]

        if forced and msg['u'] < self.last_update_id[pair]:
            skip_update = True
        elif forced and msg['U'] <= self.last_update_id[pair] <= msg['u']:
            self.last_update_id[pair] = msg['u']
            self.forced[pair] = True
        elif not forced and self.last_update_id[pair] == msg['pu']:
            self.last_update_id[pair] = msg['u']
        else:
            self._reset()
            LOG.warning("%s: Missing book update detected, resetting book", self.id)
            skip_update = True
        return skip_update, forced

    async def _open_interest(self, msg: dict, timestamp: float):
        """
        {
            "openInterest": "10659.509",
            "symbol": "BTCUSDT",
            "time": 1589437530011   // Transaction time
        }
        """
        pair = msg['symbol']
        oi = Decimal(msg['openInterest'])
        if oi != self.open_interest.get(pair, None):
            await self.callback(OPEN_INTEREST,
                                feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(pair),
                                open_interest=oi,
                                timestamp=timestamp_normalize(self.id, msg['time']),
                                receipt_timestamp=time.time()
                                )
            self.open_interest[pair] = oi

    async def _funding_with_index_price(self, msg: dict, timestamp: float):
        """
        {
            "e": "markPriceUpdate",     // Event type
            "E": 1562305380000,         // Event time
            "s": "BTCUSDT",             // Symbol
            "p": "11794.15000000",      // Mark price
            "i": "11784.62659091",      // Index price
            "P": "11784.25641265",      // Estimated Settle Price, only useful in the last hour before the settlement starts
            "r": "0.00038167",          // Funding rate
            "T": 1562306400000          // Next funding time
        }
        """
        symbol = self.exchange_symbol_to_std_symbol(msg['s'])
        exchange_index_symbol = self._translate_index_symbol(msg['s'], False)
        index_symbol = self.exchange_symbol_to_std_symbol(exchange_index_symbol)
        ts = timestamp_normalize(self.id, msg['E'])

        if self._subscribed_to_feed_and_symbol(FUNDING, msg['s']):
            # Subscribed to the non-index symbol and FUNDING. Send an update.
            await self.callback(FUNDING,
                                feed=self.id,
                                symbol=symbol,
                                timestamp=ts,
                                receipt_timestamp=timestamp,
                                mark_price=msg['p'],
                                rate=msg['r'],
                                next_funding_time=timestamp_normalize(self.id, msg['T']),
                                )

        if self._subscribed_to_feed_and_symbol(UNDERLYING_INDEX, exchange_index_symbol):
            # Subscribed to the index symbol and UNDERLYING_INDEX. Send an update.
            await self.callback(UNDERLYING_INDEX, feed=self.id,
                            symbol=index_symbol,
                            timestamp=ts,
                            receipt_timestamp=timestamp,
                            price=msg['i'])

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        ret = []
        if self.address:
            ret = super().connect()

        for chan in set(self.subscription):
            if chan == 'open_interest':
                addrs = []
                for pair in self.subscription[chan]:
                    if pair.startswith(INDEX_PREFIX):
                        continue
                    addrs.append(f"{self.rest_endpoint}/openInterest?symbol={pair}")
                ret.append((HTTPPoll(addrs, self.id, delay=60.0, sleep=1.0), self.subscribe, self.message_handler, self.authenticate))
        return ret

    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        # Handle REST endpoint messages first
        if 'openInterest' in msg:
            await self._open_interest(msg, timestamp)
            return

        # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        # streamName is of format <symbol>@<channel>
        pair, _ = msg['stream'].split('@', 1)
        msg = msg['data']

        pair = pair.upper()

        msg_type = msg.get('e')
        if msg_type == 'bookTicker':
            await self._ticker(msg, timestamp)
        elif msg_type == 'depthUpdate':
            await self._book(msg, pair, timestamp)
        elif msg_type == 'aggTrade':
            await self._trade(msg, timestamp)
        elif msg_type == 'forceOrder':
            await self._liquidations(msg, timestamp)
        elif msg_type == 'markPriceUpdate':
            # This channel is used for both kinds of updates
            # Attempt to publish both (but only publish if we're subscribed to a symbol that supports the event)
            await self._funding_with_index_price(msg, timestamp)
        elif msg['e'] == 'kline':
            await self._candle(msg, timestamp)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)
