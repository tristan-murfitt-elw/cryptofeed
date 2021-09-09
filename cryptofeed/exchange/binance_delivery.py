'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging
from typing import Tuple, Dict, List

from yapic import json

from cryptofeed.defines import BINANCE_DELIVERY, UNDERLYING_INDEX
from cryptofeed.exchange.binance import Binance
from cryptofeed.standards import timestamp_normalize

LOG = logging.getLogger('feedhandler')


class BinanceDelivery(Binance):
    valid_depths = [5, 10, 20, 50, 100, 500, 1000]
    id = BINANCE_DELIVERY
    symbol_endpoint = ['https://dapi.binance.com/dapi/v1/exchangeInfo', 'https://dapi.binance.com/dapi/v1/premiumIndex']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # overwrite values previously set by the super class Binance
        self.ws_endpoint = 'wss://dstream.binance.com'
        self.rest_endpoint = 'https://dapi.binance.com/dapi/v1'
        self.address = self._address()

    @classmethod
    def _parse_symbol_data(cls, data: List, symbol_separator: str) -> Tuple[Dict, Dict]:
        ret = {}
        for symbols_result in data:
            if cls._is_index_symbols_result(symbols_result):
                for index in symbols_result:
                    # Index symbol
                    exchange_symbol = cls._translate_index_symbol(index['pair'], False)
                    ret[exchange_symbol] = exchange_symbol
            else:
                # Regular symbols
                base, info = super()._parse_symbol_data(symbols_result, symbol_separator)
                ret.update(base)
        return ret, info

    @classmethod
    def _is_index_symbols_result(cls, symbols_result) -> bool:
        """
        Returns true if the entry is contains index results. In the Binance API, index data is returned as an array of dictionaries,
        while regular symbols are returned as part of a dictionary of general market information.
        """
        return isinstance(symbols_result, list)

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

    async def _index_price(self, msg: dict, timestamp: float):
        """
        {
            "e": "indexPriceUpdate",  // Event type
            "E": 1591261236000,       // Event time
            "i": "BTCUSD",            // Pair
            "p": "9636.57860000",     // Index Price
        }
        """
        exchange_index_symbol = self._translate_index_symbol(msg['i'], False)
        pair = self.exchange_symbol_to_std_symbol(exchange_index_symbol)
        price = Decimal(msg['p'])

        if 'E' in msg:
            ts = timestamp_normalize(self.id, msg['E'])
        else:
            ts = timestamp
        await self.callback(UNDERLYING_INDEX, feed=self.id,
                            symbol=pair,
                            timestamp=ts,
                            receipt_timestamp=timestamp,
                            price=price)        
    

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

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
            await self._funding(msg, timestamp)
        elif msg_type == 'indexPriceUpdate':
            await self._index_price(msg, timestamp)
        elif msg_type == 'kline':
            await self._candle(msg, timestamp)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)
