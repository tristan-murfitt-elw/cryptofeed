'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from collections import defaultdict
from decimal import Decimal
import logging
from itertools import islice
from functools import partial
import time
from typing import Dict, List, Tuple, Callable
from cryptofeed.feed import Feed
import zlib
from sortedcontainers import SortedDict as sd

from yapic import json

from cryptofeed.connection import AsyncConnection, WSAsyncConn
from cryptofeed.defines import OKEX, LIQUIDATIONS, BUY, SELL, FILLED, UNFILLED, INDEX_PREFIX, ASK, BID, FUNDING, L2_BOOK, OPEN_INTEREST, UNDERLYING_INDEX, TICKER, TRADES
from cryptofeed.feed import Feed
from cryptofeed.exceptions import BadChecksum
from cryptofeed.standards import feed_to_exchange, timestamp_normalize, is_authenticated_channel


LOG = logging.getLogger("feedhandler")


class OKEx(Feed):
    """
    OKEx has the same api as OKCoin, just a different websocket endpoint
    """
    id = OKEX
    api = 'https://www.okex.com/api/'
    symbol_endpoint = ['https://www.okex.com/api/v5/public/instruments?instType=SPOT', 'https://www.okex.com/api/v5/public/instruments?instType=SWAP', 'https://www.okex.com/api/v5/public/instruments?instType=FUTURES', 'https://www.okex.com/api/v5/public/instruments?instType=OPTION&uly=BTC-USD', 'https://www.okex.com/api/v5/public/instruments?instType=OPTION&uly=ETH-USD', 'https://www.okex.com/api/v5/public/underlying?instType=FUTURES', 'https://www.okex.com/api/v5/public/underlying?instType=OPTION']

    @classmethod
    def _parse_symbol_data(cls, data: list, symbol_separator: str) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for entry in data:
            for e in entry['data']:
                if cls._entry_is_index_symbol(e):
                    # Index
                    for s in e:
                        standard_symbol = cls._translate_index_symbol(s, False)
                        ret[standard_symbol] = standard_symbol
                        info['instrument_type'][standard_symbol] = 'index'
                else:
                    standard_symbol = e['instId'].replace("-", symbol_separator)
                    ret[standard_symbol] = e['instId']
                    info['tickSz'][standard_symbol] = e['tickSz']
                    if e['instType'] == 'OPTION':
                        info['instrument_type'][standard_symbol] = 'option'
                    elif e['instType'] == 'SWAP':
                        info['instrument_type'][standard_symbol] = 'swap'
                    elif e['instType'] == 'FUTURES':
                        info['instrument_type'][standard_symbol] = 'futures'
                    elif e['instType'] == 'SPOT':
                        info['instrument_type'][standard_symbol] = 'spot'
        return ret, info

    @classmethod
    def _entry_is_index_symbol(cls, entry) -> bool:
        """
        Returns true if the entry is an index symbol. In the OkEX API,
        index data is returned as an array of strings, while regular symbols are an array of dictionaries
        """
        return isinstance(entry, list)
    
    @classmethod
    def instrument_type(cls, symbol: str):
        return cls.info()['instrument_type'][symbol]

    def __init__(self, **kwargs):
        super().__init__('wss://ws.okex.com:8443/ws/v5/public', **kwargs)
        self.ws_defaults['compression'] = None

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {"arg": {"channel": "tickers", "instId": "LTC-USD-200327"}, "data": [{"instType": "SWAP","instId": "LTC-USD-SWAP","last": "9999.99","lastSz": "0.1","askPx": "9999.99","askSz": "11","bidPx": "8888.88","bidSz": "5","open24h": "9000","high24h": "10000","low24h": "8888.88","volCcy24h": "2222","vol24h": "2222","sodUtc0": "2222","sodUtc8": "2222","ts": "1597026383085"}]}
        """
        pair = self.exchange_symbol_to_std_symbol(msg['arg']['instId'])
        for update in msg['data']:
            update_timestamp = timestamp_normalize(self.id, int(update['ts']))
            extra_fields = {
                'bbo': self.get_book_bbo(pair),
                'high': Decimal(update.get('high24h', 0)),
                'low': Decimal(update.get('low24h', 0)),
                'last': Decimal(update.get('last', 0)),
                'last_size': Decimal(update.get('lastSz', 0)),
                'volume': Decimal(update.get('volCcy24h', 0)),
                'best_bid_size': Decimal(update.get('bidSz') or 0),   # nullable
                'best_ask_size': Decimal(update.get('askSz') or 0),   # nullable
            }
            await self.callback(TICKER, feed=self.id,
                                symbol=pair,
                                bid=Decimal(update['bidPx']) if update['bidPx'] else Decimal(0),
                                ask=Decimal(update['askPx']) if update['askPx'] else Decimal(0),
                                timestamp=update_timestamp,
                                receipt_timestamp=timestamp,
                                **extra_fields)

    async def _index_price(self, msg: dict, timestamp: float):
        """
        {'table': 'index/ticker', 'data': [{'instrument_id': 'BTC-USD', 'last': '3977.74', 'open_24h': '3978.21', 'high_24h': '3995.43', 'low_24h': '3961.02', 'timestamp': '2019-03-22T22:26:34.019Z'}]}
        """
        for update in msg['data']:
            pair = self._translate_index_symbol(update['instId'], False)
            update_timestamp = timestamp_normalize(self.id, int(update['ts']))
            price = Decimal(update['idxPx'])
            await self.callback(UNDERLYING_INDEX, feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(pair),
                                timestamp=update_timestamp,
                                receipt_timestamp=timestamp,
                                price=price)

    async def _open_interest(self, msg: dict, timestamp: float):
        """
        {
            'arg': {
                'channel': 'open-interest',
                'instId': 'BTC-USDT-SWAP
            },
            'data': [
                {
                    'instId': 'BTC-USDT-SWAP',
                    'instType': 'SWAP',
                    'oi':'565474',
                    'oiCcy': '5654.74',
                    'ts': '1630338003010'
                }
            ]
        }
        """
        for update in msg['data']:
            await self.callback(OPEN_INTEREST,
                                feed=self.id,
                                symbol=update['instId'],
                                open_interest=Decimal(update['oi']),
                                timestamp=timestamp_normalize(self.id, int(update['ts'])),
                                receipt_timestamp=timestamp
                                )

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "arg": {
                "channel": "trades",
                "instId": "BTC-USD-191227"
            },
            "data": [
                {
                    "instId": "BTC-USD-191227",
                    "tradeId": "9",
                    "px": "0.016",
                    "sz": "50",
                    "side": "buy",
                    "ts": "1597026383085"
                }
            ]
        }
        """
        for trade in msg['data']:
            await self.callback(TRADES,
                                feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(trade['instId']),
                                order_id=trade['tradeId'],
                                side=BUY if trade['side'] == 'buy' else SELL,
                                amount=Decimal(trade['sz']),
                                price=Decimal(trade['px']),
                                timestamp=timestamp_normalize(self.id, int(trade['ts'])),
                                receipt_timestamp=timestamp
                                )

    async def _funding(self, msg: dict, timestamp: float):
        for update in msg['data']:
            # OKEx, OKCoin use the previous period's funding rate (`funding_rate`) at the funding time
            interval = 60*60*8  # 8 hours
            funding_time = timestamp_normalize(self.id, int(update['fundingTime']))

            await self.callback(FUNDING,
                                feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(update['instId']),
                                timestamp=timestamp,
                                receipt_timestamp=timestamp,
                                rate=Decimal(update['fundingRate']),
                                funding_time=funding_time,
                                next_funding_rate=Decimal(update['nextFundingRate']),
                                next_funding_time=funding_time + interval,
                                interval=interval)

    async def _book(self, msg: dict, timestamp: float):
        if msg['action'] == 'snapshot':
            # snapshot
            pair = self.exchange_symbol_to_std_symbol(msg['arg']['instId'])
            for update in msg['data']:
                self.l2_book[pair] = {
                    BID: sd({
                        Decimal(price): Decimal(amount) for price, amount, *_ in update['bids']
                    }),
                    ASK: sd({
                        Decimal(price): Decimal(amount) for price, amount, *_ in update['asks']
                    })
                }
                if self.validate_checksum() and self.__calc_checksum(pair) != (update['checksum'] & 0xFFFFFFFF):
                    raise BadChecksum
                await self.book_callback(self.l2_book[pair], L2_BOOK, pair, True, None, timestamp_normalize(self.id, int(update['ts'])), timestamp)
        else:
            # update
            pair = self.exchange_symbol_to_std_symbol(msg['arg']['instId'])
            for update in msg['data']:
                delta = {BID: [], ASK: []}

                for side in ('bids', 'asks'):
                    s = BID if side == 'bids' else ASK
                    for price, amount, *_ in update[side]:
                        price = Decimal(price)
                        amount = Decimal(amount)
                        if amount == 0:
                            if price in self.l2_book[pair][s]:
                                delta[s].append((price, 0))
                                del self.l2_book[pair][s][price]
                        else:
                            delta[s].append((price, amount))
                            self.l2_book[pair][s][price] = amount
                if self.validate_checksum() and self.__calc_checksum(pair) != (update['checksum'] & 0xFFFFFFFF):
                    raise BadChecksum
                await self.book_callback(self.l2_book[pair], L2_BOOK, pair, False, delta, timestamp_normalize(self.id, int(update['ts'])), timestamp)

    async def _liquidations(self, pairs: list, conn: AsyncConnection):
        """
        for PERP liquidations, the following arguments are required: uly, state
        for FUTURES liquidations, the following arguments are required: uly, state, alias
        FUTURES, MARGIN and OPTION liquidation request not currently supported by the below
        """
        last_update = defaultdict(dict)
        interval = 60

        while conn.is_open:
            for pair in pairs:
                if 'SWAP' in pair:
                    instrument_type = 'SWAP'
                    uly = pair.split("-")[0] + "-" + pair.split("-")[1]
                else:
                    continue

                for status in (FILLED, UNFILLED):
                    since_ts = int((time.time() - interval*2.5) * 1000)   # only fetch liqs in the last few mins, then inverse normalise the ts
                    end_point = f"{self.api}v5/public/liquidation-orders?instType={instrument_type}&limit=100&state={status}&uly={uly}&before={since_ts}"
                    try:
                        data = await self.http_conn.read(end_point)
                        data = json.loads(data, parse_float=Decimal)
                    except Exception as e:
                        logging.error(f'failed to parse {self.id} liquidations endpoint: url: {end_point}, err: {e}')
                        continue
                    timestamp = time.time()
                    if len(data['data'][0]['details']) == 0 or (len(data['data'][0]['details']) > 0 and last_update.get(pair) == data['data'][0]['details'][0]):
                        continue
                    for entry in data['data'][0]['details']:
                        if pair in last_update:
                            if entry == last_update[pair].get(status):
                                break

                        if entry.get('ts', '') == '':
                            continue

                        await self.callback(LIQUIDATIONS,
                                            feed=self.id,
                                            symbol=pair,
                                            side=BUY if entry['side'] == 'buy' else SELL,
                                            leaves_qty=Decimal(entry['sz']),
                                            price=Decimal(entry['bkPx']),
                                            order_id=None,
                                            status=FILLED if status == 1 else UNFILLED,
                                            timestamp=timestamp_normalize(self.id, int(entry['ts'])),
                                            receipt_timestamp=timestamp
                                            )
                    last_update[pair][status] = data['data'][0]['details'][0]   # cache the most recent
                await asyncio.sleep(0.1)
            await asyncio.sleep(interval)

    def __reset(self):
        self._l2_book = {}

    def __calc_checksum(self, pair):
        bid_it = reversed(self.l2_book[pair][BID])
        ask_it = iter(self.l2_book[pair][ASK])

        bids = (f"{bid}:{self.l2_book[pair][BID][bid]}" for bid in bid_it)
        bids = list(islice(bids, 25))
        asks = (f"{ask}:{self.l2_book[pair][ASK][ask]}" for ask in ask_it)
        asks = list(islice(asks, 25))

        if len(bids) == len(asks):
            combined = [val for pair in zip(bids, asks) for val in pair]
        elif len(bids) > len(asks):
            combined = [val for pair in zip(bids[:len(asks)], asks) for val in pair]
            combined += bids[len(asks):]
        else:
            combined = [val for pair in zip(bids, asks[:len(bids)]) for val in pair]
            combined += asks[len(bids):]

        computed = ":".join(combined).encode()
        return zlib.crc32(computed)


    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        return [(WSAsyncConn(self.address, self.id, **self.ws_defaults),
                    self.subscribe, self.message_handler, self.authenticate)]

    async def subscribe(self, connection: AsyncConnection):
        self.__reset()

        channels = []
        for chan in self.subscription:
            if chan == LIQUIDATIONS:
                asyncio.create_task(self._liquidations(self.subscription[LIQUIDATIONS], connection))
            else: 
                for pair in self.subscription[chan]:
                    instrument_type = self.instrument_type(self.exchange_symbol_to_std_symbol(pair))
                    if instrument_type == 'index':
                        if chan != feed_to_exchange(self.id, UNDERLYING_INDEX):
                            continue # Only use index symbols for the index feed
                        pair = self._translate_index_symbol(pair, True)
                    elif chan == feed_to_exchange(self.id, UNDERLYING_INDEX):
                        continue    # underlying index only for index symbols 

                    if instrument_type != 'swap' and chan == feed_to_exchange(self.id, FUNDING):
                        continue  # No funding for spot, futures and options
                    if instrument_type == 'spot' and chan == feed_to_exchange(self.id, OPEN_INTEREST):
                        continue  # No open interest for spots

                    
                    channels.append({"channel": chan, "instId": pair})

        if channels:
            msg = {"op": "subscribe",
                   "args": channels}
            LOG.debug(f'{connection.uuid}: Subscribing to public channels with message {msg}')
            await connection.write(json.dumps(msg))

    async def message_handler(self, msg: str, conn, timestamp: float):
        # DEFLATE compression, no header
        # msg = zlib.decompress(msg, -15)
        # not required, as websocket now set to "Per-Message Deflate"
        msg = json.loads(msg, parse_float=Decimal)

        if 'event' in msg:
            if msg['event'] == 'error':
                LOG.error("%s: Error: %s", self.id, msg)
            elif msg['event'] == 'subscribe':
                pass
            elif msg['event'] == 'login':
                await self._login(msg, timestamp)
            else:
                LOG.warning("%s: Unhandled event %s", self.id, msg)
        elif 'arg' in msg:
            if 'books-l2-tbt' in msg['arg']['channel']:
                await self._book(msg, timestamp)
            elif 'tickers' in msg['arg']['channel']:
                if 'index' in msg['arg']['channel']:
                    await self._index_price(msg, timestamp)
                else:
                    await self._ticker(msg, timestamp)
            elif 'trades' in msg['arg']['channel']:
                await self._trade(msg, timestamp)
            elif 'funding-rate' in msg['arg']['channel']:
                await self._funding(msg, timestamp)
            elif 'orders' in msg['arg']['channel']:
                await self._order(msg, timestamp)
            elif 'open-interest' in msg['arg']['channel']:
                await self._open_interest(msg, timestamp)
        else:
            LOG.warning("%s: Unhandled message %s", self.id, msg)
