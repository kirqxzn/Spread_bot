import aiohttp
from utils.logger import log
from utils.helpers import is_stablecoin_pair


class SpotAPI:
    def __init__(self):
        self.spot_pairs = {
            'BINANCE': set(),
            'KUCOIN': set(),
            'MEXC': set(),
        }
        self.session = None

    async def load_all_pairs(self):
        async with aiohttp.ClientSession() as session:
            self.session = session
            for ex in self.spot_pairs.keys():
                pairs = await self.fetch_spot_pairs(ex)
                filtered = [p for p in pairs if is_stablecoin_pair(p)]
                self.spot_pairs[ex] = set(filtered)
                log(f"[Info] {ex}: Loaded {len(filtered)} pairs after stablecoin filter")

    @staticmethod
    def normalize_symbol(exchange, symbol):
        if exchange == 'KUCOIN':
            return symbol.replace('-', '').upper()
        elif exchange == 'MEXC':
            return symbol.replace('_', '').upper()
        else:  # BINANCE та інші
            return symbol.upper()

    async def fetch_spot_pairs(self, exchange):
        try:
            if exchange == 'BINANCE':
                url = 'https://api.binance.com/api/v3/exchangeInfo'
                async with self.session.get(url) as resp:
                    data = await resp.json()
                    return [self.normalize_symbol(exchange, s['symbol']) for s in data['symbols'] if
                            s['status'] == 'TRADING']

            elif exchange == 'KUCOIN':
                url = 'https://api.kucoin.com/api/v1/symbols'
                async with self.session.get(url) as resp:
                    data = await resp.json()
                    return [self.normalize_symbol(exchange, s['symbol']) for s in data['data'] if s['enableTrading']]

            elif exchange == 'MEXC':
                url = 'https://api.mexc.com/api/v3/exchangeInfo'
                async with self.session.get(url) as resp:
                    data = await resp.json()
                    return [self.normalize_symbol(exchange, s['symbol']) for s in data['symbols']]

        except Exception as e:
            log(f"[Error] Fetching pairs from {exchange}: {e}")
            return []

    async def filter_by_futures(self, futures_pairs):
        for ex in self.spot_pairs:
            self.spot_pairs[ex] = self.spot_pairs[ex].intersection(futures_pairs[ex])
            log(f"[Info] {ex}: Filtered down to {len(self.spot_pairs[ex])} pairs present in both spot and futures")

    async def fetch_quick_prices(self):
        results = {ex: {} for ex in self.spot_pairs.keys()}

        async with aiohttp.ClientSession() as session:
            self.session = session

            # BINANCE
            try:
                url = 'https://api.binance.com/api/v3/ticker/bookTicker'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        results['BINANCE'][item['symbol']] = {
                            'bid': float(item['bidPrice']),
                            'ask': float(item['askPrice']),
                        }
            except Exception as e:
                log(f"[Error] Binance quick prices fetch exception: {e}")

            # KUCOIN
            try:
                url = 'https://api.kucoin.com/api/v1/market/allTickers'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for t in data['data']['ticker']:
                        symbol = t['symbol'].replace('-', '')
                        bid = float(t.get('buy') or 0)
                        ask = float(t.get('sell') or 0)
                        if bid > 0 and ask > 0:
                            results['KUCOIN'][symbol] = {'bid': bid, 'ask': ask}
            except Exception as e:
                log(f"[Error] KuCoin quick prices fetch exception: {e}")

            # MEXC
            try:
                url = 'https://api.mexc.com/api/v3/ticker/bookTicker'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        results['MEXC'][item['symbol']] = {
                            'bid': float(item['bidPrice']),
                            'ask': float(item['askPrice']),
                        }
            except Exception as e:
                log(f"[Error] MEXC quick prices fetch exception: {e}")

        return results


    async def fetch_last_prices(self):
        """
        Отримати last price (останню ціну, середню) для всіх пар, по кожній біржі.
        Повертає dict: {exchange: {pair: float_last_price}}
        """
        last_prices = {ex: {} for ex in self.spot_pairs.keys()}


        async with aiohttp.ClientSession() as session:
            self.session = session

            # BINANCE
            try:
                url = 'https://api.binance.com/api/v3/ticker/price'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        symbol = item['symbol']
                        price = float(item['price'])
                        last_prices['BINANCE'][symbol] = price
            except Exception as e:
                log(f"[Error] Binance last prices fetch exception: {e}")

            # KUCOIN
            try:
                url = 'https://api.kucoin.com/api/v1/market/allTickers'
                async with session.get(url) as resp:
                    data = await resp.json()
                    tickers = data.get('data', {}).get('ticker', [])
                    for t in tickers:
                        symbol = t['symbol'].replace('-', '')
                        price = float(t.get('last') or 0)
                        last_prices['KUCOIN'][symbol] = price
            except Exception as e:
                log(f"[Error] KuCoin last prices fetch exception: {e}")

            # MEXC
            try:
                url = 'https://api.mexc.com/api/v3/ticker/price'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        symbol = item['symbol']
                        price = float(item['price'])
                        last_prices['MEXC'][symbol] = price
            except Exception as e:
                log(f"[Error] MEXC last prices fetch exception: {e}")

        return last_prices
