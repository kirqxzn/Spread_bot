import asyncio
import aiohttp
from datetime import datetime
from prettytable import PrettyTable
from asyncio import Semaphore


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def is_stablecoin_pair(symbol: str) -> bool:
    STABLECOINS = ["USDT", "USDC", "BUSD", "DAI"]
    for stablecoin in STABLECOINS:
        if symbol.endswith(stablecoin):
            return True
    return False


class ArbitrageBot:
    def __init__(self):
        self.exchanges = ['BINANCE', 'KUCOIN', 'MEXC']
        self.min_spread_percent = 0.3
        self.max_spread_percent = 5
        self.session = None
        self.prices = {}  # {exchange: {pair: price}}
        self.pairs = {}  # {exchange: [pairs]}
        self.futures_pairs = {
            'BINANCE': set(),
            'MEXC': set(),
            'KUCOIN': set(),
        }
        self.semaphore = Semaphore(10)

    @staticmethod
    def normalize_spot_symbol(exchange: str, symbol: str) -> str:
        if exchange == 'KUCOIN':
            return symbol.replace('-', '').upper()
        elif exchange == 'MEXC':
            return symbol.replace('_', '').upper()
        else:  # BINANCE та інші
            return symbol.upper()

    @staticmethod
    def normalize_futures_symbol(exchange: str, symbol: str) -> str:
        if exchange == 'KUCOIN':
            if symbol.endswith('M'):
                return symbol[:-1].upper()
            return symbol.upper()
        elif exchange == 'MEXC':
            return symbol.replace('_', '').upper()
        elif exchange == 'BINANCE':
            return symbol.upper()
        else:
            return symbol.upper()

    async def load_all_pairs(self):
        tasks = [self.fetch_spot_pairs(ex) for ex in self.exchanges]
        results = await asyncio.gather(*tasks)
        for ex, pairs in zip(self.exchanges, results):
            filtered = [p for p in pairs if is_stablecoin_pair(p)]
            normalized = [self.normalize_spot_symbol(ex, p) for p in filtered]
            self.pairs[ex] = normalized
            log(f"[Info] {ex}: Loaded {len(normalized)} pairs after stablecoin filter")

    async def load_futures_pairs(self):
        # BINANCE futures
        try:
            url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    symbols = data.get('symbols', [])
                    self.futures_pairs['BINANCE'] = set(
                        self.normalize_futures_symbol('BINANCE', s['symbol'])
                        for s in symbols if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING'
                    )
                    log(f"[Info] Binance Futures: Loaded {len(self.futures_pairs['BINANCE'])} pairs")
                else:
                    log(f"[Error] Binance futures pairs fetch failed: {resp.status}")
        except Exception as e:
            log(f"[Error] Binance futures pairs fetch exception: {e}")

        # MEXC futures
        try:
            url = "https://contract.mexc.com/api/v1/contract/detail"
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    contracts = data.get('data', [])
                    mexc_futures = set()
                    for pair in contracts:
                        symbol = pair.get('symbol')
                        if symbol:
                            mexc_futures.add(self.normalize_futures_symbol('MEXC', symbol))
                    self.futures_pairs['MEXC'] = mexc_futures
                    log(f"[Info] MEXC Futures: Loaded {len(mexc_futures)} pairs")
                else:
                    log(f"[Error] MEXC futures pairs fetch failed: {resp.status}")
                    self.futures_pairs['MEXC'] = set()
        except Exception as e:
            log(f"[Error] MEXC futures pairs fetch exception: {e}")
            self.futures_pairs['MEXC'] = set()

        # KUCOIN futures
        try:
            url = 'https://api-futures.kucoin.com/api/v1/contracts/active'
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    contracts = data.get('data', [])
                    kucoin_futures = set()
                    for contract in contracts:
                        symbol = contract.get('symbol', '')
                        if symbol:
                            kucoin_futures.add(self.normalize_futures_symbol('KUCOIN', symbol))
                    self.futures_pairs['KUCOIN'] = kucoin_futures
                    log(f"[Info] KuCoin Futures: Loaded {len(kucoin_futures)} pairs")
                else:
                    log(f"[Error] KuCoin futures pairs fetch failed: {resp.status}")
                    self.futures_pairs['KUCOIN'] = set()
        except Exception as e:
            log(f"[Error] KuCoin futures pairs fetch exception: {e}")
            self.futures_pairs['KUCOIN'] = set()

    async def filter_spot_futures_pairs(self):
        for ex in self.exchanges:
            spot_pairs_raw = self.pairs.get(ex, [])
            futures_pairs_raw = self.futures_pairs.get(ex, set())
            spot_pairs = set(self.normalize_spot_symbol(ex, p) for p in spot_pairs_raw)
            futures_pairs = set(self.normalize_futures_symbol(ex, p) for p in futures_pairs_raw)
            filtered = futures_pairs.intersection(spot_pairs)
            self.pairs[ex] = list(filtered)
            log(f"[Info] {ex}: Filtered down to {len(filtered)} pairs present in both spot and futures")

    async def fetch_spot_pairs(self, exchange):
        try:
            if exchange == 'BINANCE':
                url = 'https://api.binance.com/api/v3/exchangeInfo'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] Fetching pairs from {exchange}: {resp.status}")
                        return []
                    data = await resp.json()
                    symbols = data.get('symbols', [])
                    raw_symbols = [s['symbol'] for s in symbols if s['status'] == 'TRADING']
                    normalized = [self.normalize_spot_symbol(exchange, sym) for sym in raw_symbols]
                    return normalized

            elif exchange == 'KUCOIN':
                url = 'https://api.kucoin.com/api/v1/symbols'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] Fetching pairs from {exchange}: {resp.status}")
                        return []
                    data = await resp.json()
                    symbols = data.get('data', [])
                    raw_symbols = [s['symbol'] for s in symbols if s['enableTrading']]
                    normalized = [self.normalize_spot_symbol(exchange, sym) for sym in raw_symbols]
                    return normalized

            elif exchange == 'MEXC':
                url = 'https://api.mexc.com/api/v3/exchangeInfo'
                headers = {'User-Agent': 'Mozilla/5.0'}
                async with self.session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        log(f"[Error] Fetching pairs from {exchange}: {resp.status}")
                        return []
                    data = await resp.json()
                    symbols = data.get('symbols', [])
                    raw_symbols = [s['symbol'] for s in symbols if 'symbol' in s]
                    normalized = [self.normalize_spot_symbol(exchange, sym) for sym in raw_symbols]
                    return normalized
            else:
                return []
        except Exception as e:
            log(f"[Error] Fetching pairs from {exchange}: {e}")
            return []

    async def fetch_all_prices(self):
        tasks = [self.fetch_prices(exchange) for exchange in self.exchanges]
        await asyncio.gather(*tasks)

    async def fetch_prices(self, exchange):
        try:
            if exchange == 'BINANCE':
                url = 'https://api.binance.com/api/v3/ticker/price'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] Fetching prices from {exchange}: {resp.status}")
                        return
                    data = await resp.json()
                    prices_dict = {}
                    for item in data:
                        symbol = self.normalize_spot_symbol(exchange, item['symbol'])
                        try:
                            prices_dict[symbol] = float(item['price'])
                        except:
                            continue
                    self.prices[exchange] = prices_dict

            elif exchange == 'KUCOIN':
                url = 'https://api.kucoin.com/api/v1/market/allTickers'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] Fetching prices from {exchange}: {resp.status}")
                        return
                    data = await resp.json()
                    tickers = data.get('data', {}).get('ticker', [])
                    prices = {}
                    for t in tickers:
                        price_raw = t.get('last')
                        if price_raw is not None:
                            try:
                                symbol = self.normalize_spot_symbol(exchange, t['symbol'])
                                prices[symbol] = float(price_raw)
                            except ValueError:
                                continue
                    self.prices[exchange] = prices

            elif exchange == 'MEXC':
                url = 'https://api.mexc.com/api/v3/ticker/price'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] Fetching prices from {exchange}: {resp.status}")
                        return
                    data = await resp.json()
                    prices_dict = {}
                    for item in data:
                        symbol = self.normalize_spot_symbol(exchange, item['symbol'])
                        try:
                            prices_dict[symbol] = float(item['price'])
                        except:
                            continue
                    self.prices[exchange] = prices_dict

        except Exception as e:
            log(f"[Error] Fetching prices from {exchange}: {e}")

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            log("[Start] Starting arbitrage bot")

            await self.load_all_pairs()
            await self.load_futures_pairs()
            await self.filter_spot_futures_pairs()
            await self.fetch_all_prices()

    async def analyze_arbitrage_opportunities(self):
        print("🔍 Analyzing arbitrage spreads...")

        candidate_pairs = {}
        for pair in set(p for pairs in self.pairs.values() for p in pairs):
            buy_exchanges = []
            sell_exchanges = []
            for ex in self.exchanges:
                spot_pairs = set(self.pairs.get(ex, []))
                futures_pairs = self.futures_pairs.get(ex, set())

                if pair in spot_pairs:
                    buy_exchanges.append(ex)

                if pair in spot_pairs and pair in futures_pairs:
                    sell_exchanges.append(ex)

            if buy_exchanges and sell_exchanges:
                candidate_pairs[pair] = {
                    'buy': buy_exchanges,
                    'sell': sell_exchanges,
                }

        print(f"[Info] Number of pairs to analyze: {len(candidate_pairs)}")

        results = []

        for pair, exchanges in candidate_pairs.items():
            for buy_ex in exchanges['buy']:
                for sell_ex in exchanges['sell']:
                    if buy_ex == sell_ex:
                        continue

                    pair_key = pair.upper()
                    buy_price = self.prices.get(buy_ex, {}).get(pair_key, 0)
                    sell_price = self.prices.get(sell_ex, {}).get(pair_key, 0)

                    if buy_price == 0 or sell_price == 0:
                        continue

                    spread = (sell_price - buy_price) / buy_price * 100

                    if spread < self.min_spread_percent or spread > self.max_spread_percent:
                        continue

                    results.append({
                        'pair': pair,
                        'buy_exchange': buy_ex,
                        'sell_exchange': sell_ex,
                        'buy_price': buy_price,
                        'sell_price': sell_price,
                        'spread': spread,
                    })

        results.sort(key=lambda x: x['spread'], reverse=True)
        self.print_results(results)

    def print_results(self, results):
        if not results:
            print("No spreads found.")
            return
        table = PrettyTable()
        table.field_names = [
            "Pair", "Buy on", "Buy price",
            "Sell on", "Sell price", "Spread %"
        ]
        for r in results:
            table.add_row([
                r['pair'], r['buy_exchange'], f"{r['buy_price']:.6f}",
                r['sell_exchange'], f"{r['sell_price']:.6f}", f"{r['spread']:.2f}"
            ])
        print(table)


async def main():
    bot = ArbitrageBot()
    await bot.start()
    await bot.analyze_arbitrage_opportunities()


if __name__ == "__main__":
    asyncio.run(main())
