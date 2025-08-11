import asyncio
import aiohttp
from datetime import datetime
from prettytable import PrettyTable
from asyncio import Semaphore

MIN_VOLUME_USDT_24H = 100000  # мінімальний обсяг для фільтрації


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def is_stablecoin_pair(symbol: str) -> bool:
    STABLECOINS = ["USDT", "USDC", "BUSD", "DAI"]
    for stablecoin in STABLECOINS:
        if symbol.endswith(stablecoin):
            return True
    return False


async def get_all_exchange_volumes(candidate_pairs, available_pairs):
    """
    Отримує обсяги 24h для заданих пар на заданих біржах.
    - candidate_pairs: {pair: {'buy': [...], 'sell': [...]} }
    - available_pairs: {exchange: set(пари)}

    Повертає:
    {pair: {exchange: volume_usdt, ...}, ...}
    """

    result = {}

    async with aiohttp.ClientSession() as session:
        # BINANCE volumes
        binance_volumes = {}
        try:
            url = 'https://api.binance.com/api/v3/ticker/24hr'
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        symbol = item['symbol'].upper()
                        if symbol in available_pairs.get('BINANCE', set()):
                            price = float(item.get('weightedAvgPrice') or item.get('lastPrice') or 0)
                            volume = float(item.get('volume') or 0)
                            vol_usdt = price * volume
                            binance_volumes[symbol] = vol_usdt
                else:
                    log(f"[Error] Binance volumes fetch failed: {resp.status}")
        except Exception as e:
            log(f"[Error] Binance volumes fetch exception: {e}")

        # KUCOIN volumes
        kucoin_volumes = {}
        try:
            url = 'https://api.kucoin.com/api/v1/market/allTickers'
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    tickers = data.get('data', {}).get('ticker', [])
                    for t in tickers:
                        symbol = t['symbol'].replace('-', '').upper()
                        if symbol in available_pairs.get('KUCOIN', set()):
                            price = float(t.get('last') or 0)
                            volume = float(t.get('vol') or 0)
                            vol_usdt = price * volume
                            kucoin_volumes[symbol] = vol_usdt
                else:
                    log(f"[Error] KuCoin volumes fetch failed: {resp.status}")
        except Exception as e:
            log(f"[Error] KuCoin volumes fetch exception: {e}")

        # MEXC volumes
        mexc_volumes = {}
        try:
            url = 'https://api.mexc.com/api/v3/ticker/24hr'
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        symbol = item['symbol'].upper()
                        if symbol in available_pairs.get('MEXC', set()):
                            price = float(item.get('weightedAvgPrice') or item.get('lastPrice') or 0)
                            volume = float(item.get('volume') or 0)
                            vol_usdt = price * volume
                            mexc_volumes[symbol] = vol_usdt
                else:
                    log(f"[Error] MEXC volumes fetch failed: {resp.status}")
        except Exception as e:
            log(f"[Error] MEXC volumes fetch exception: {e}")

        # Формуємо результат для candidate_pairs:
        for pair, ex_dict in candidate_pairs.items():
            result[pair] = {}
            for ex_list_name in ['buy', 'sell']:
                for ex in ex_dict.get(ex_list_name, []):
                    symbol_norm = pair.upper()
                    volume = 0
                    if ex == 'BINANCE':
                        volume = binance_volumes.get(symbol_norm, 0)
                    elif ex == 'KUCOIN':
                        volume = kucoin_volumes.get(symbol_norm, 0)
                    elif ex == 'MEXC':
                        volume = mexc_volumes.get(symbol_norm, 0)
                    if volume > 0:
                        result[pair][ex] = volume
                    else:
                        result[pair][ex] = 0

    return result


class ArbitrageBot:
    STABLECOINS = ['USDT', 'USDC', 'BUSD']

    def is_stable_pair(self, pair: str) -> bool:
        return any(stable in pair for stable in self.STABLECOINS)

    def __init__(self):
        self.exchanges = ['BINANCE', 'KUCOIN', 'MEXC']
        self.min_spread_percent = 0.3
        self.max_spread_percent = 5
        self.session = None
        self.prices = {}  # {exchange: {pair: price}}
        self.pairs = {} # {exchange: [pairs]}
        self.spot_pairs = {
            'BINANCE': set(),  # сюди треба записати всі пари споту BINANCE
            'KUCOIN': set(),  # аналогічно для KUCOIN
            'MEXC': set(),  # і для MEXC
        }
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

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            log("[Start] Starting arbitrage bot")

            await self.load_all_pairs()
            await self.load_futures_pairs()
            await self.load_spot_pairs()
            await self.filter_spot_futures_pairs()


            # --- НОВИНКА: спочатку швидкі ціни ---
            quick_prices = await self.fetch_quick_prices()
            candidates = self.find_candidates_by_quick_prices(quick_prices)

            log(f"[Info] Candidates from quick prices: {len(candidates)}")

            # --- 🟡 Друк кандидатів ---
            self.print_candidates_table(candidates, quick_prices)

            # --- Потім глибокий аналіз orderbook для кандидатів ---
            results, _ = await self.analyze_arbitrage_opportunities(candidates)
            self.print_arbitrage_opportunities(results)

    async def load_all_pairs(self):
        tasks = [self.fetch_spot_pairs(ex) for ex in self.exchanges]
        results = await asyncio.gather(*tasks)
        for ex, pairs in zip(self.exchanges, results):
            filtered = [p for p in pairs if is_stablecoin_pair(p)]
            normalized = [self.normalize_spot_symbol(ex, p) for p in filtered]
            self.pairs[ex] = normalized
            log(f"[Info] {ex}: Loaded {len(normalized)} pairs after stablecoin filter")

    async def load_spot_pairs(self):
        # Приклад заповнення spot_pairs
        # Тут припускаємо, що у self.pairs[exchange] вже завантажені всі пари біржі
        for ex in self.exchanges:
            pairs_list = self.pairs.get(ex, [])
            self.spot_pairs[ex] = set(pairs_list)

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
        # Залишаю як є, щоб не ламати логіку, але не використовується для розрахунку спреду
        tasks = [self.fetch_prices(exchange) for exchange in self.exchanges]
        await asyncio.gather(*tasks)

    async def fetch_prices(self, exchange):
        # Залишаю як є, щоб не ламати логіку, але не використовується для розрахунку спреду
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

    async def fetch_order_book_price(self, exchange, pair):
        async with self.semaphore:
            try:
                symbol = pair.upper()
                if exchange == 'BINANCE':
                    url = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5'
                    async with self.session.get(url) as resp:
                        if resp.status != 200:
                            log(f"[Error] Fetching order book from {exchange} for {symbol}: {resp.status}")
                            return None, None
                        data = await resp.json()
                        best_bid = float(data['bids'][0][0]) if data['bids'] else None
                        best_ask = float(data['asks'][0][0]) if data['asks'] else None
                        return best_bid, best_ask

                elif exchange == 'KUCOIN':
                    # KuCoin orderbook — символи у форматі XXX-YYY (через дефіс)
                    symbol_kucoin = pair[:-4] + '-' + pair[-4:]
                    url = f'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={symbol_kucoin}'
                    async with self.session.get(url) as resp:
                        if resp.status != 200:
                            log(f"[Error] Fetching order book from {exchange} for {symbol_kucoin}: {resp.status}")
                            return None, None
                        data = await resp.json()
                        if data.get('code') == '200000':
                            best_bid = float(data['data']['bestBid']) if 'bestBid' in data['data'] else None
                            best_ask = float(data['data']['bestAsk']) if 'bestAsk' in data['data'] else None
                            return best_bid, best_ask
                        else:
                            log(f"[Error] KuCoin orderbook API error: {data}")
                            return None, None

                elif exchange == 'MEXC':
                    url = f'https://api.mexc.com/api/v3/depth?symbol={symbol}&limit=5'
                    async with self.session.get(url) as resp:
                        if resp.status != 200:
                            log(f"[Error] Fetching order book from {exchange} for {symbol}: {resp.status}")
                            return None, None
                        data = await resp.json()
                        best_bid = float(data['bids'][0][0]) if data.get('bids') else None
                        best_ask = float(data['asks'][0][0]) if data.get('asks') else None
                        return best_bid, best_ask

                else:
                    return None, None

            except Exception as e:
                log(f"[Error] Fetching order book from {exchange} for {pair}: {e}")
                return None, None

    async def analyze_arbitrage_opportunities(self, candidate_pairs):
        print("🔍 Analyzing arbitrage opportunities...")
        results = []
        excluded = []
        tasks = []

        for pair, ex_dict in candidate_pairs.items():
            buy_exchanges = ex_dict.get('buy', [])
            sell_exchanges = ex_dict.get('sell', [])
            for buy_ex in buy_exchanges:
                for sell_ex in sell_exchanges:
                    if buy_ex == sell_ex:
                        continue
                    tasks.append(self.analyze_pair(pair, buy_ex, sell_ex))

        analyzed = await asyncio.gather(*tasks)
        for res in analyzed:
            if res:
                results.append(res)
            else:
                excluded.append(res)
        return results, excluded

    async def analyze_pair(self, pair, buy_ex, sell_ex):
        sell_bid, sell_ask = await self.fetch_order_book_price(sell_ex, pair)
        buy_bid, buy_ask = await self.fetch_order_book_price(buy_ex, pair)

        if sell_bid is None or buy_ask is None:
            return None

        spread = (sell_bid - buy_ask) / buy_ask * 100
        if spread < self.min_spread_percent or spread > self.max_spread_percent:
            return None

        return {
            'pair': pair,
            'buy_ex': buy_ex,
            'sell_ex': sell_ex,
            'buy_price': buy_ask,
            'sell_price': sell_bid,
            'spread': spread,
        }

    def print_candidates_table(self, candidates, quick_prices):
        if not candidates:
            print("🚫 No candidates found in quick prices.")
            return
        table = PrettyTable()
        table.field_names = ["Pair", "Buy Exchange", "Sell Exchange", "Buy Ask", "Sell Bid", "Approx Spread %"]
        for pair, ex_dict in candidates.items():
            buys = ex_dict.get('buy', [])
            sells = ex_dict.get('sell', [])
            # Виводимо всі можливі пари buy/sell
            for buy_ex in buys:
                for sell_ex in sells:
                    if buy_ex == sell_ex:
                        continue
                    buy_ask = quick_prices.get(buy_ex, {}).get(pair, {}).get('ask')
                    sell_bid = quick_prices.get(sell_ex, {}).get(pair, {}).get('bid')
                    if buy_ask is None or sell_bid is None:
                        continue
                    if buy_ask >= sell_bid:
                        continue
                    approx_spread = (sell_bid - buy_ask) / buy_ask * 100
                    if not (self.min_spread_percent <= approx_spread <= self.max_spread_percent):
                        continue
                    table.add_row([
                        pair,
                        buy_ex,
                        sell_ex,
                        f"{buy_ask:.8f}",
                        f"{sell_bid:.8f}",
                        f"{approx_spread:.4f}"
                    ])
        print("🟡 Candidates based on quick prices:")
        print(table)

    def print_arbitrage_opportunities(self, opportunities):
        if not opportunities:
            print("🚫 No arbitrage opportunities found.")
            return
        table = PrettyTable()
        table.field_names = ["Pair", "Buy Exchange", "Sell Exchange", "Buy Price", "Sell Price", "Spread %"]
        for opp in opportunities:
            table.add_row([
                opp['pair'],
                opp['buy_ex'],
                opp['sell_ex'],
                f"{opp['buy_price']:.8f}",
                f"{opp['sell_price']:.8f}",
                f"{opp['spread']:.4f}"
            ])
        print(table)

    async def fetch_quick_prices(self):
        """
        Швидкий збір bid/ask для всіх пар.
        Повертає {exchange: {pair: {'bid': float, 'ask': float, 'last': float}}}
        """
        results = {ex: {} for ex in self.exchanges}

        async def fetch_binance():
            try:
                url = 'https://api.binance.com/api/v3/ticker/bookTicker'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] Binance quick prices fetch failed: {resp.status}")
                        return
                    data = await resp.json()
                    for item in data:
                        symbol = self.normalize_spot_symbol('BINANCE', item['symbol'])
                        bid_raw = item.get('bidPrice')
                        ask_raw = item.get('askPrice')
                        if bid_raw is None or ask_raw is None:
                            continue
                        bid = float(bid_raw)
                        ask = float(ask_raw)

                        results['BINANCE'][symbol] = {'bid': bid, 'ask': ask, 'last': (bid + ask) / 2}
            except Exception as e:
                log(f"[Error] Binance quick prices fetch exception: {e}")

        async def fetch_kucoin():
            try:
                url = 'https://api.kucoin.com/api/v1/market/allTickers'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] KuCoin quick prices fetch failed: {resp.status}")
                        return
                    data = await resp.json()
                    tickers = data.get('data', {}).get('ticker', [])
                    for t in tickers:
                        symbol = self.normalize_spot_symbol('KUCOIN', t['symbol'])
                        bid = float(t.get('buy') or 0)
                        ask = float(t.get('sell') or 0)
                        if bid > 0 and ask > 0:
                            results['KUCOIN'][symbol] = {'bid': bid, 'ask': ask, 'last': (bid + ask) / 2}
            except Exception as e:
                log(f"[Error] KuCoin quick prices fetch exception: {e}")

        async def fetch_mexc():
            try:
                url = 'https://api.mexc.com/api/v3/ticker/bookTicker'
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log(f"[Error] MEXC quick prices fetch failed: {resp.status}")
                        return
                    data = await resp.json()
                    for item in data:
                        symbol = self.normalize_spot_symbol('MEXC', item['symbol'])
                        bid = float(item['bidPrice'])
                        ask = float(item['askPrice'])
                        results['MEXC'][symbol] = {'bid': bid, 'ask': ask, 'last': (bid + ask) / 2}
            except Exception as e:
                log(f"[Error] MEXC quick prices fetch exception: {e}")

        await asyncio.gather(fetch_binance(), fetch_kucoin(), fetch_mexc())
        return results

    def find_candidates_by_quick_prices(self, quick_prices):
        candidates = {}

        all_pairs = set()
        for ex_prices in quick_prices.values():
            all_pairs.update(ex_prices.keys())

        for pair in all_pairs:
            if not self.is_stable_pair(pair):
                continue

            buy_exchanges = []
            sell_exchanges = []

            for ex in self.exchanges:
                ex_prices = quick_prices.get(ex, {})
                # Перевірка чи є пара в споті біржі
                if pair in ex_prices and pair in self.spot_pairs.get(ex, set()):
                    buy_exchanges.append(ex)
                # Для продажу перевіряємо, чи є ф’ючерси на біржі (зокрема для sell_ex)
                if pair in ex_prices and pair in self.futures_pairs.get(ex, set()):
                    sell_exchanges.append(ex)

            for buy_ex in buy_exchanges:
                for sell_ex in sell_exchanges:
                    if buy_ex == sell_ex:
                        continue

                    # Додаткова перевірка, що ф’ючерси є саме на біржі продажу
                    if pair not in self.futures_pairs.get(sell_ex, set()):
                        continue

                    buy_ask = quick_prices[buy_ex][pair]['ask']
                    sell_bid = quick_prices[sell_ex][pair]['bid']

                    if buy_ask == 0 or sell_bid == 0:
                        continue

                    if buy_ask >= sell_bid:
                        continue

                    spread = (sell_bid - buy_ask) / buy_ask * 100
                    if self.min_spread_percent <= spread <= self.max_spread_percent:
                        if pair not in candidates:
                            candidates[pair] = {'buy': [], 'sell': []}
                        if buy_ex not in candidates[pair]['buy']:
                            candidates[pair]['buy'].append(buy_ex)
                        if sell_ex not in candidates[pair]['sell']:
                            candidates[pair]['sell'].append(sell_ex)
                        # Вивід для діагностики
                        print(
                            f"[Debug] Pair {pair}: buy on {buy_ex} at {buy_ask}, sell on {sell_ex} at {sell_bid}, spread {spread:.4f}%")

        return candidates


if __name__ == "__main__":
    bot = ArbitrageBot()
    asyncio.run(bot.start())
