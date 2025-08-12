import aiohttp
from utils.logger import log
from utils.helpers import is_stablecoin_pair
from prettytable import PrettyTable


def normalize_symbol(exchange, symbol):
    if exchange == 'KUCOIN':
        return symbol.replace('-', '').upper()
    elif exchange == 'MEXC':
        return symbol.replace('_', '').upper()
    else:  # BINANCE та інші
        return symbol.upper()


async def fetch_quick_prices(exchanges):
    results = {ex: {} for ex in exchanges}

    async with aiohttp.ClientSession() as session:
        if 'BINANCE' in exchanges:
            try:
                url = 'https://api.binance.com/api/v3/ticker/bookTicker'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        sym = normalize_symbol('BINANCE', item['symbol'])
                        results['BINANCE'][sym] = {
                            'bid': float(item['bidPrice']),
                            'ask': float(item['askPrice']),
                        }
                log(f"[Info] Binance quick prices loaded: {len(results['BINANCE'])} items")
            except Exception as e:
                log(f"[Error] Binance quick prices: {e}")

        if 'KUCOIN' in exchanges:
            try:
                url = 'https://api.kucoin.com/api/v1/market/allTickers'
                async with session.get(url) as resp:
                    data = await resp.json()
                    count = 0
                    for t in data['data']['ticker']:
                        sym = normalize_symbol('KUCOIN', t['symbol'])
                        bid = float(t.get('buy') or 0)
                        ask = float(t.get('sell') or 0)
                        if bid > 0 and ask > 0:
                            results['KUCOIN'][sym] = {'bid': bid, 'ask': ask}
                        count += 1
                log(f"[Info] KuCoin quick prices loaded: {count} items")
            except Exception as e:
                log(f"[Error] KuCoin quick prices: {e}")

        if 'MEXC' in exchanges:
            try:
                url = 'https://api.mexc.com/api/v3/ticker/bookTicker'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        sym = normalize_symbol('MEXC', item['symbol'])
                        results['MEXC'][sym] = {
                            'bid': float(item['bidPrice']),
                            'ask': float(item['askPrice']),
                        }
                log(f"[Info] MEXC quick prices loaded: {len(results['MEXC'])} items")
            except Exception as e:
                log(f"[Error] MEXC quick prices: {e}")

    return results


async def fetch_last_prices(exchanges):
    results = {ex: {} for ex in exchanges}

    async with aiohttp.ClientSession() as session:
        if 'BINANCE' in exchanges:
            try:
                url = 'https://api.binance.com/api/v3/ticker/price'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        sym = normalize_symbol('BINANCE', item['symbol'])
                        price = float(item['price'])
                        results['BINANCE'][sym] = price
                log(f"[Info] Binance last prices loaded: {len(results['BINANCE'])} items")
            except Exception as e:
                log(f"[Error] Binance last prices: {e}")

        if 'KUCOIN' in exchanges:
            try:
                url = 'https://api.kucoin.com/api/v1/market/allTickers'
                async with session.get(url) as resp:
                    data = await resp.json()
                    count = 0
                    for t in data['data']['ticker']:
                        sym = normalize_symbol('KUCOIN', t['symbol'])
                        price = float(t.get('last') or 0)
                        if price > 0:
                            results['KUCOIN'][sym] = price
                            count += 1
                log(f"[Info] KuCoin last prices loaded: {count} items")
            except Exception as e:
                log(f"[Error] KuCoin last prices: {e}")

        if 'MEXC' in exchanges:
            try:
                url = 'https://api.mexc.com/api/v3/ticker/price'
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data:
                        sym = normalize_symbol('MEXC', item['symbol'])
                        price = float(item['price'])
                        results['MEXC'][sym] = price
                log(f"[Info] MEXC last prices loaded: {len(results['MEXC'])} items")
            except Exception as e:
                log(f"[Error] MEXC last prices: {e}")

    return results


def find_candidates_by_last_price(last_prices, spot_pairs, futures_pairs, exchanges, min_spread_percent, max_spread_percent):
    candidates = {}
    all_pairs = set()
    for ex_prices in last_prices.values():
        all_pairs.update(ex_prices.keys())

    # Тільки інформативний лог, можна закоментувати або прибрати, якщо хочеш
    log(f"[Info] Total unique pairs in last_prices: {len(all_pairs)}")

    for pair in all_pairs:
        if not is_stablecoin_pair(pair):
            continue

        buy_exchanges = []
        sell_exchanges = []

        for ex in exchanges:
            if pair in last_prices.get(ex, {}) and pair in spot_pairs.get(ex, set()):
                buy_exchanges.append(ex)
            if pair in last_prices.get(ex, {}) and pair in futures_pairs.get(ex, set()):
                sell_exchanges.append(ex)

        if not buy_exchanges or not sell_exchanges:
            continue

        for buy_ex in buy_exchanges:
            for sell_ex in sell_exchanges:
                if buy_ex == sell_ex:
                    continue

                buy_price = last_prices[buy_ex][pair]
                sell_price = last_prices[sell_ex][pair]

                if buy_price <= 0 or sell_price <= 0:
                    continue
                if buy_price >= sell_price:
                    continue

                spread = (sell_price - buy_price) / buy_price * 100
                if min_spread_percent <= spread <= max_spread_percent:
                    if pair not in candidates:
                        candidates[pair] = {'buy': [], 'sell': []}
                    if buy_ex not in candidates[pair]['buy']:
                        candidates[pair]['buy'].append(buy_ex)
                    if sell_ex not in candidates[pair]['sell']:
                        candidates[pair]['sell'].append(sell_ex)

    log(f"[Info] Total candidates found by last price: {len(candidates)}")
    return candidates


def find_candidates_by_quick_prices(quick_prices, candidate_pairs, min_spread_percent, max_spread_percent):
    filtered_candidates = {}
    for pair, exchanges in candidate_pairs.items():
        for buy_ex in exchanges['buy']:
            for sell_ex in exchanges['sell']:
                if buy_ex == sell_ex:
                    continue
                if pair not in quick_prices.get(buy_ex, {}) or pair not in quick_prices.get(sell_ex, {}):
                    continue

                buy_ask = quick_prices[buy_ex][pair]['ask']
                sell_bid = quick_prices[sell_ex][pair]['bid']

                if buy_ask <= 0 or sell_bid <= 0:
                    continue
                if buy_ask >= sell_bid:
                    continue

                spread = (sell_bid - buy_ask) / buy_ask * 100
                if min_spread_percent <= spread <= max_spread_percent:
                    if pair not in filtered_candidates:
                        filtered_candidates[pair] = {'buy': [], 'sell': [], 'spread': spread}
                    if buy_ex not in filtered_candidates[pair]['buy']:
                        filtered_candidates[pair]['buy'].append(buy_ex)
                    if sell_ex not in filtered_candidates[pair]['sell']:
                        filtered_candidates[pair]['sell'].append(sell_ex)

    log(f"[Info] Total candidates found by quick prices: {len(filtered_candidates)}")
    return filtered_candidates
