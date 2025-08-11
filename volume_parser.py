import aiohttp
from datetime import datetime

MIN_VOLUME_USDT_24H = 100000  # мінімальний обсяг для фільтрації


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


async def get_all_exchange_volumes(candidate_pairs, available_pairs):
    """
    Отримує обсяги 24h для заданих пар на заданих біржах.
    - candidate_pairs: {pair: {'buy': [...], 'sell': [...]}}
    - available_pairs: {exchange: set(пари)}

    Повертає:
    {pair: {exchange: volume_usdt, ...}, ...}
    """

    result = {}

    async with aiohttp.ClientSession() as session:
        # Для кожної біржі отримаємо загальні обсяги разом
        # (API повертають всі пари, ми потім беремо тільки потрібні)

        # Приклад для Binance:
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

        # Аналогічно KuCoin
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

        # І MEXC
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

        # Тепер формуємо результат для candidate_pairs:
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
                        # Якщо обсягу немає, можна поставити 0
                        result[pair][ex] = 0

    return result
