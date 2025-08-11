import aiohttp
from utils.logger import log


async def get_all_exchange_volumes(candidate_pairs, available_pairs):
    result = {}
    async with aiohttp.ClientSession() as session:

        # Binance volumes
        binance_volumes = {}
        try:
            url = 'https://api.binance.com/api/v3/ticker/24hr'
            async with session.get(url) as resp:
                data = await resp.json()
                for item in data:
                    symbol = item['symbol']
                    if symbol in available_pairs.get('BINANCE', set()):
                        price = float(item.get('lastPrice') or 0)
                        volume = float(item.get('volume') or 0)
                        binance_volumes[symbol] = price * volume
        except Exception as e:
            log(f"[Error] Binance volumes fetch: {e}")

        # KuCoin volumes
        kucoin_volumes = {}
        try:
            url = 'https://api.kucoin.com/api/v1/market/allTickers'
            async with session.get(url) as resp:
                data = await resp.json()
                for t in data['data']['ticker']:
                    symbol = t['symbol'].replace('-', '')
                    if symbol in available_pairs.get('KUCOIN', set()):
                        price = float(t.get('last') or 0)
                        volume = float(t.get('vol') or 0)
                        kucoin_volumes[symbol] = price * volume
        except Exception as e:
            log(f"[Error] KuCoin volumes fetch: {e}")

        # MEXC volumes
        mexc_volumes = {}
        try:
            url = 'https://api.mexc.com/api/v3/ticker/24hr'
            async with session.get(url) as resp:
                data = await resp.json()
                for item in data:
                    symbol = item['symbol']
                    if symbol in available_pairs.get('MEXC', set()):
                        price = float(item.get('lastPrice') or 0)
                        volume = float(item.get('volume') or 0)
                        mexc_volumes[symbol] = price * volume
        except Exception as e:
            log(f"[Error] MEXC volumes fetch: {e}")

        # Combine results
        for pair, ex_dict in candidate_pairs.items():
            result[pair] = {}
            for ex in ex_dict.get('buy', []) + ex_dict.get('sell', []):
                if ex == 'BINANCE':
                    result[pair][ex] = binance_volumes.get(pair, 0)
                elif ex == 'KUCOIN':
                    result[pair][ex] = kucoin_volumes.get(pair, 0)
                elif ex == 'MEXC':
                    result[pair][ex] = mexc_volumes.get(pair, 0)

    return result
