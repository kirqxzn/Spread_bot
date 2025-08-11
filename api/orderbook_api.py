import aiohttp
from utils.logger import log


async def fetch_order_book_price(exchange, pair):
    try:
        async with aiohttp.ClientSession() as session:
            if exchange == 'BINANCE':
                url = f'https://api.binance.com/api/v3/depth?symbol={pair}&limit=5'
                async with session.get(url) as resp:
                    data = await resp.json()
                    return float(data['bids'][0][0]), float(data['asks'][0][0])

            elif exchange == 'KUCOIN':
                symbol = pair[:-4] + '-' + pair[-4:]
                url = f'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={symbol}'
                async with session.get(url) as resp:
                    data = await resp.json()
                    return float(data['data']['bestBid']), float(data['data']['bestAsk'])

            elif exchange == 'MEXC':
                url = f'https://api.mexc.com/api/v3/depth?symbol={pair}&limit=5'
                async with session.get(url) as resp:
                    data = await resp.json()
                    return float(data['bids'][0][0]), float(data['asks'][0][0])

    except Exception as e:
        log(f"[Error] Order book fetch error from {exchange} for {pair}: {e}")
        return None, None
