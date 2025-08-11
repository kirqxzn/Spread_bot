import aiohttp
from utils.logger import log


class FuturesAPI:
    def __init__(self):
        self.futures_pairs = {
            'BINANCE': set(),
            'MEXC': set(),
            'KUCOIN': set(),
        }

    async def load_futures_pairs(self):
        async with aiohttp.ClientSession() as session:

            # BINANCE
            try:
                url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
                async with session.get(url) as resp:
                    data = await resp.json()
                    self.futures_pairs['BINANCE'] = {
                        s['symbol'] for s in data['symbols']
                        if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING'
                    }
                    log(f"[Info] Binance Futures: Loaded {len(self.futures_pairs['BINANCE'])} pairs")
            except Exception as e:
                log(f"[Error] Binance futures fetch exception: {e}")

            # MEXC
            try:
                url = "https://contract.mexc.com/api/v1/contract/detail"
                async with session.get(url) as resp:
                    data = await resp.json()
                    self.futures_pairs['MEXC'] = {
                        p['symbol'].replace('_', '').upper() for p in data['data']
                    }
                    log(f"[Info] MEXC Futures: Loaded {len(self.futures_pairs['MEXC'])} pairs")
            except Exception as e:
                log(f"[Error] MEXC futures fetch exception: {e}")

            # KUCOIN
            try:
                url = 'https://api-futures.kucoin.com/api/v1/contracts/active'
                async with session.get(url) as resp:
                    data = await resp.json()
                    self.futures_pairs['KUCOIN'] = {
                        p['symbol'].replace('-', '').upper() for p in data['data']
                    }
                    log(f"[Info] KuCoin Futures: Loaded {len(self.futures_pairs['KUCOIN'])} pairs")
            except Exception as e:
                log(f"[Error] KuCoin futures fetch exception: {e}")
