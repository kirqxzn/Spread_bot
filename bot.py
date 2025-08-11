import asyncio
import aiohttp

from utils.logger import log
from utils.constants import MIN_SPREAD_PERCENT, MAX_SPREAD_PERCENT, EXCHANGES
from utils.helpers import is_stablecoin_pair

from api.spot_api import SpotAPI
from api.futures_api import FuturesAPI
from core.quick_price import fetch_last_prices, fetch_quick_prices, find_candidates_by_last_price, find_candidates_by_quick_prices
from core.analyzer import analyze_arbitrage_opportunities
from core.printer import print_candidates_by_last_price, print_candidates_table, print_arbitrage_opportunities


class ArbitrageBot:
    def __init__(self):
        self.exchanges = EXCHANGES
        self.min_spread_percent = MIN_SPREAD_PERCENT
        self.max_spread_percent = MAX_SPREAD_PERCENT
        self.session = None

        self.spot_api = SpotAPI()
        self.futures_api = FuturesAPI()

        self.spot_pairs = {ex: set() for ex in self.exchanges}
        self.futures_pairs = {ex: set() for ex in self.exchanges}

        # Закоментовано, бо тут поки пусто
        # for ex, pairs in self.spot_pairs.items():
        #     log(f"[Debug] {ex} spot pairs count: {len(pairs)}")
        #     log(f"[Debug] {ex} spot pairs sample: {list(pairs)[:5]}")

        # for ex, pairs in self.futures_pairs.items():
        #     log(f"[Debug] {ex} futures pairs count: {len(pairs)}")
        #     log(f"[Debug] {ex} futures pairs sample: {list(pairs)[:5]}")

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            log("[Start] Starting arbitrage bot")

            # Load pairs
            await self.spot_api.load_all_pairs()
            self.spot_pairs = self.spot_api.spot_pairs
            log(f"[Info] Spot pairs loaded")

            # Логування після завантаження spot_pairs
            for ex, pairs in self.spot_pairs.items():
                log(f"[Debug] {ex} spot pairs count: {len(pairs)}")
                log(f"[Debug] {ex} spot pairs sample: {list(pairs)[:5]}")

            await self.futures_api.load_futures_pairs()
            self.futures_pairs = self.futures_api.futures_pairs
            log(f"[Info] Futures pairs loaded")

            # Логування після завантаження futures_pairs
            for ex, pairs in self.futures_pairs.items():
                log(f"[Debug] {ex} futures pairs count: {len(pairs)}")
                log(f"[Debug] {ex} futures pairs sample: {list(pairs)[:5]}")

            # Fetch last prices & find candidates by last price
            last_prices = await fetch_last_prices(self.exchanges)
            log(f"[Info] Last prices fetched")
            for ex, prices in last_prices.items():
                log(f"[Debug] {ex} last_prices count: {len(prices)}")
                sample = list(prices.items())[:5]
                log(f"[Debug] {ex} sample last prices: {sample}")

            candidates_last = find_candidates_by_last_price(
                last_prices,
                self.spot_pairs,
                self.futures_pairs,
                self.exchanges,
                self.min_spread_percent,
                self.max_spread_percent
            )
            log(f"[Info] Candidates from last prices: {len(candidates_last)}")
            print_candidates_by_last_price(candidates_last, last_prices, self.min_spread_percent,
                                           self.max_spread_percent)

            # Fetch quick prices & filter candidates by bid/ask spread
            quick_prices = await fetch_quick_prices(self.exchanges)
            log(f"[Info] Quick prices fetched")

            candidates_quick = find_candidates_by_quick_prices(
                quick_prices,
                candidates_last,
                self.min_spread_percent,
                self.max_spread_percent
            )
            log(f"[Info] Candidates from quick prices (filtered): {len(candidates_quick)}")
            print_candidates_table(candidates_quick, quick_prices, self.min_spread_percent, self.max_spread_percent)

            # Analyze arbitrage opportunities deeper if хочеш
            results, _ = await analyze_arbitrage_opportunities(
                candidates_quick,
                self.min_spread_percent,
                self.max_spread_percent
            )
            # Потрібно імпортувати print_arbitrage_opportunities
            print_arbitrage_opportunities(results)


if __name__ == "__main__":
    asyncio.run(ArbitrageBot().start())
