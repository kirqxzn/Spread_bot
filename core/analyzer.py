from api.orderbook_api import fetch_order_book_price


async def analyze_pair(pair, buy_ex, sell_ex, min_spread_percent, max_spread_percent):
    sell_bid, sell_ask = await fetch_order_book_price(sell_ex, pair)
    buy_bid, buy_ask = await fetch_order_book_price(buy_ex, pair)

    if sell_bid is None or buy_ask is None:
        return None

    spread = (sell_bid - buy_ask) / buy_ask * 100
    if spread < min_spread_percent or spread > max_spread_percent:
        return None

    return {
        'pair': pair,
        'buy_ex': buy_ex,
        'sell_ex': sell_ex,
        'buy_price': buy_ask,
        'sell_price': sell_bid,
        'spread': spread,
    }


async def analyze_arbitrage_opportunities(candidate_pairs, min_spread_percent, max_spread_percent):
    results = []
    excluded = []
    for pair, ex_dict in candidate_pairs.items():
        for buy_ex in ex_dict.get('buy', []):
            for sell_ex in ex_dict.get('sell', []):
                if buy_ex == sell_ex:
                    continue
                res = await analyze_pair(pair, buy_ex, sell_ex, min_spread_percent, max_spread_percent)
                if res:
                    results.append(res)
                else:
                    excluded.append((pair, buy_ex, sell_ex))
    return results, excluded
