from prettytable import PrettyTable


def print_candidates_by_last_price(candidates, last_prices, min_spread_percent, max_spread_percent):
    """
    Вивід таблиці кандидатів, знайдених за last price (середньою ціною).
    last_prices — це словник {exchange: {pair: price}}.
    """
    if not candidates:
        print("🚫 No candidates found by last price.")
        return
    table = PrettyTable()
    table.field_names = ["Pair", "Buy Exchange", "Sell Exchange", "Buy Price", "Sell Price", "Approx Spread %"]
    for pair, ex_dict in candidates.items():
        buys = ex_dict.get('buy', [])
        sells = ex_dict.get('sell', [])
        for buy_ex in buys:
            for sell_ex in sells:
                if buy_ex == sell_ex:
                    continue
                buy_price = last_prices.get(buy_ex, {}).get(pair)
                sell_price = last_prices.get(sell_ex, {}).get(pair)
                if buy_price is None or sell_price is None:
                    continue
                spread = (sell_price - buy_price) / buy_price * 100
                if not (min_spread_percent <= spread <= max_spread_percent):
                    continue
                table.add_row([
                    pair,
                    buy_ex,
                    sell_ex,
                    f"{buy_price:.8f}",
                    f"{sell_price:.8f}",
                    f"{spread:.4f}"
                ])
    print("🟢 Candidates based on last prices:")
    print(table)


def print_candidates_table(candidates, quick_prices, min_spread_percent, max_spread_percent):
    if not candidates:
        print("🚫 No candidates found in quick prices.")
        return
    table = PrettyTable()
    table.field_names = ["Pair", "Buy Exchange", "Sell Exchange", "Buy Ask", "Sell Bid", "Approx Spread %"]
    for pair, ex_dict in candidates.items():
        buys = ex_dict.get('buy', [])
        sells = ex_dict.get('sell', [])
        for buy_ex in buys:
            for sell_ex in sells:
                if buy_ex == sell_ex:
                    continue
                buy_ask = quick_prices.get(buy_ex, {}).get(pair, {}).get('ask')
                sell_bid = quick_prices.get(sell_ex, {}).get(pair, {}).get('bid')
                if buy_ask is None or sell_bid is None:
                    continue
                spread = (sell_bid - buy_ask) / buy_ask * 100
                if not (min_spread_percent <= spread <= max_spread_percent):
                    continue
                table.add_row([
                    pair,
                    buy_ex,
                    sell_ex,
                    f"{buy_ask:.8f}",
                    f"{sell_bid:.8f}",
                    f"{spread:.4f}"
                ])
    print("🟡 Candidates based on quick prices:")
    print(table)


def print_arbitrage_opportunities(opportunities):
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
