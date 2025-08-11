from .constants import STABLECOINS

def is_stablecoin_pair(symbol: str) -> bool:
    for stablecoin in STABLECOINS:
        if symbol.endswith(stablecoin):
            return True
    return False
