import asyncio
from bot import ArbitrageBot

if __name__ == "__main__":
    bot = ArbitrageBot()
    asyncio.run(bot.start())
