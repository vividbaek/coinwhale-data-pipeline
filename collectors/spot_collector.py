"""
현물(Spot) aggTrade + bookTicker 수집.
- spot-trade: 현물 체결 데이터
- spot-bookticker: 현물 최우선 호가

데이터 확인용: python3 -m collectors.spot_collector
"""

import asyncio

from collectors.base_collector import BaseBinanceCollector
from utils.binance_stream_enum import BinanceStreamType


class SpotCollector(BaseBinanceCollector):
    def __init__(self, symbol: str, streams: list):
        super().__init__(symbol, streams, market_type="spot")

    async def process_data(self, stream_name: str, payload: dict):
        await self._send_to_kafka(stream_name, payload)


if __name__ == "__main__":
    streams = [
        BinanceStreamType.AGG_TRADE,
        BinanceStreamType.BOOK_TICKER,
    ]
    collector = SpotCollector("btcusdt", streams)
    asyncio.run(collector.start())
