"""
현물 시장 메트릭 수집: miniTicker + ticker
- spot-ticker: 현물 24h 시장 요약 (miniTicker/ticker 통합)

데이터 확인용: python3 -m collectors.spot_ticker_collector
"""

import asyncio

from collectors.base_collector import BaseBinanceCollector
from utils.binance_stream_enum import BinanceStreamType


class SpotTickerCollector(BaseBinanceCollector):
    def __init__(self, symbol: str, streams: list):
        super().__init__(symbol, streams, market_type="spot")

    async def process_data(self, stream_name: str, payload: dict):
        await self._send_to_kafka(stream_name, payload)


if __name__ == "__main__":
    streams = [
        BinanceStreamType.MINI_TICKER,
        BinanceStreamType.TICKER,
    ]
    collector = SpotTickerCollector("btcusdt", streams)
    asyncio.run(collector.start())
