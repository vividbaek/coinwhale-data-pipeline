"""
선물 시장 메트릭 수집: miniTicker + ticker + compositeIndex
- binance-ticker: 24h 시장 요약 (miniTicker/ticker 통합)
- binance-composite-index: 복합 지수 (BTC 전용)

데이터 확인용: python3 -m collectors.market_metrics_collector
"""

import asyncio

from collectors.base_collector import BaseBinanceCollector
from utils.binance_stream_enum import BinanceStreamType


class MarketMetricsCollector(BaseBinanceCollector):
    async def process_data(self, stream_name: str, payload: dict):
        await self._send_to_kafka(stream_name, payload)


if __name__ == "__main__":
    streams = [
        BinanceStreamType.MINI_TICKER,
        BinanceStreamType.TICKER,
        BinanceStreamType.COMPOSITE_INDEX,
    ]
    collector = MarketMetricsCollector("btcusdt", streams)
    asyncio.run(collector.start())
