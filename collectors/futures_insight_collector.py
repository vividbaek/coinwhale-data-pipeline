"""
선물 추가 스트림 수집: forceOrder + markPrice@1s + bookTicker
- binance-liquidation: 강제 청산 (forceOrder)
- binance-markprice: 마크 가격 (1초 간격)
- binance-bookticker: 선물 최우선 호가

데이터 확인용: python3 -m collectors.futures_insight_collector
"""

import asyncio

from collectors.base_collector import BaseBinanceCollector
from utils.binance_stream_enum import BinanceStreamType


class FuturesInsightCollector(BaseBinanceCollector):
    async def process_data(self, stream_name: str, payload: dict):
        await self._send_to_kafka(stream_name, payload)


if __name__ == "__main__":

    async def main():
        collectors = [
            FuturesInsightCollector("btcusdt", [BinanceStreamType.BOOK_TICKER]),
            FuturesInsightCollector(
                "btcusdt",
                [BinanceStreamType.LIQUIDATION_ORDER, BinanceStreamType.MARK_PRICE],
            ),
        ]
        await asyncio.gather(*(collector.start() for collector in collectors))

    asyncio.run(main())
