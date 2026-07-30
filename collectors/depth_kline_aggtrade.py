"""
Depth + 1분봉(kline_1m) + aggTrade 수집.
- binance-depth: 호가 스트림 (초단위 고빈도)
- binance-kline: Binance가 만든 1분봉 (이미 1분 집계됨)
- binance-trade: aggTrade 체결 (거래 단위)

데이터 확인용: python3 -m collectors.depth_kline_aggtrade
"""

import asyncio

from collectors.base_collector import BaseBinanceCollector
from utils.binance_stream_enum import BinanceStreamType


class DepthKlineAggTradeCollector(BaseBinanceCollector):
    async def process_data(self, stream_name: str, payload: dict):
        await self._send_to_kafka(stream_name, payload)


if __name__ == "__main__":

    async def main():
        collectors = [
            DepthKlineAggTradeCollector("btcusdt", [BinanceStreamType.DEPTH]),
            DepthKlineAggTradeCollector(
                "btcusdt",
                [BinanceStreamType.KLINE_1M, BinanceStreamType.AGG_TRADE],
            ),
        ]
        await asyncio.gather(*(collector.start() for collector in collectors))

    asyncio.run(main())
