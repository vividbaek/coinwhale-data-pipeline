import asyncio

from collectors.base_collector import BaseBinanceCollector
from utils.binance_stream_enum import BinanceStreamType


class BookTickerDepthCollector(BaseBinanceCollector):
    async def process_data(self, stream_name: str, payload: dict):
        # kafka로 전송송
        await self._send_to_kafka(stream_name, payload)


# 실행 예시
if __name__ == "__main__":
    # Enum을 사용하여 타입 안전성 확보
    streams = [BinanceStreamType.DEPTH]
    collector = BookTickerDepthCollector("btcusdt", streams)
    asyncio.run(collector.start())
