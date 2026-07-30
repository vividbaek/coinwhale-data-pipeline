"""
모든 Collector + Poller 통합 실행 스크립트 (멀티 심볼 지원).
- 선물 public: depth, bookTicker
- 선물 market: kline_1m, aggTrade, forceOrder, markPrice@1s, miniTicker, ticker
- 신규 선물: forceOrder + markPrice@1s + bookTicker
- 신규 현물: aggTrade + bookTicker + kline_1m + depth@100ms
- 시장 메트릭: 선물 miniTicker + ticker + compositeIndex
- 현물 메트릭: 현물 miniTicker + ticker
- REST: Open Interest polling (5초), L/S Ratio polling (5분)

심볼 설정:
  - 기본값: common/config.py의 Config.COLLECT_SYMBOLS
  - 환경변수 오버라이드: COLLECT_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT

실행: python3 -m collectors.run_all
"""

from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# .env 로드 — os.getenv 기본값(WS_IDLE_RECONNECT_SEC 등) 적용 전에 먼저 실행
load_dotenv(Path(__file__).parent.parent / ".env")

from collectors.base_collector import BaseBinanceCollector, _dash_log
from collectors.depth_kline_aggtrade import DepthKlineAggTradeCollector
from collectors.futures_insight_collector import FuturesInsightCollector
from collectors.market_metrics_collector import MarketMetricsCollector
from collectors.rest_poller import LongShortRatioPoller, OpenInterestPoller
from collectors.spot_collector import SpotCollector
from collectors.spot_ticker_collector import SpotTickerCollector
from common.config import Config
from common.metrics import start_metrics_server
from utils.binance_stream_enum import BinanceStreamType


def create_collectors_for_symbol(
    symbol: str,
) -> tuple[list[BaseBinanceCollector], list[Any]]:
    """단일 심볼에 대한 전체 collector 세트 생성.

    Returns:
        tuple of (ws_collectors, rest_pollers)
    """
    sym = symbol.lower()

    # 1a) 선물 public: depth
    futures_depth = DepthKlineAggTradeCollector(
        sym,
        [
            BinanceStreamType.DEPTH,
        ],
    )

    # 1b) 선물 market: kline_1m + aggTrade
    futures_trade_kline = DepthKlineAggTradeCollector(
        sym,
        [
            BinanceStreamType.KLINE_1M,
            BinanceStreamType.AGG_TRADE,
        ],
    )

    # 2a) 선물 부가 — bookTicker (고빈도: BTC는 초당 수천 건)
    # bookTicker를 markPrice/forceOrder와 분리: 고빈도 스트림이 동일 WebSocket
    # 버퍼를 독점하면 markPrice(1s 주기)와 forceOrder(이벤트 기반)가 밀려
    # 침묵 상태가 되는 버그 방지.
    futures_insight = FuturesInsightCollector(
        sym,
        [
            BinanceStreamType.BOOK_TICKER,
        ],
    )

    # 2b) 선물 부가 — markPrice@1s + forceOrder (저빈도 전용 연결)
    futures_mark = FuturesInsightCollector(
        sym,
        [
            BinanceStreamType.MARK_PRICE,
            BinanceStreamType.LIQUIDATION_ORDER,
        ],
    )

    # 3) 현물: aggTrade + bookTicker + kline_1m + depth@100ms
    spot = SpotCollector(
        sym,
        [
            BinanceStreamType.AGG_TRADE,
            BinanceStreamType.BOOK_TICKER,
            BinanceStreamType.KLINE_1M,
            BinanceStreamType.DEPTH,
        ],
    )

    # 4) 선물 시장 메트릭: miniTicker + ticker
    # compositeIndex는 USDT 선물에서 BTC/ETH/SOL 대상 스트림이 존재하지 않아
    # 15초마다 idle timeout → 재연결 반복 → Binance IP rate limit 유발. 제거.
    market_metrics = MarketMetricsCollector(
        sym,
        [
            BinanceStreamType.MINI_TICKER,
            BinanceStreamType.TICKER,
        ],
    )

    # 5) 현물 시장 메트릭: miniTicker + ticker
    spot_ticker = SpotTickerCollector(
        sym,
        [
            BinanceStreamType.MINI_TICKER,
            BinanceStreamType.TICKER,
        ],
    )

    # 6) REST poller: Open Interest (5초)
    oi_poller = OpenInterestPoller(symbol.upper(), interval=5.0)

    # 7) REST poller: L/S Ratio 4종 (5분)
    ls_poller = LongShortRatioPoller(symbol.upper(), interval=300.0)

    ws_collectors = [
        futures_depth,
        futures_trade_kline,
        futures_insight,
        futures_mark,
        spot,
        spot_ticker,
        market_metrics,
    ]
    rest_pollers = [oi_poller, ls_poller]

    return ws_collectors, rest_pollers


async def run_ws_collector_task(collector: BaseBinanceCollector, semaphore: asyncio.Semaphore) -> None:
    """Semaphore는 연결 시작 시 순차 throttle에만 사용 (모든 심볼 WS 가동 보장)."""
    async with semaphore:
        pass  # 동시 시작 수만 제한 — release 후 collector 실행
    await collector.start()


async def main() -> None:
    symbols = Config.COLLECT_SYMBOLS

    # Prometheus 메트릭 서버 - 프로세스당 고유 포트 필요 (심볼별 분리 실행 시)
    metrics_port = int(os.getenv("METRICS_PORT", "0")) or 8889
    start_metrics_server(metrics_port)

    all_ws_collectors: list[BaseBinanceCollector] = []
    all_rest_pollers: list[Any] = []

    for symbol in symbols:
        ws_collectors, rest_pollers = create_collectors_for_symbol(symbol)
        all_ws_collectors.extend(ws_collectors)
        all_rest_pollers.extend(rest_pollers)

    loop = asyncio.get_event_loop()

    def shutdown_handler():
        _dash_log("종료 신호 수신, 모든 collector 중단 중...")
        for c in all_ws_collectors:
            c.running = False
        for p in all_rest_pollers:
            p.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_handler)

    # 동시 WebSocket 연결 수 제한 (Binance connection limit 완화 + CPU 경감)
    # 각 심볼당 7개 WS, 3심볼 = 21동시 → 10으로 제한
    _MAX_CONCURRENT_WS = 10

    topic_count = 18 * len(symbols)
    print("=" * 60)
    print("  CoinWhale Data Collector - Multi Symbol")
    print(f"  symbols: {', '.join(symbols)}")
    print(f"  streams per symbol: 18 | total: {topic_count}")
    print(f"  max concurrent WS: {_MAX_CONCURRENT_WS}")
    print("=" * 60)

    ws_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_WS)

    tasks = []
    # WebSocket collectors: semaphore로 동시 연결 수 제한
    for c in all_ws_collectors:
        tasks.append(asyncio.create_task(run_ws_collector_task(c, ws_semaphore)))
    # REST pollers: 별도 tasks (동시성 제한 없음, 빈도 낮음)
    for p in all_rest_pollers:
        tasks.append(asyncio.create_task(p.start()))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass

    print("\n모든 collector 종료 완료")


if __name__ == "__main__":
    asyncio.run(main())
