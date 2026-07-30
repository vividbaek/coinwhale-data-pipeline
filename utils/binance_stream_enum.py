# utils/binance_stream_enum.py
from enum import Enum


class BinanceStreamType(Enum):
    TRADE = "trade"
    AGG_TRADE = "aggTrade"
    KLINE = "kline"
    KLINE_1M = "kline_1m"  # 1분봉 (Binance 스트림명: symbol@kline_1m)
    BOOK_TICKER = "bookTicker"
    DEPTH = "depth@100ms"
    MINI_TICKER = "miniTicker"
    TICKER = "ticker"
    OPEN_INTEREST = "openInterest"
    FUNDING_RATE = "fundingRate"
    TAKER_LONG_SHORT_RATIO = "takerLongShortRatio"
    MARK_PRICE = "markPrice@1s"
    LIQUIDATION_ORDER = "forceOrder"
    COMPOSITE_INDEX = "compositeIndex"
