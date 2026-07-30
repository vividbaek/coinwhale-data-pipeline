# spark_jobs/silver/market_metrics.py
"""
Insight: 24h 시장 요약 지표 (miniTicker / ticker 기반).
담당: 팀원 C

소스 토픽: binance-ticker (선물 miniTicker + ticker)
저장 테이블: boaz.insight_market_metrics
집계 방식: LAST (10초 윈도우의 마지막 스냅샷)

스키마 컬럼:
  price_change_pct, weighted_avg_price, last_price,
  volume_24h, quote_volume_24h, high_24h, low_24h, open_price_24h,
  composite_price (Nullable — compositeIndex 파서 추가 시 연결)

확장 포인트:
  - spot-ticker 추가 시: start() override → union 패턴 (CVDInsight 참고)
  - compositeIndex 연결 시: parse() 확장 또는 별도 join 로직
"""

import kafka_reader as _kr
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, last, lit, window
from silver.base import SilverBase


class MarketMetricsInsight(SilverBase):
    TOPIC = "binance-ticker"
    TABLE = "market_metrics"
    CHECKPOINT = "/opt/spark/work-dir/checkpoints/stream-market-metrics"
    ENABLED = True

    def parse(self, raw_df: DataFrame) -> DataFrame:
        return _kr.parse_ticker_data(raw_df)

    def aggregate(self, parsed_df: DataFrame) -> DataFrame:
        with_ts = parsed_df.withColumn(
            "event_ts", from_unixtime(col("event_time_sec")).cast("timestamp")
        ).withWatermark("event_ts", self.WATERMARK_DELAY)

        return with_ts.groupBy(window("event_ts", self.WINDOW_DURATION), "symbol").agg(
            last("price_change_pct").alias("price_change_pct"),
            last("weighted_avg_price").alias("weighted_avg_price"),
            last("last_price").alias("last_price"),
            last("volume_24h").alias("volume_24h"),
            last("quote_volume_24h").alias("quote_volume_24h"),
            last("high_24h").alias("high_24h"),
            last("low_24h").alias("low_24h"),
            last("open_price_24h").alias("open_price_24h"),
        )

    def select_output(self, agg_df: DataFrame) -> DataFrame:
        return agg_df.select(
            col("window.start").cast("timestamp").alias("ts"),
            col("symbol"),
            col("price_change_pct"),
            col("weighted_avg_price"),
            col("last_price"),
            col("volume_24h"),
            col("quote_volume_24h"),
            col("high_24h"),
            col("low_24h"),
            col("open_price_24h"),
            lit(None).cast("double").alias("composite_price"),  # TODO: compositeIndex 토픽 파서 추가 후 연결
        )
