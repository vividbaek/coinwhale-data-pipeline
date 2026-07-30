# spark_jobs/silver/liquidation.py
"""
Insight 2: 강제 청산 (Liquidation Cascade) 집계.
담당: 팀원 C

소스 토픽: binance-liquidation (forceOrder)
저장 테이블: boaz.insight_liquidation
집계 방식: SUM (10초간 청산 건수·물량·USD 가치 누적)

스키마 컬럼:
  liq_long_count/vol/usd  — side=SELL (롱 청산 → 하락 압력)
  liq_short_count/vol/usd — side=BUY  (숏 청산 → 상승 압력, short squeeze)

전처리 포인트:
  - liq_long_usd / liq_short_usd: price × quantity → USD 가치 계산
    BTC 단위만으로는 규모 파악 불가 (BTC 가격에 따라 의미가 달라지기 때문)
"""

import kafka_reader as _kr
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, from_unixtime
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when, window
from silver.base import SilverBase


class LiquidationInsight(SilverBase):
    TOPIC = "binance-liquidation"
    TABLE = "liquidation"
    CHECKPOINT = "/opt/spark/work-dir/checkpoints/stream-liquidation"
    ENABLED = True
    WATERMARK_DELAY = "5 minutes"

    def parse(self, raw_df: DataFrame) -> DataFrame:
        return _kr.parse_liquidation_data(raw_df)

    def aggregate(self, parsed_df: DataFrame) -> DataFrame:
        with_ts = parsed_df.withColumn(
            "event_ts", from_unixtime(col("event_time_sec")).cast("timestamp")
        ).withWatermark("event_ts", self.WATERMARK_DELAY)

        return with_ts.groupBy(window("event_ts", self.WINDOW_DURATION), "symbol").agg(
            # 롱 청산: side=SELL (강제 매도 → 하락 압력)
            count(when(col("side") == "SELL", True)).alias("liq_long_count"),
            spark_sum(when(col("side") == "SELL", col("quantity")).otherwise(0)).alias("liq_long_vol"),
            spark_sum(when(col("side") == "SELL", col("price") * col("quantity")).otherwise(0)).alias(
                "liq_long_usd"
            ),  # ← 전처리: BTC→USD 환산
            # 숏 청산: side=BUY (강제 매수 → 상승 압력)
            count(when(col("side") == "BUY", True)).alias("liq_short_count"),
            spark_sum(when(col("side") == "BUY", col("quantity")).otherwise(0)).alias("liq_short_vol"),
            spark_sum(when(col("side") == "BUY", col("price") * col("quantity")).otherwise(0)).alias(
                "liq_short_usd"
            ),  # ← 전처리: BTC→USD 환산
        )

    def select_output(self, agg_df: DataFrame) -> DataFrame:
        return agg_df.select(
            col("window.start").cast("timestamp").alias("ts"),
            col("symbol"),
            col("liq_long_count").cast("int"),
            col("liq_long_vol"),
            col("liq_long_usd"),
            col("liq_short_count").cast("int"),
            col("liq_short_vol"),
            col("liq_short_usd"),
        )
