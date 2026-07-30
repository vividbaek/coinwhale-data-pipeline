# spark_jobs/silver/funding.py
"""
Insight 4: 마크 가격 + Funding Rate 스냅샷.
담당: 팀원 C

소스 토픽: binance-markprice (markPrice@1s)
저장 테이블: boaz.insight_funding
집계 방식: LAST (10초 윈도우의 마지막 값)

스키마 컬럼:
  mark_price, funding_rate, index_price, next_funding_time, mark_premium_pct

전처리 포인트:
  - mark_premium_pct: (mark_price - index_price) / index_price * 100
    → 선물-현물 내재 괴리율. 양수=선물 프리미엄(롱 과열), 음수=디스카운트
    → 매번 조회 시 수식 재계산 비용 제거. 인사이트 엔진에서 바로 threshold 판단 가능.
"""

import kafka_reader as _kr
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, last, window
from silver.base import SilverBase


class FundingInsight(SilverBase):
    TOPIC = "binance-markprice"
    TABLE = "funding"
    CHECKPOINT = "/opt/spark/work-dir/checkpoints/stream-funding"
    ENABLED = True

    def parse(self, raw_df: DataFrame) -> DataFrame:
        return _kr.parse_markprice_data(raw_df)

    def aggregate(self, parsed_df: DataFrame) -> DataFrame:
        with_ts = parsed_df.withColumn(
            "event_ts", from_unixtime(col("event_time_sec")).cast("timestamp")
        ).withWatermark("event_ts", self.WATERMARK_DELAY)

        return with_ts.groupBy(window("event_ts", self.WINDOW_DURATION), "symbol").agg(
            last("mark_price").alias("mark_price"),
            last("funding_rate").alias("funding_rate"),
            last("index_price").alias("index_price"),
            last("next_funding_time_sec").alias("next_funding_time_sec"),
        )

    def select_output(self, agg_df: DataFrame) -> DataFrame:
        mark = col("mark_price")
        index = col("index_price")
        return agg_df.select(
            col("window.start").cast("timestamp").alias("ts"),
            col("symbol"),
            mark,
            col("funding_rate"),
            index,
            from_unixtime(col("next_funding_time_sec")).cast("timestamp").alias("next_funding_time"),
            # 전처리: mark-index premium (%) — 조회 시 재계산 불필요
            ((mark - index) / index * 100).alias("mark_premium_pct"),
        )
