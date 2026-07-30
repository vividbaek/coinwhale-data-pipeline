# spark_jobs/silver/ls_ratio.py
"""
롱/숏 비율 Silver 집계.

소스 토픽 (4개, 5분 간격 REST polling):
  - binance-ls-ratio       : 전체 계정 롱/숏 비율 (globalLongShortAccountRatio)
  - binance-top-ls-account : 상위 트레이더 계정 기준 롱/숏
  - binance-top-ls-position: 상위 트레이더 포지션 기준 롱/숏
  - binance-taker-ls-ratio : 테이커 매수/매도 비율

집계 방식:
  - 4개 토픽 unionByName → 5분 윈도우 LAST → stream.ls_ratio
  - ratio_type 컬럼($.stream)으로 출처 구분

설계 근거:
  REST polling 5분 간격 데이터이므로 별도 고빈도 집계 불필요.
  단순 LAST 값으로 최신 비율 보존.
  taker 비율은 long_ratio/short_ratio = 0.0 (계정 분류 없음),
  ls_ratio = buySellRatio로 해석.
"""

from __future__ import annotations

import kafka_reader as _kr
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_unixtime, last, window
from silver.base import SilverBase

_LS_TOPICS: list[str] = [
    "binance-ls-ratio",
    "binance-top-ls-account",
    "binance-top-ls-position",
    "binance-taker-ls-ratio",
]


class LSRatioInsight(SilverBase):
    TOPIC = "binance-ls-ratio"  # 대표 토픽 (실제론 4개 union)
    TABLE = "ls_ratio"
    CHECKPOINT = "/opt/spark/work-dir/checkpoints/stream-ls-ratio"
    ENABLED = True
    WINDOW_DURATION = "5 minutes"  # REST polling 주기에 맞춤
    WATERMARK_DELAY = "10 minutes"
    TRIGGER_INTERVAL = "30 seconds"  # 5분 1건이므로 30초 트리거로 충분

    def parse(self, raw_df: DataFrame) -> DataFrame:
        return _kr.parse_ls_ratio_data(raw_df)

    def aggregate(self, parsed_df: DataFrame) -> DataFrame:
        with_ts = parsed_df.withColumn(
            "event_ts", from_unixtime(col("event_time_sec")).cast("timestamp")
        ).withWatermark("event_ts", self.WATERMARK_DELAY)

        return with_ts.groupBy(window("event_ts", self.WINDOW_DURATION), "symbol", "ratio_type").agg(
            last("ls_ratio").alias("ls_ratio"),
            last("long_ratio").alias("long_ratio"),
            last("short_ratio").alias("short_ratio"),
        )

    def select_output(self, agg_df: DataFrame) -> DataFrame:
        return agg_df.select(
            col("window.start").cast("timestamp").alias("ts"),
            col("symbol"),
            col("ratio_type"),
            col("ls_ratio"),
            col("long_ratio"),
            col("short_ratio"),
        )

    def start(self, spark: SparkSession):
        if not self.ENABLED:
            print(f"[{self.__class__.__name__}] ENABLED=False — 스킵")
            return None

        frames: list[DataFrame] = []
        for topic in _LS_TOPICS:
            raw = _kr.read_from_kafka(spark, topic)
            frames.append(_kr.parse_ls_ratio_data(raw))

        unified = frames[0]
        for f in frames[1:]:
            unified = unified.unionByName(f)

        agg = self.aggregate(unified)
        result = self.select_output(agg)

        query = (
            result.writeStream.outputMode(self.OUTPUT_MODE)
            .foreachBatch(self.process_batch)
            .option("checkpointLocation", self.CHECKPOINT)
            .trigger(processingTime=self.TRIGGER_INTERVAL)
            .start()
        )
        print(f"[{self.__class__.__name__}] 시작 → " f"{_LS_TOPICS} → {self.DATABASE}.{self.TABLE}")
        return query
