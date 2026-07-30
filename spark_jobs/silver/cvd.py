# spark_jobs/silver/cvd.py
"""
Insight 1 + 7: CVD (Cumulative Volume Delta) + Whale 감지.
담당: 백형준

특이사항:
  - 선물(binance-trade) + 현물(spot-trade) 두 토픽을 unionByName 후 집계
  - 단일 토픽 패턴이 아니므로 start() override
"""

import os

import ch_writer as _cw
import kafka_reader as _kr
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, from_unixtime, lit
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when, window
from silver.base import SilverBase

# 심볼별 고래 감지 기준 (단일 aggTrade USD 기준)
# BTC: $500k ≈ 6 BTC / ETH: $300k ≈ 130 ETH / SOL: $100k ≈ 700 SOL
# XRP: $100k ≈ 40,000 XRP @$2.5 / BNB: $200k ≈ 330 BNB @$600
_WHALE_THRESHOLD_BY_SYMBOL = {
    "BTCUSDT": 500_000,
    "ETHUSDT": 300_000,
    "SOLUSDT": 100_000,
    "XRPUSDT": 100_000,
    "BNBUSDT": 200_000,
}
_WHALE_THRESHOLD_DEFAULT = 300_000


class CVDInsight(SilverBase):
    TOPIC = "binance-trade"  # 대표 토픽 (실제로는 spot-trade도 읽음)
    TABLE = "cvd"
    CHECKPOINT = "/opt/spark/work-dir/checkpoints/stream-cvd"
    ENABLED = True
    WATERMARK_DELAY = os.getenv("CVD_WATERMARK_DELAY", "30 seconds")
    TRIGGER_INTERVAL = os.getenv("CVD_TRIGGER_INTERVAL", "5 seconds")
    FUTURES_MAX_OFFSETS = os.getenv("CVD_FUTURES_MAX_OFFSETS", "2000")
    SPOT_MAX_OFFSETS = os.getenv("CVD_SPOT_MAX_OFFSETS", "800")
    USE_TRADE_ROLLUP = os.getenv("CVD_USE_TRADE_ROLLUP", "0") == "1"
    TRADE_ROLLUP_TOPIC = os.getenv("CVD_TRADE_ROLLUP_TOPIC", "rollup-trade-cvd-1s")
    TRADE_ROLLUP_CHECKPOINT = os.getenv(
        "CVD_TRADE_ROLLUP_CHECKPOINT",
        "/opt/spark/work-dir/checkpoints/stream-cvd-trade-rollup",
    )
    TRADE_ROLLUP_MAX_OFFSETS = os.getenv("CVD_TRADE_ROLLUP_MAX_OFFSETS", "5000")

    def parse(self, raw_df: DataFrame) -> DataFrame:
        """선물 aggTrade 파싱."""
        return _kr.parse_trade_data(raw_df).withColumn("source", lit("futures"))

    def aggregate(self, parsed_df: DataFrame) -> DataFrame:
        """union 후 집계 — start()에서 직접 호출."""
        # 심볼별 임계값 컬럼 — when 체인으로 브로드캐스트 없이 처리
        threshold_col = when(col("symbol") == "BTCUSDT", _WHALE_THRESHOLD_BY_SYMBOL["BTCUSDT"])
        for sym, thr in list(_WHALE_THRESHOLD_BY_SYMBOL.items())[1:]:
            threshold_col = threshold_col.when(col("symbol") == sym, thr)
        threshold_col = threshold_col.otherwise(_WHALE_THRESHOLD_DEFAULT)

        enriched = (
            parsed_df.withColumn("trade_value_usd", col("price") * col("quantity"))
            .withColumn(
                "is_whale",
                (col("price") * col("quantity")) >= threshold_col,
            )
            .withColumn(
                "futures_taker_buy_qty",
                when(
                    (col("source") == "futures") & (~col("is_buyer_maker")),
                    col("quantity"),
                ).otherwise(0.0),
            )
            .withColumn(
                "futures_taker_sell_qty",
                when(
                    (col("source") == "futures") & (col("is_buyer_maker")),
                    col("quantity"),
                ).otherwise(0.0),
            )
            .withColumn(
                "spot_taker_buy_qty",
                when(
                    (col("source") == "spot") & (~col("is_buyer_maker")),
                    col("quantity"),
                ).otherwise(0.0),
            )
            .withColumn(
                "spot_taker_sell_qty",
                when((col("source") == "spot") & (col("is_buyer_maker")), col("quantity")).otherwise(0.0),
            )
            .withColumn(
                "whale_buy_qty",
                when(col("is_whale") & (~col("is_buyer_maker")), col("quantity")).otherwise(0.0),
            )
            .withColumn(
                "whale_sell_qty",
                when(col("is_whale") & col("is_buyer_maker"), col("quantity")).otherwise(0.0),
            )
        )
        with_ts = enriched.withColumn("event_ts", from_unixtime(col("event_time_sec")).cast("timestamp")).withWatermark(
            "event_ts", self.WATERMARK_DELAY
        )

        return with_ts.groupBy(window("event_ts", self.WINDOW_DURATION), "symbol").agg(
            spark_sum(col("futures_taker_buy_qty")).alias("futures_taker_buy_vol"),
            spark_sum(col("futures_taker_sell_qty")).alias("futures_taker_sell_vol"),
            count(when(col("source") == "futures", True)).alias("futures_trade_count"),
            spark_sum(col("spot_taker_buy_qty")).alias("spot_taker_buy_vol"),
            spark_sum(col("spot_taker_sell_qty")).alias("spot_taker_sell_vol"),
            count(when(col("source") == "spot", True)).alias("spot_trade_count"),
            count(when(col("is_whale") & (~col("is_buyer_maker")), True)).alias("whale_buy_count"),
            count(when(col("is_whale") & col("is_buyer_maker"), True)).alias("whale_sell_count"),
            spark_sum(col("whale_buy_qty")).alias("whale_buy_vol"),
            spark_sum(col("whale_sell_qty")).alias("whale_sell_vol"),
        )

    def aggregate_rollup(self, parsed_df: DataFrame) -> DataFrame:
        """1초 trade CVD rollup을 기존 5초 CVD 출력 스키마로 합산."""
        with_ts = parsed_df.withColumn(
            "event_ts", from_unixtime(col("event_time_sec")).cast("timestamp")
        ).withWatermark("event_ts", self.WATERMARK_DELAY)

        return with_ts.groupBy(window("event_ts", self.WINDOW_DURATION), "symbol").agg(
            spark_sum(col("futures_taker_buy_qty")).alias("futures_taker_buy_vol"),
            spark_sum(col("futures_taker_sell_qty")).alias("futures_taker_sell_vol"),
            spark_sum(col("futures_trade_count")).alias("futures_trade_count"),
            spark_sum(col("spot_taker_buy_qty")).alias("spot_taker_buy_vol"),
            spark_sum(col("spot_taker_sell_qty")).alias("spot_taker_sell_vol"),
            spark_sum(col("spot_trade_count")).alias("spot_trade_count"),
            spark_sum(col("whale_buy_count")).alias("whale_buy_count"),
            spark_sum(col("whale_sell_count")).alias("whale_sell_count"),
            spark_sum(col("whale_buy_qty")).alias("whale_buy_vol"),
            spark_sum(col("whale_sell_qty")).alias("whale_sell_vol"),
        )

    def select_output(self, agg_df: DataFrame) -> DataFrame:
        return agg_df.select(
            col("window.start").cast("timestamp").alias("ts"),
            col("symbol"),
            col("futures_taker_buy_vol"),
            col("futures_taker_sell_vol"),
            (col("futures_taker_buy_vol") - col("futures_taker_sell_vol")).alias("futures_cvd_delta"),
            col("futures_trade_count").cast("int"),
            col("spot_taker_buy_vol"),
            col("spot_taker_sell_vol"),
            (col("spot_taker_buy_vol") - col("spot_taker_sell_vol")).alias("spot_cvd_delta"),
            col("spot_trade_count").cast("int"),
            col("whale_buy_count").cast("int"),
            col("whale_sell_count").cast("int"),
            col("whale_buy_vol"),
            col("whale_sell_vol"),
        )

    def start(self, spark: SparkSession):
        """
        Custom CVD stream: either consume trade rollups or union futures and spot
        trades. Both modes run the Silver quality gate before aggregation.
        """
        if not self.ENABLED:
            print(f"[{self.__class__.__name__}] ENABLED=False — 스킵")
            return None

        if self.USE_TRADE_ROLLUP:
            rollup_max_offsets = self._get_optimal_max_offsets(
                checkpoint_path=self.TRADE_ROLLUP_CHECKPOINT,
                topic_filter=self.TRADE_ROLLUP_TOPIC,
                fallback=self.TRADE_ROLLUP_MAX_OFFSETS,
            )
            rollup_raw = _kr.read_from_kafka(
                spark,
                self.TRADE_ROLLUP_TOPIC,
                max_offsets_per_trigger=rollup_max_offsets,
            )
            rollup_parsed = _kr.parse_trade_cvd_rollup_data(rollup_raw)
            rollup_quality = self.apply_quality_gate(rollup_parsed, topic=self.TRADE_ROLLUP_TOPIC)
            agg = self.aggregate_rollup(self.prepare_for_aggregate(rollup_quality))
            result = self.select_output(agg)
            query = (
                result.writeStream.outputMode(self.OUTPUT_MODE)
                .foreachBatch(self.process_batch)
                .option("checkpointLocation", self.TRADE_ROLLUP_CHECKPOINT)
                .trigger(processingTime=self.TRIGGER_INTERVAL)
                .start()
            )
            (
                rollup_quality.writeStream.outputMode("append")
                .foreachBatch(
                    lambda df, batch_id: self.process_quality_batch(
                        df,
                        batch_id,
                        topic=self.TRADE_ROLLUP_TOPIC,
                        checkpoint=f"{self.TRADE_ROLLUP_CHECKPOINT}-quality",
                    )
                )
                .option("checkpointLocation", f"{self.TRADE_ROLLUP_CHECKPOINT}-quality")
                .trigger(processingTime=self.TRIGGER_INTERVAL)
                .start()
            )
            print(
                f"[{self.__class__.__name__}] 시작 → topic={self.TRADE_ROLLUP_TOPIC}, "
                f"table={self.TABLE}, trade_mode=rollup, maxOffsetsPerTrigger={rollup_max_offsets}"
            )
            return query

        futures_max_offsets = self._get_optimal_max_offsets(
            checkpoint_path=self.CHECKPOINT,
            topic_filter="binance-trade",
            fallback=self.FUTURES_MAX_OFFSETS,
        )
        spot_max_offsets = self._get_optimal_max_offsets(
            checkpoint_path=self.CHECKPOINT,
            topic_filter="spot-trade",
            fallback=self.SPOT_MAX_OFFSETS,
        )
        futures_raw = _kr.read_from_kafka(
            spark,
            "binance-trade",
            max_offsets_per_trigger=futures_max_offsets,
        )
        futures_quality = self.apply_quality_gate(
            _kr.parse_trade_data(futures_raw).withColumn("source", lit("futures")),
            topic="binance-trade",
        )
        spot_raw = _kr.read_from_kafka(
            spark,
            "spot-trade",
            max_offsets_per_trigger=spot_max_offsets,
        )
        spot_quality = self.apply_quality_gate(
            _kr.parse_spot_trade_data(spot_raw).withColumn("source", lit("spot")),
            topic="spot-trade",
        )
        futures_parsed = self.prepare_for_aggregate(futures_quality)
        spot_parsed = self.prepare_for_aggregate(spot_quality)
        parsed = futures_parsed.unionByName(spot_parsed, allowMissingColumns=True)
        agg = self.aggregate(parsed)
        result = self.select_output(agg)
        query = (
            result.writeStream.outputMode(self.OUTPUT_MODE)
            .foreachBatch(self.process_batch)
            .option("checkpointLocation", self.CHECKPOINT)
            .trigger(processingTime=self.TRIGGER_INTERVAL)
            .start()
        )
        (
            futures_quality.writeStream.outputMode("append")
            .foreachBatch(
                lambda df, batch_id: self.process_quality_batch(
                    df,
                    batch_id,
                    topic="binance-trade",
                    checkpoint=f"{self.CHECKPOINT}-quality-binance-trade",
                )
            )
            .option("checkpointLocation", f"{self.CHECKPOINT}-quality-binance-trade")
            .trigger(processingTime=self.TRIGGER_INTERVAL)
            .start()
        )
        (
            spot_quality.writeStream.outputMode("append")
            .foreachBatch(
                lambda df, batch_id: self.process_quality_batch(
                    df,
                    batch_id,
                    topic="spot-trade",
                    checkpoint=f"{self.CHECKPOINT}-quality-spot-trade",
                )
            )
            .option("checkpointLocation", f"{self.CHECKPOINT}-quality-spot-trade")
            .trigger(processingTime=self.TRIGGER_INTERVAL)
            .start()
        )
        print(
            f"[{self.__class__.__name__}] 시작 → topics=binance-trade+spot-trade, "
            f"table={self.TABLE}, "
            f"maxOffsetsPerTrigger=futures:{futures_max_offsets}/spot:{spot_max_offsets}"
        )
        return query
