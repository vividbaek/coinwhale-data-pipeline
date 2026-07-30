# spark_jobs/silver/oi.py
"""
Insight 3: 미결제약정 (Open Interest).
담당: 팀원 C

소스 토픽: binance-openinterest (REST 5초 polling)
저장 테이블: boaz.insight_oi
집계 방식: LAST (5초마다 수신되는 스냅샷)

전처리 포인트:
  - oi_change_pct: (현재 OI - 이전 OI) / 이전 OI * 100
    Spark Structured Streaming은 lag()를 직접 지원하지 않으므로
    foreachBatch 내 클로저 state(_prev_oi dict)로 이전 값 추적.

해석 가이드:
  OI↑ + 가격↑ = 신규 롱 포지션 유입 (강세 지속 가능)
  OI↑ + 가격↓ = 신규 숏 포지션 유입 (약세 지속 가능)
  OI↓ + 가격↑ = 숏 청산 (short squeeze)
  OI↓ + 가격↓ = 롱 청산 (패닉 청산)
"""

from __future__ import annotations

import os
import time
from typing import Any

import ch_writer as _cw
import kafka_reader as _kr
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, get_json_object, last, window
from silver.base import SilverBase


class OIInsight(SilverBase):
    TOPIC = "binance-openinterest"
    TABLE = "oi"
    CHECKPOINT = "/opt/spark/work-dir/checkpoints/stream-oi"
    ENABLED = True
    VERBOSE_BATCH_LOG = os.getenv("OI_VERBOSE_BATCH_LOG", "0") == "1"

    def __init__(self) -> None:
        # foreachBatch 배치 간 이전 OI 값 추적 (symbol → float)
        self._prev_oi: dict[str, float] = {}

    def parse(self, raw_df: DataFrame) -> DataFrame:
        """
        OI 메시지 구조:
          {"symbol": "BTCUSDT", "stream": "openInterest",
           "data": {"openInterest": "123456.789", "symbol": "BTCUSDT", "time": 1771857342000}}
        """
        v = col("value").cast("string")
        return raw_df.select(
            get_json_object(v, "$.symbol").alias("symbol"),
            get_json_object(v, "$.data.openInterest").cast("double").alias("open_interest"),
            (get_json_object(v, "$.data.time").cast("long") / 1000).alias("event_time_sec"),
        )

    def aggregate(self, parsed_df: DataFrame) -> DataFrame:
        with_ts = parsed_df.withColumn(
            "event_ts", from_unixtime(col("event_time_sec")).cast("timestamp")
        ).withWatermark("event_ts", self.WATERMARK_DELAY)

        # OI는 5초 polling 스냅샷이므로 LAST = 최신 스냅샷 그대로
        return with_ts.groupBy(window("event_ts", self.WINDOW_DURATION), "symbol").agg(
            last("open_interest").alias("open_interest"),
        )

    def select_output(self, agg_df: DataFrame) -> DataFrame:
        # oi_change_pct는 process_batch에서 pandas로 계산 (lag 대체)
        return agg_df.select(
            col("window.start").cast("timestamp").alias("ts"),
            col("symbol"),
            col("open_interest"),
        )

    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        """
        Base의 process_batch를 override.
        oi_change_pct를 foreachBatch 내 state(_prev_oi)로 계산 후 CH 저장.
        """
        started_at = time.monotonic()
        output_table = f"{self.DATABASE}.{self.TABLE}"
        # Spark DF → pandas (ts string 변환으로 pandas 2.x 호환)
        df_str = df.withColumn("ts", col("ts").cast("string"))
        pdf: pd.DataFrame = df_str.toPandas()
        if pdf.empty:
            _cw.log_write_audit_event(
                job_name=self.__class__.__name__,
                batch_id=batch_id,
                output_table=output_table,
                checkpoint_path=self.CHECKPOINT,
                query_name=self.__class__.__name__,
                pdf=pdf,
                status="skipped_empty",
                duration_ms=int((time.monotonic() - started_at) * 1000),
            )
            return
        pdf["ts"] = pd.to_datetime(pdf["ts"])

        # oi_change_pct 계산 (symbol별 이전 값 추적)
        def _calc_change_pct(row: Any) -> float:
            symbol: str = row["symbol"]
            curr: float = row["open_interest"]
            prev: float | None = self._prev_oi.get(symbol)
            if prev is None or prev == 0.0:
                pct = 0.0
            else:
                pct = (curr - prev) / prev * 100
            self._prev_oi[symbol] = curr
            return pct

        pdf["oi_change_pct"] = pdf.apply(_calc_change_pct, axis=1)

        if self.VERBOSE_BATCH_LOG:
            print(f"\n{'='*70}")
            print(f"  [OIInsight] Batch {batch_id}  |  rows: {len(pdf)}")
            print(f"{'='*70}")
            print(pdf[["ts", "symbol", "open_interest", "oi_change_pct"]].to_string(index=False))
            print(f"{'='*70}\n")

        _cw.log_write_audit_event(
            job_name=self.__class__.__name__,
            batch_id=batch_id,
            output_table=output_table,
            checkpoint_path=self.CHECKPOINT,
            query_name=self.__class__.__name__,
            pdf=pdf,
            status="started",
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )
        try:
            client = _cw.get_client()
            client.insert_df(output_table, pdf)
        except Exception as exc:
            error_type = _cw._classify_ch_error(exc)
            _cw.log_write_audit_event(
                job_name=self.__class__.__name__,
                batch_id=batch_id,
                output_table=output_table,
                checkpoint_path=self.CHECKPOINT,
                query_name=self.__class__.__name__,
                pdf=pdf,
                status="failed",
                duration_ms=int((time.monotonic() - started_at) * 1000),
                error_type=error_type,
                error=exc,
            )
            raise
        _cw.log_write_audit_event(
            job_name=self.__class__.__name__,
            batch_id=batch_id,
            output_table=output_table,
            checkpoint_path=self.CHECKPOINT,
            query_name=self.__class__.__name__,
            pdf=pdf,
            status="success",
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )
        if self.VERBOSE_BATCH_LOG:
            print(f"[CH] batch {batch_id} -> {output_table}: {len(pdf)} rows")
