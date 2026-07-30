# spark_jobs/silver/price.py
"""
Insight: 호가 스프레드 + Basis + markPrice.

소스 토픽:
  - binance-markprice  : 선물 mark_price, funding_rate, index_price (1초 간격)
  - rollup-bookticker-1s 또는 raw binance/spot bookticker: 선물/현물 bid/ask

집계 방식:
  - markprice 5초 LAST → 메인 집계 (clock 역할)
  - bookticker는 rollup topic 또는 raw 두 토픽을 foreachBatch로 클래스 state dict 업데이트
  - markprice process_batch에서 state dict merge → stream.price INSERT

계산:
  futures_spread = futures_ask - futures_bid
  spot_spread    = spot_ask - spot_bid
  basis_pct      = (futures_mid - spot_mid) / spot_mid * 100

설계 근거:
  Spark stream-stream join은 watermark 양쪽 필수 + 지연/복잡도 높음.
  bookticker 는 드라이버 state cache를 쓰되, merge 시 freshness를 검사한다.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

import ch_writer as _cw
import kafka_reader as _kr
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_unixtime, last, window
from silver.base import SilverBase


@dataclass(frozen=True)
class BookTickerSnapshot:
    bid: float
    ask: float
    event_time_sec: float


class PriceInsight(SilverBase):
    TOPIC = "binance-markprice"
    TABLE = "price"
    CHECKPOINT = "/opt/spark/work-dir/checkpoints/stream-price"
    ENABLED = True
    BOOKTICKER_MAX_OFFSETS = os.getenv("PRICE_BT_MAX_OFFSETS", "5000")
    BOOKTICKER_TRIGGER_INTERVAL = os.getenv("PRICE_BT_TRIGGER_INTERVAL", "5 seconds")
    BOOKTICKER_STALE_AFTER_SEC = float(os.getenv("PRICE_BT_STALE_AFTER_SEC", "15"))
    VERBOSE_BATCH_LOG = os.getenv("PRICE_VERBOSE_BATCH_LOG", "0") == "1"
    USE_BOOKTICKER_ROLLUP = os.getenv("PRICE_USE_BOOKTICKER_ROLLUP", "0") == "1"
    BOOKTICKER_ROLLUP_TOPIC = os.getenv("PRICE_BOOKTICKER_ROLLUP_TOPIC", "rollup-bookticker-1s")
    BOOKTICKER_ROLLUP_CHECKPOINT = os.getenv(
        "PRICE_BOOKTICKER_ROLLUP_CHECKPOINT",
        "/opt/spark/work-dir/checkpoints/stream-price-bt-rollup",
    )
    BOOKTICKER_ROLLUP_MAX_OFFSETS = os.getenv("PRICE_BOOKTICKER_ROLLUP_MAX_OFFSETS", "5000")
    BOOKTICKER_ROLLUP_TRIGGER_INTERVAL = os.getenv(
        "PRICE_BOOKTICKER_ROLLUP_TRIGGER_INTERVAL",
        BOOKTICKER_TRIGGER_INTERVAL,
    )

    def __init__(self) -> None:
        # bookticker 최신값 캐시 (symbol → bid/ask/event_time)
        self._futures_book: dict[str, BookTickerSnapshot] = {}
        self._spot_book: dict[str, BookTickerSnapshot] = {}
        self._last_stale_log_monotonic: float = 0.0
        self._aux_queries: list[object] = []

    # ── 파싱 / 집계 / 출력 선택 (base.start()에서 markprice 단일 파이프라인용) ──

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
        )

    def select_output(self, agg_df: DataFrame) -> DataFrame:
        return agg_df.select(
            col("window.start").cast("timestamp").alias("ts"),
            col("symbol"),
            col("mark_price"),
            col("funding_rate"),
            col("index_price"),
        )

    # ── bookticker state 업데이트 핸들러 ──────────────────────────────────────

    def _update_futures_bt(self, df: DataFrame, batch_id: int) -> None:
        """binance-bookticker foreachBatch → 선물 bid/ask 상태 갱신.

        Spark 내에서 symbol별 last() 집계 후 드라이버로 전송해
        드라이버 전송량을 심볼 수(수십 개)로 최소화한다.
        """
        from pyspark.sql.functions import last as _last

        latest = df.groupBy("symbol").agg(
            _last("bid_price", ignorenulls=True).alias("bid_price"),
            _last("ask_price", ignorenulls=True).alias("ask_price"),
            _last("event_time_sec", ignorenulls=True).alias("event_time_sec"),
        )
        rows = latest.collect()
        if not rows:
            return
        for row in rows:
            bid_price = row["bid_price"]
            ask_price = row["ask_price"]
            event_time_sec = row["event_time_sec"]
            if bid_price is None or ask_price is None or event_time_sec is None:
                continue
            self._futures_book[row["symbol"]] = BookTickerSnapshot(
                bid=float(bid_price),
                ask=float(ask_price),
                event_time_sec=float(event_time_sec),
            )

    def _update_spot_bt(self, df: DataFrame, batch_id: int) -> None:
        """spot-bookticker foreachBatch → 현물 bid/ask 상태 갱신."""
        from pyspark.sql.functions import last as _last

        latest = df.groupBy("symbol").agg(
            _last("bid_price", ignorenulls=True).alias("bid_price"),
            _last("ask_price", ignorenulls=True).alias("ask_price"),
            _last("event_time_sec", ignorenulls=True).alias("event_time_sec"),
        )
        rows = latest.collect()
        if not rows:
            return
        for row in rows:
            bid_price = row["bid_price"]
            ask_price = row["ask_price"]
            event_time_sec = row["event_time_sec"]
            if bid_price is None or ask_price is None or event_time_sec is None:
                continue
            self._spot_book[row["symbol"]] = BookTickerSnapshot(
                bid=float(bid_price),
                ask=float(ask_price),
                event_time_sec=float(event_time_sec),
            )

    def _update_rollup_bt(self, df: DataFrame, batch_id: int) -> None:
        """rollup-bookticker-1s foreachBatch -> futures/spot bid/ask state."""
        rows = (
            df.select("market", "symbol", "bid_price", "ask_price", "event_time_sec")
            .orderBy("event_time_sec")
            .collect()
        )
        if not rows:
            return

        futures_updates = 0
        spot_updates = 0
        for row in rows:
            market = row["market"]
            symbol = row["symbol"]
            bid_price = row["bid_price"]
            ask_price = row["ask_price"]
            event_time_sec = row["event_time_sec"]
            if market is None or symbol is None or bid_price is None or ask_price is None or event_time_sec is None:
                continue

            snapshot = BookTickerSnapshot(
                bid=float(bid_price),
                ask=float(ask_price),
                event_time_sec=float(event_time_sec),
            )
            if market == "spot":
                self._spot_book[symbol] = snapshot
                spot_updates += 1
            else:
                self._futures_book[symbol] = snapshot
                futures_updates += 1

        if self.VERBOSE_BATCH_LOG:
            print(
                f"[PriceInsight] rollup bookticker batch={batch_id} "
                f"rows={len(rows)} futures_updates={futures_updates} spot_updates={spot_updates}"
            )

    def _apply_bookticker_snapshot(
        self,
        pdf: pd.DataFrame,
        snapshots: dict[str, BookTickerSnapshot],
        prefix: str,
        batch_processed_at_sec: float,
    ) -> pd.Series:
        """캐시된 bookticker snapshot을 주입하고 stale row를 걸러낸다."""
        snapshot_series = pdf["symbol"].map(snapshots)
        event_time_col = f"{prefix}_book_event_time_sec"
        pdf[f"{prefix}_bid"] = snapshot_series.map(
            lambda snap: snap.bid if isinstance(snap, BookTickerSnapshot) else 0.0
        ).astype("float64")
        pdf[f"{prefix}_ask"] = snapshot_series.map(
            lambda snap: snap.ask if isinstance(snap, BookTickerSnapshot) else 0.0
        ).astype("float64")
        pdf[event_time_col] = snapshot_series.map(
            lambda snap: (snap.event_time_sec if isinstance(snap, BookTickerSnapshot) else 0.0)
        ).astype("float64")

        fresh_mask = (pdf[event_time_col] > 0.0) & (
            (batch_processed_at_sec - pdf[event_time_col]) <= self.BOOKTICKER_STALE_AFTER_SEC
        )
        stale_mask = ~fresh_mask
        if stale_mask.any():
            pdf.loc[stale_mask, [f"{prefix}_bid", f"{prefix}_ask"]] = 0.0
        return fresh_mask

    def _log_stale_bookticker_once(
        self,
        batch_id: int,
        futures_stale_rows: int,
        spot_stale_rows: int,
    ) -> None:
        """stale bookticker가 보일 때 로그를 제한적으로 남긴다."""
        if futures_stale_rows == 0 and spot_stale_rows == 0:
            return

        now_monotonic = time.monotonic()
        if now_monotonic - self._last_stale_log_monotonic < 30:
            return

        print(
            "[PriceInsight] stale bookticker 감지 "
            f"(batch={batch_id}, futures_rows={futures_stale_rows}, "
            f"spot_rows={spot_stale_rows}, threshold={self.BOOKTICKER_STALE_AFTER_SEC}s)"
        )
        self._last_stale_log_monotonic = now_monotonic

    # ── 메인 process_batch: state merge + CH INSERT ───────────────────────────

    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        started_at = time.monotonic()
        output_table = f"{self.DATABASE}.{self.TABLE}"
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
        batch_processed_at_sec = time.time()

        futures_fresh = self._apply_bookticker_snapshot(
            pdf,
            self._futures_book,
            prefix="futures",
            batch_processed_at_sec=batch_processed_at_sec,
        )
        spot_fresh = self._apply_bookticker_snapshot(
            pdf,
            self._spot_book,
            prefix="spot",
            batch_processed_at_sec=batch_processed_at_sec,
        )
        self._log_stale_bookticker_once(
            batch_id=batch_id,
            futures_stale_rows=int((~futures_fresh).sum()),
            spot_stale_rows=int((~spot_fresh).sum()),
        )

        # 파생 컬럼 계산
        pdf["futures_spread"] = (pdf["futures_ask"] - pdf["futures_bid"]).where(futures_fresh, 0.0)
        pdf["spot_spread"] = (pdf["spot_ask"] - pdf["spot_bid"]).where(spot_fresh, 0.0)
        futures_mid = (pdf["futures_bid"] + pdf["futures_ask"]) / 2
        spot_mid = (pdf["spot_bid"] + pdf["spot_ask"]) / 2
        valid_basis_mask = futures_fresh & spot_fresh & (spot_mid != 0)
        pdf["basis_pct"] = (
            ((futures_mid - spot_mid) / spot_mid.where(valid_basis_mask, other=float("nan")) * 100).where(
                valid_basis_mask, 0.0
            )
        ).fillna(0.0)

        # ClickHouse 컬럼 순서 맞추기
        out = pdf[
            [
                "ts",
                "symbol",
                "futures_bid",
                "futures_ask",
                "futures_spread",
                "spot_bid",
                "spot_ask",
                "spot_spread",
                "basis_pct",
                "mark_price",
                "funding_rate",
                "index_price",
            ]
        ]

        if self.VERBOSE_BATCH_LOG:
            print(f"\n{'='*70}")
            print(f"  [PriceInsight] Batch {batch_id}  |  rows: {len(out)}")
            print(f"{'='*70}")
            print(out.to_string(index=False))
            print(f"{'='*70}\n")

        _cw.log_write_audit_event(
            job_name=self.__class__.__name__,
            batch_id=batch_id,
            output_table=output_table,
            checkpoint_path=self.CHECKPOINT,
            query_name=self.__class__.__name__,
            pdf=out,
            status="started",
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )
        try:
            client = _cw.get_client()
            client.insert_df(output_table, out)
        except Exception as exc:
            error_type = _cw._classify_ch_error(exc)
            _cw.log_write_audit_event(
                job_name=self.__class__.__name__,
                batch_id=batch_id,
                output_table=output_table,
                checkpoint_path=self.CHECKPOINT,
                query_name=self.__class__.__name__,
                pdf=out,
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
            pdf=out,
            status="success",
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )
        if self.VERBOSE_BATCH_LOG:
            print(f"[CH] batch {batch_id} -> {output_table}: {len(out)} rows")

    # ── start() override: 3개 토픽 쿼리 동시 실행 ────────────────────────────

    def start(self, spark: SparkSession):
        """markprice + bookticker state 쿼리 동시 실행.

        raw bookticker는 체크포인트가 토픽별로 분리(stream-price-fbt/sbt)되어 있고,
        rollup bookticker는 stream-price-bt-rollup 체크포인트를 사용한다.
        각 체크포인트 단위로 lag-aware maxOffsetsPerTrigger를 산출한다.
        markprice 메인 쿼리는 base의 1000 fallback을 그대로 쓴다(고빈도 1초 소스).
        """
        if not self.ENABLED:
            print(f"[{self.__class__.__name__}] ENABLED=False — 스킵")
            return None

        markprice_max_offsets = self._get_optimal_max_offsets(
            checkpoint_path=self.CHECKPOINT,
            fallback="1000",
        )

        bookticker_mode = "raw"
        bookticker_offsets_desc = ""
        self._aux_queries = []
        if self.USE_BOOKTICKER_ROLLUP:
            rollup_max_offsets = self._get_optimal_max_offsets(
                checkpoint_path=self.BOOKTICKER_ROLLUP_CHECKPOINT,
                fallback=self.BOOKTICKER_ROLLUP_MAX_OFFSETS,
            )
            rollup_raw = _kr.read_from_kafka(
                spark,
                self.BOOKTICKER_ROLLUP_TOPIC,
                max_offsets_per_trigger=rollup_max_offsets,
            )
            rollup_parsed = _kr.parse_bookticker_rollup_data(rollup_raw)
            rollup_quality = self.apply_quality_gate(rollup_parsed, topic=self.BOOKTICKER_ROLLUP_TOPIC)
            rollup_valid = self.prepare_for_aggregate(rollup_quality)
            q_rollup_bt = (
                rollup_valid.writeStream.outputMode("append")
                .foreachBatch(self._update_rollup_bt)
                .option("checkpointLocation", self.BOOKTICKER_ROLLUP_CHECKPOINT)
                .trigger(processingTime=self.BOOKTICKER_ROLLUP_TRIGGER_INTERVAL)
                .start()
            )
            self._aux_queries.append(q_rollup_bt)
            bookticker_mode = f"rollup:{self.BOOKTICKER_ROLLUP_TOPIC}"
            bookticker_offsets_desc = f"rollup:{rollup_max_offsets}"
        else:
            fbt_checkpoint = "/opt/spark/work-dir/checkpoints/stream-price-fbt"
            sbt_checkpoint = "/opt/spark/work-dir/checkpoints/stream-price-sbt"

            fbt_max_offsets = self._get_optimal_max_offsets(
                checkpoint_path=fbt_checkpoint,
                fallback=self.BOOKTICKER_MAX_OFFSETS,
            )
            sbt_max_offsets = self._get_optimal_max_offsets(
                checkpoint_path=sbt_checkpoint,
                fallback=self.BOOKTICKER_MAX_OFFSETS,
            )

            # bookticker 상태 유지 쿼리 (체크포인트 분리, 5초 트리거)
            futures_bt_raw = _kr.read_from_kafka(
                spark,
                "binance-bookticker",
                max_offsets_per_trigger=fbt_max_offsets,
            )
            futures_bt_parsed = _kr.parse_bookticker_data(futures_bt_raw)
            futures_bt_quality = self.apply_quality_gate(futures_bt_parsed, topic="binance-bookticker")
            futures_bt_valid = self.prepare_for_aggregate(futures_bt_quality)
            q_futures_bt = (
                futures_bt_valid.writeStream.outputMode("append")
                .foreachBatch(self._update_futures_bt)
                .option("checkpointLocation", fbt_checkpoint)
                .trigger(processingTime=self.BOOKTICKER_TRIGGER_INTERVAL)
                .start()
            )

            spot_bt_raw = _kr.read_from_kafka(
                spark,
                "spot-bookticker",
                max_offsets_per_trigger=sbt_max_offsets,
            )
            spot_bt_parsed = _kr.parse_bookticker_data(spot_bt_raw)
            spot_bt_quality = self.apply_quality_gate(spot_bt_parsed, topic="spot-bookticker")
            spot_bt_valid = self.prepare_for_aggregate(spot_bt_quality)
            q_spot_bt = (
                spot_bt_valid.writeStream.outputMode("append")
                .foreachBatch(self._update_spot_bt)
                .option("checkpointLocation", sbt_checkpoint)
                .trigger(processingTime=self.BOOKTICKER_TRIGGER_INTERVAL)
                .start()
            )
            self._aux_queries.extend([q_futures_bt, q_spot_bt])
            bookticker_offsets_desc = f"fbt:{fbt_max_offsets}/sbt:{sbt_max_offsets}"

        # 메인 markprice 5초 집계 쿼리
        mark_raw = _kr.read_from_kafka(
            spark,
            self.TOPIC,
            max_offsets_per_trigger=markprice_max_offsets,
        )
        parsed = self.parse(mark_raw)
        quality_input = self.apply_quality_gate(parsed, topic=self.TOPIC)
        valid_input = self.prepare_for_aggregate(quality_input)
        agg = self.aggregate(valid_input)
        result = self.select_output(agg)
        q_main = (
            result.writeStream.outputMode(self.OUTPUT_MODE)
            .foreachBatch(self.process_batch)
            .option("checkpointLocation", self.CHECKPOINT)
            .trigger(processingTime=self.TRIGGER_INTERVAL)
            .start()
        )
        (
            quality_input.writeStream.outputMode("append")
            .foreachBatch(
                lambda df, batch_id: self.process_quality_batch(
                    df,
                    batch_id,
                    topic=self.TOPIC,
                    checkpoint=f"{self.CHECKPOINT}-quality",
                )
            )
            .option("checkpointLocation", f"{self.CHECKPOINT}-quality")
            .trigger(processingTime=self.TRIGGER_INTERVAL)
            .start()
        )

        print(
            f"[PriceInsight] 시작 → "
            f"binance-markprice + {bookticker_mode} "
            f"→ {self.DATABASE}.{self.TABLE}, "
            f"maxOffsetsPerTrigger=mark:{markprice_max_offsets}/{bookticker_offsets_desc}"
        )
        return q_main
