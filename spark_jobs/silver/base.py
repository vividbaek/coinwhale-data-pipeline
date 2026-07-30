# spark_jobs/silver/base.py
"""
SilverBase — 모든 Silver 레이어 5초 집계 쿼리의 추상 기반 클래스.

새 Silver 테이블 추가 방법:
  1. spark_jobs/silver/ 에 파일 생성 (예: price.py)
  2. SilverBase 상속 후 TOPIC / TABLE / CHECKPOINT 선언
  3. parse() / aggregate() / select_output() 구현
  4. silver_aggregator.py 는 수정 불필요 — 자동 탐색됨

단일 토픽 → 단일 테이블 패턴.
두 토픽을 union하는 경우(예: CVD)는 start()를 override.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Optional

import ch_writer as _cw

# kafka_reader / ch_writer 는 /opt/spark/work-dir 에 있으므로 flat import
import kafka_reader as _kr
import quality_gate as _dq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_unixtime, window


class SilverBase(ABC):
    # ── 서브클래스에서 반드시 선언 ──────────────────────────────────────────
    TOPIC: str  # Kafka 토픽 이름
    TABLE: str  # ClickHouse 테이블 이름 (DB 제외, 예: "cvd")
    CHECKPOINT: str  # Spark checkpoint 경로

    # ── 공통 설정 (필요 시 서브클래스에서 override) ──────────────────────────
    DATABASE: str = "stream"  # ClickHouse DB (라이브=stream, 백테스팅=hist)
    ENABLED: bool = True  # False 로 설정하면 aggregator가 스킵
    WINDOW_DURATION: str = "5 seconds"  # 집계 윈도우 크기
    WATERMARK_DELAY: str = "30 seconds"  # 늦게 도착한 데이터 허용 범위
    TRIGGER_INTERVAL: str = "5 seconds"  # 배치 트리거 주기 (5초마다 CH INSERT)
    OUTPUT_MODE: str = "update"
    KAFKA_EXPORTER_FALLBACK_URLS: tuple[str, ...] = (
        "http://kafka-exporter:9308/metrics",
        "http://localhost:9308/metrics",
    )

    # ── 구현 필수 메서드 ────────────────────────────────────────────────────

    @abstractmethod
    def parse(self, raw_df: DataFrame) -> DataFrame:
        """Kafka raw bytes → 의미있는 컬럼으로 파싱."""
        ...

    @abstractmethod
    def aggregate(self, parsed_df: DataFrame) -> DataFrame:
        """파싱된 DF → 5초 윈도우 집계."""
        ...

    @abstractmethod
    def select_output(self, agg_df: DataFrame) -> DataFrame:
        """집계 결과 → ClickHouse 스키마와 일치하는 최종 컬럼 선택."""
        ...

    # ── 공통 구현 (필요 시 override) ────────────────────────────────────────

    def _quality_topic(self) -> str:
        return self.TOPIC

    def apply_quality_gate(self, parsed_df: DataFrame, topic: str | None = None) -> DataFrame:
        dq_topic = topic or self._quality_topic()
        if not _dq.is_hot_path_topic(dq_topic):
            return parsed_df
        return _dq.apply_quality_gate(parsed_df, topic=dq_topic)

    def prepare_for_aggregate(self, parsed_df: DataFrame) -> DataFrame:
        if "_dq_is_valid" not in parsed_df.columns:
            return parsed_df
        if _dq.gate_mode() == "observe":
            return _dq.drop_quality_columns(parsed_df)
        return _dq.drop_quality_columns(_dq.valid_rows(parsed_df))

    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        """
        foreachBatch handler. Writes ClickHouse output and, when available,
        records pre-aggregate Spark data-quality summaries.
        """
        _cw.write_to_clickhouse(
            df,
            batch_id,
            f"{self.DATABASE}.{self.TABLE}",
            original_topic=self.TOPIC,
            job_name=self.__class__.__name__,
            checkpoint_path=self.CHECKPOINT,
            query_name=self.__class__.__name__,
        )

    def process_quality_batch(
        self,
        df: DataFrame,
        batch_id: int,
        *,
        topic: str | None = None,
        checkpoint: str | None = None,
    ) -> None:
        dq_topic = topic or self._quality_topic()
        if not _dq.is_hot_path_topic(dq_topic):
            return
        mode = _dq.gate_mode()
        checkpoint_path = checkpoint or self.CHECKPOINT
        try:
            if mode in {"shadow", "strict"}:
                _dq.quarantine_invalid_batch(
                    df,
                    topic=dq_topic,
                    pipeline=self.__class__.__name__,
                    batch_id=batch_id,
                )
            event = _dq.build_quality_event(
                batch_df=df,
                output_df=None,
                pipeline=self.__class__.__name__,
                topic=dq_topic,
                checkpoint=checkpoint_path,
                batch_id=batch_id,
            )
            _dq.write_quality_event(event)
            if mode == "strict":
                max_ratio = _dq.strict_max_invalid_ratio()
                if max_ratio is not None and event["input_rows"]:
                    invalid_ratio = event["dropped_rows"] / event["input_rows"]
                    if invalid_ratio > max_ratio:
                        raise RuntimeError(
                            f"Spark DQ strict failure topic={dq_topic} batch_id={batch_id} "
                            f"invalid_ratio={invalid_ratio:.6f} max={max_ratio:.6f}"
                        )
        except Exception as exc:
            if mode == "strict":
                raise
            _dq.fallback_quality_event(
                {
                    "pipeline": self.__class__.__name__,
                    "stage": "silver_pre_aggregate",
                    "topic": dq_topic,
                    "symbol": None,
                    "checkpoint": checkpoint_path,
                    "batch_id": int(batch_id),
                    "schema_version": None,
                    "input_rows": 0,
                    "parsed_rows": 0,
                    "dropped_rows": 0,
                    "null_critical_count": 0,
                    "parse_error_count": 0,
                    "event_lag_p95_ms": None,
                    "event_lag_p99_ms": None,
                    "write_rows": 0,
                    "error_type": "quality_event_error",
                    "sample_payload": str(exc),
                },
                exc,
            )

    def start(self, spark: SparkSession) -> Optional[object]:
        """Connect a single-topic Silver pipeline to ClickHouse."""
        if not self.ENABLED:
            print(f"[{self.__class__.__name__}] ENABLED=False — 스킵")
            return None

        max_offsets = self._get_optimal_max_offsets()
        group_id = f"silver-{self.TABLE}" if os.getenv("SILVER_USE_KAFKA_GROUP_ID", "0") == "1" else None
        raw = _kr.read_from_kafka(spark, self.TOPIC, max_offsets_per_trigger=max_offsets, group_id=group_id)
        parsed = self.parse(raw)
        quality_input = self.apply_quality_gate(parsed)
        valid_input = self.prepare_for_aggregate(quality_input)
        agg = self.aggregate(valid_input)
        result = self.select_output(agg)
        query = (
            result.writeStream.outputMode(self.OUTPUT_MODE)
            .foreachBatch(self.process_batch)
            .option("checkpointLocation", self.CHECKPOINT)
            .trigger(processingTime=self.TRIGGER_INTERVAL)
            .start()
        )
        if _dq.is_hot_path_topic(self.TOPIC):
            (
                quality_input.writeStream.outputMode("append")
                .foreachBatch(self.process_quality_batch)
                .option("checkpointLocation", f"{self.CHECKPOINT}-quality")
                .trigger(processingTime=self.TRIGGER_INTERVAL)
                .start()
            )
        print(
            f"[{self.__class__.__name__}] 시작 → topic={self.TOPIC}, table={self.TABLE}, group_id={group_id or '-'}, maxOffsetsPerTrigger={max_offsets}"
        )
        return query

    def _get_optimal_max_offsets(
        self,
        checkpoint_path: Optional[str] = None,
        topic_filter: Optional[str] = None,
        fallback: str = "1000",
    ) -> str:
        """
        Checkpoint와 Kafka offset을 비교하여 최적의 maxOffsetsPerTrigger 반환.
        Lag가 크면 더 큰 값을 사용하여 빠르게 catch-up.

        Args:
            checkpoint_path: 사용할 checkpoint 경로 (None이면 self.CHECKPOINT).
                start()를 override하여 토픽-체크포인트가 분리된 경우(price 등)
                토픽별 체크포인트를 직접 지정한다.
            topic_filter: lag 계산 대상 토픽 (None이면 체크포인트의 모든 토픽).
                CVD처럼 한 체크포인트에 토픽 2개가 union된 경우 토픽별로 호출한다.
            fallback: lag 정보를 못 얻거나 lag가 작을 때 반환값. 환경변수로 설정한
                기존 max_offsets를 그대로 쓰고 싶을 때 fallback에 넘긴다.
        """
        try:
            import json
            import re
            from pathlib import Path
            from urllib.request import urlopen

            checkpoint_dir = Path(checkpoint_path or self.CHECKPOINT)
            offsets_dir = checkpoint_dir / "offsets"

            # Checkpoint가 없으면 fallback (첫 실행 등)
            if not offsets_dir.exists():
                return fallback

            offset_files = sorted(
                [f for f in offsets_dir.iterdir() if f.is_file() and f.name.isdigit()],
                key=lambda p: int(p.name),
            )
            if not offset_files:
                return fallback

            latest_offset_file = offset_files[-1]
            try:
                with open(latest_offset_file, "r") as f:
                    lines = [line.strip() for line in f if line.strip()]
                    if len(lines) < 3:
                        return fallback
                    offsets: list[tuple[str, str, int]] = []
                    for line in lines[2:]:
                        payload = json.loads(line)
                        for topic, partitions in payload.items():
                            if topic_filter and topic != topic_filter:
                                continue
                            for partition, offset in partitions.items():
                                offsets.append((topic, str(partition), int(offset)))
            except Exception:
                return fallback

            if not offsets:
                # topic_filter가 체크포인트와 안 맞는 케이스
                return fallback

            # Spark driver는 보통 spark-master 컨테이너 안에서 실행되므로
            # kafka-exporter service name을 우선 사용하고, 로컬 실행 시 localhost로 fallback한다.
            try:
                metrics_url_raw = (
                    os.getenv("KAFKA_EXPORTER_METRICS_URL")
                    or os.getenv("KAFKA_EXPORTER_URL")
                    or ",".join(self.KAFKA_EXPORTER_FALLBACK_URLS)
                )
                metrics_urls = [url.strip() for url in metrics_url_raw.split(",") if url.strip()]
                metrics_text = ""
                for metrics_url in metrics_urls:
                    try:
                        metrics_text = urlopen(metrics_url, timeout=2).read().decode("utf-8", errors="replace")
                        if metrics_text:
                            break
                    except Exception:
                        continue
                if not metrics_text:
                    return fallback
                kafka_offsets: dict[tuple[str, str], int] = {}
                for line in metrics_text.splitlines():
                    if not line.startswith("kafka_topic_partition_current_offset{"):
                        continue
                    topic_match = re.search(r'topic="([^"]+)"', line)
                    partition_match = re.search(r'partition="([^"]+)"', line)
                    value_match = re.search(r"} ([0-9.eE+-]+)$", line)
                    if topic_match and partition_match and value_match:
                        kafka_offsets[(topic_match.group(1), partition_match.group(1))] = int(
                            float(value_match.group(1))
                        )
            except Exception:
                return fallback

            total_lag = 0
            for topic, partition, offset in offsets:
                log_end = kafka_offsets.get((topic, partition))
                if log_end is None:
                    continue
                total_lag += max(0, log_end - offset)

            return self._lag_to_max_offsets(total_lag, fallback)
        except Exception:
            return fallback

    @staticmethod
    def _lag_to_max_offsets(total_lag: int, fallback: str) -> str:
        """Lag 임계치 → maxOffsetsPerTrigger 매핑.

        burst가 짧게 끝나는 시그널 데이터 특성상, 평상시는 fallback(고정값)을
        유지해 트리거 시간을 안정적으로 5초로 두고, lag가 누적되면 단계적으로
        키워 catch-up한다.
        """
        if total_lag > 10_000_000:
            return "50000"
        if total_lag > 1_000_000:
            return "10000"
        if total_lag > 100_000:
            return "5000"
        return fallback
