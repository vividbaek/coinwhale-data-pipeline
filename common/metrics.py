"""
Prometheus 메트릭 정의 모듈.
collectors/run_all.py 시작 시 start_metrics_server()를 한 번 호출하고,
각 프로세스가 Gauge/Counter를 갱신한다.

── 메트릭 목록 ────────────────────────────────────────────────────────
[Gauge]
  collector_tps_msgs_per_sec          - 수집기별 WebSocket 수신 메시지 처리량
  collector_last_message_timestamp    - 수집기별 마지막 메시지 수신 시각 (staleness 감지)
  bookticker_rollup_pending_buckets   - bookticker rollup 대기 bucket 수
  trade_cvd_rollup_pending_buckets    - trade CVD rollup 대기 bucket 수
  rollup_processor_input_event_lag_seconds
                                      - rollup processor가 본 최신 event-time의 지연
  rollup_processor_oldest_pending_bucket_age_seconds
                                      - 가장 오래된 미 flush bucket의 wall-clock age
  rollup_processor_last_poll_records  - 마지막 Kafka poll record 수
  rollup_processor_last_poll_duration_seconds
                                      - 마지막 Kafka poll 소요 시간
  rollup_processor_last_flush_records - 마지막 flush output record 수
  rollup_processor_last_flush_duration_seconds
                                      - 마지막 flush+producer.flush 소요 시간
  rollup_processor_last_commit_duration_seconds
                                      - 마지막 consumer commit 소요 시간
  rollup_processor_output_event_lag_seconds
                                      - output record event-time 기준 지연
  paper_price_rows_total              - paper trader가 최근 관측한 view row 수
  paper_price_fallback_rows           - last_price<=0, mark_price>0 row 수
  paper_price_fallback_ratio          - 유효 가격 fallback 사용 비율
  paper_price_anomaly_active          - price source anomaly 활성 여부 (0/1)
  trading_pipeline_stage_total        - 자동매매 funnel 단계별 누적 이벤트 수
  trading_decision_events_total       - orchestrator decision event 누적 수
  trading_decision_proposals_total    - 최종 resolved trading proposal 누적 수
  metrics_last_push_timestamp         - Pushgateway 마지막 전송 시각
  metrics_push_duration_seconds       - Pushgateway 마지막 전송 소요 시간

[Counter]
  metrics_push_success_total          - Pushgateway 전송 성공 누적 수
  metrics_push_failure_total          - Pushgateway 전송 실패 누적 수
  kafka_messages_produced_total       - 수집기·토픽별 Kafka 전송 누적 수
  collector_errors_total              - 수집기·에러 유형별 에러 누적 수
  rollup_processor_flush_batches_total
                                      - rollup processor flush batch 누적 수
  rollup_processor_flush_records_total
                                      - rollup processor flush output record 누적 수
  rollup_processor_commit_errors_total
                                      - rollup processor commit 실패 누적 수
  websocket_reconnects_total          - 수집기별 WebSocket 재연결 누적 횟수
  news_articles_indexed_total         - ClickHouse + Qdrant 적재 뉴스 기사 누적 수 (소스별)
  ai_enrichment_success_total         - 뉴스 AI enrichment 성공 누적 수
  ai_enrichment_auth_failure_total    - 뉴스 AI provider 인증 실패 누적 수
  ai_enrichment_fallback_total        - 뉴스 local fallback 적용 누적 수
  translation_complete_total          - 한국어 번역/요약 완료 누적 수
  paper_price_anomaly_events_total    - paper trader anomaly 진입/회복 이벤트 수
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable

try:
    from prometheus_client import (
        REGISTRY,
        Counter,
        Gauge,
        push_to_gateway,
        start_http_server,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - import guard for lean test environments
    _PROMETHEUS_IMPORT_ERROR = exc

    class _NullMetricValue:
        def __init__(self) -> None:
            self.value = 0.0

        def get(self) -> float:
            return self.value

        def set(self, value: float) -> None:
            self.value = float(value)

        def inc(self, amount: float = 1.0) -> None:
            self.value += float(amount)

    class _NullMetric:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self._value = _NullMetricValue()

        def labels(self, *_args: object, **_kwargs: object) -> "_NullMetric":
            return self

        def set(self, value: float, *_args: object, **_kwargs: object) -> None:
            self._value.set(value)

        def inc(self, amount: float = 1.0, *_args: object, **_kwargs: object) -> None:
            self._value.inc(amount)

        def clear(self) -> None:
            return None

    class _NullRegistry:
        pass

    REGISTRY = _NullRegistry()

    def Gauge(*_args: object, **_kwargs: object) -> _NullMetric:
        return _NullMetric()

    def Counter(*_args: object, **_kwargs: object) -> _NullMetric:
        return _NullMetric()

    def push_to_gateway(*_args: object, **_kwargs: object) -> None:
        return None

    def start_http_server(*_args: object, **_kwargs: object) -> None:
        return None

else:
    _PROMETHEUS_IMPORT_ERROR = None

logger = logging.getLogger(__name__)
_push_thread_started = False


metrics_push_success_total = Counter(
    "metrics_push_success_total",
    "Pushgateway push success count by job.",
    ["job"],
)

metrics_push_failure_total = Counter(
    "metrics_push_failure_total",
    "Pushgateway push failure count by job and coarse error type.",
    ["job", "error_type"],
)

metrics_last_push_timestamp = Gauge(
    "metrics_last_push_timestamp",
    "Unix timestamp of the last Pushgateway push attempt by status.",
    ["job", "status"],
)

metrics_push_duration_seconds = Gauge(
    "metrics_push_duration_seconds",
    "Duration of the last Pushgateway push attempt by status.",
    ["job", "status"],
)


def _push_health_metrics_for_registry(
    registry: object,
) -> tuple[object, object, object, object]:
    if registry is REGISTRY:
        return (
            metrics_push_success_total,
            metrics_push_failure_total,
            metrics_last_push_timestamp,
            metrics_push_duration_seconds,
        )

    success_total = Counter(
        "metrics_push_success_total",
        "Pushgateway push success count by job.",
        ["job"],
        registry=registry,
    )
    failure_total = Counter(
        "metrics_push_failure_total",
        "Pushgateway push failure count by job and coarse error type.",
        ["job", "error_type"],
        registry=registry,
    )
    last_push_timestamp = Gauge(
        "metrics_last_push_timestamp",
        "Unix timestamp of the last Pushgateway push attempt by status.",
        ["job", "status"],
        registry=registry,
    )
    push_duration_seconds = Gauge(
        "metrics_push_duration_seconds",
        "Duration of the last Pushgateway push attempt by status.",
        ["job", "status"],
        registry=registry,
    )
    return success_total, failure_total, last_push_timestamp, push_duration_seconds


def _record_push_health(
    *,
    registry: object,
    job_name: str,
    status: str,
    duration: float,
    error_type: str | None = None,
) -> None:
    success_total, failure_total, last_push_timestamp, push_duration_seconds = _push_health_metrics_for_registry(
        registry
    )
    if status == "success":
        success_total.labels(job=job_name).inc()
    else:
        failure_total.labels(job=job_name, error_type=error_type or "unknown").inc()
    last_push_timestamp.labels(job=job_name, status=status).set(time.time())
    push_duration_seconds.labels(job=job_name, status=status).set(duration)


def push_to_gateway_with_health(
    gateway_url: str,
    job_name: str,
    *,
    registry: object = REGISTRY,
    push_func: Callable[..., None] | None = None,
    publish_result: bool = False,
) -> None:
    """Push metrics and record Pushgateway health metrics in the same registry.

    `publish_result=True` is useful for one-shot batch registries: it performs a
    best-effort second push after recording success/failure so the health metric
    is visible in the same run.
    """
    push = push_func or push_to_gateway
    started = time.monotonic()
    try:
        push(gateway_url, job=job_name, registry=registry)
    except Exception as exc:
        duration = time.monotonic() - started
        _record_push_health(
            registry=registry,
            job_name=job_name,
            status="failure",
            duration=duration,
            error_type=exc.__class__.__name__,
        )
        if publish_result:
            try:
                push(gateway_url, job=job_name, registry=registry)
            except Exception:
                pass
        raise

    duration = time.monotonic() - started
    _record_push_health(registry=registry, job_name=job_name, status="success", duration=duration)
    if publish_result:
        try:
            push(gateway_url, job=job_name, registry=registry)
        except Exception as exc:
            logger.warning(
                "Pushgateway health metric follow-up push failed (%s): %s",
                job_name,
                exc,
            )


def _push_metrics_once(gateway_url: str, job_name: str) -> None:
    push_to_gateway_with_health(gateway_url, job_name, registry=REGISTRY)


def _push_metrics_loop(gateway_url: str, job_name: str, interval_sec: int) -> None:
    while True:
        try:
            _push_metrics_once(gateway_url, job_name)
        except Exception as exc:
            logger.warning("Pushgateway 전송 실패 (%s): %s", job_name, exc)
        time.sleep(interval_sec)


def _start_metrics_pusher() -> None:
    global _push_thread_started
    if _push_thread_started:
        return

    gateway_url = os.getenv("PROMETHEUS_PUSHGATEWAY_URL", "").strip()
    if not gateway_url:
        return

    job_name = os.getenv("PROMETHEUS_PUSH_JOB", "app-metrics").strip() or "app-metrics"
    interval_sec = max(5, int(os.getenv("PROMETHEUS_PUSH_INTERVAL_SEC", "15")))
    thread = threading.Thread(
        target=_push_metrics_loop,
        args=(gateway_url, job_name, interval_sec),
        daemon=True,
        name=f"metrics-pusher-{job_name}",
    )
    thread.start()
    _push_thread_started = True
    logger.info(
        "Pushgateway pusher 시작 gateway=%s job=%s interval=%ss",
        gateway_url,
        job_name,
        interval_sec,
    )


def start_metrics_server(port: int = 8888) -> None:
    """Prometheus scrape 엔드포인트 노출 + 선택적 Pushgateway pusher 시작.

    port <= 0 이면 HTTP 서버는 열지 않고 Pushgateway pusher만 시작한다.
    """
    if port > 0:
        try:
            start_http_server(port)
        except OSError as e:
            logger.warning("Metrics 서버 포트 %s 이미 사용 중, skip: %s", port, e)
    _start_metrics_pusher()


# ── Gauge: 현재 TPS (초당 WebSocket 수신 메시지 수) ────────────────────
collector_tps = Gauge(
    "collector_tps_msgs_per_sec",
    "초당 WebSocket/Kafka input 수신 메시지 수 (1초 슬라이딩 윈도우, Kafka ack 성공률 아님)",
    ["collector"],
)

# ── Gauge: 마지막 메시지 수신 시각 (Unix timestamp) ──────────────────────
# 값이 오래되면 수집기 hang 감지 가능
# Grafana: time() - collector_last_message_timestamp > 30 → 알림
collector_last_message_timestamp = Gauge(
    "collector_last_message_timestamp",
    "마지막 메시지 수신 Unix timestamp (staleness 감지용)",
    ["collector"],
)

# ── Counter: Kafka 누적 전송 메시지 수 ──────────────────────────────────
kafka_messages_produced_total = Counter(
    "kafka_messages_produced_total",
    "Kafka 전송 메시지 누적 수",
    ["collector", "topic"],
)

kafka_delivery_success_total = Counter(
    "kafka_delivery_success_total",
    "Kafka broker delivery ack 성공 누적 수",
    ["collector", "topic"],
)

kafka_delivery_failure_total = Counter(
    "kafka_delivery_failure_total",
    "Kafka broker delivery ack 실패 누적 수",
    ["collector", "topic", "error_type"],
)

kafka_producer_pending_messages = Gauge(
    "kafka_producer_pending_messages",
    "Kafka producer delivery ack 대기 메시지 수",
    ["backend"],
)

collector_last_kafka_ack_timestamp = Gauge(
    "collector_last_kafka_ack_timestamp",
    "수집기별 마지막 Kafka delivery ack 성공 Unix timestamp",
    ["collector", "topic"],
)

collector_last_error_timestamp = Gauge(
    "collector_last_error_timestamp",
    "수집기별 마지막 에러 Unix timestamp",
    ["collector", "error_type"],
)

collector_consecutive_errors = Gauge(
    "collector_consecutive_errors",
    "수집기별 연속 에러 횟수",
    ["collector", "error_type"],
)

collector_heartbeat_timestamp = Gauge(
    "collector_heartbeat_timestamp",
    "수집기 상태 heartbeat Unix timestamp",
    ["collector", "symbol", "source", "topic"],
)

collector_source_success_timestamp = Gauge(
    "collector_source_success_timestamp",
    "수집기별 source fetch 성공 Unix timestamp",
    ["collector", "symbol", "source", "topic"],
)

collector_kafka_send_success_timestamp = Gauge(
    "collector_kafka_send_success_timestamp",
    "수집기별 Kafka delivery ack 성공 Unix timestamp",
    ["collector", "symbol", "source", "topic"],
)

collector_source_last_error_timestamp = Gauge(
    "collector_source_last_error_timestamp",
    "수집기별 source/error 마지막 Unix timestamp",
    ["collector", "symbol", "source", "error_type"],
)

collector_consecutive_errors_by_source = Gauge(
    "collector_consecutive_errors_by_source",
    "수집기별 source/error 연속 에러 수",
    ["collector", "symbol", "source", "error_type"],
)

collector_reconnects_total = Counter(
    "collector_reconnects_total",
    "수집기별 reconnect 누적 수",
    ["collector", "symbol", "source", "reason"],
)

collector_running_state = Gauge(
    "collector_running_state",
    "수집기 실행 상태 (1=running, 0=stopped)",
    ["collector", "symbol", "source"],
)

rest_requests_total = Counter(
    "rest_requests_total",
    "REST polling 요청 결과 누적 수",
    ["collector", "symbol", "source", "status"],
)

rest_backoff_seconds = Gauge(
    "rest_backoff_seconds",
    "REST polling 현재 backoff seconds",
    ["collector", "symbol", "source"],
)

# ── Counter: Salting bucket 분포 확인용 ─────────────────────────────────
# bucket_mod 라벨은 bucket % partition_count 값을 넣을 때 사용한다.
kafka_partition_key_bucket_total = Counter(
    "kafka_partition_key_bucket_total",
    "Salting bucket 분포 (토픽별)",
    ["topic", "bucket_mod"],
)

# ── Counter: 수집기 에러 누적 수 (유형별) ────────────────────────────────
# error_type 예시: json_parse, kafka_send, ws_timeout, api_http_error
collector_errors_total = Counter(
    "collector_errors_total",
    "수집기 에러 누적 수",
    ["collector", "error_type"],
)

# ── Counter: WebSocket 재연결 횟수 ──────────────────────────────────────
websocket_reconnects_total = Counter(
    "websocket_reconnects_total",
    "WebSocket 재연결 누적 횟수",
    ["collector"],
)

bookticker_rollup_pending_buckets = Gauge(
    "bookticker_rollup_pending_buckets",
    "bookTicker rollup flush 대기 bucket 수",
    ["processor"],
)

trade_cvd_rollup_pending_buckets = Gauge(
    "trade_cvd_rollup_pending_buckets",
    "trade CVD rollup flush 대기 bucket 수",
    ["processor"],
)

rollup_processor_input_event_lag_seconds = Gauge(
    "rollup_processor_input_event_lag_seconds",
    "Rollup processor가 관측한 최신 input event-time의 wall-clock 지연",
    ["processor"],
)

rollup_processor_oldest_pending_bucket_age_seconds = Gauge(
    "rollup_processor_oldest_pending_bucket_age_seconds",
    "Rollup processor pending map에서 가장 오래된 bucket의 wall-clock age",
    ["processor"],
)

rollup_processor_last_poll_records = Gauge(
    "rollup_processor_last_poll_records",
    "Rollup processor 마지막 Kafka poll에서 읽은 record 수",
    ["processor"],
)

rollup_processor_last_poll_duration_seconds = Gauge(
    "rollup_processor_last_poll_duration_seconds",
    "Rollup processor 마지막 Kafka poll 소요 시간",
    ["processor"],
)

rollup_processor_last_flush_records = Gauge(
    "rollup_processor_last_flush_records",
    "Rollup processor 마지막 flush에서 output topic으로 보낸 record 수",
    ["processor", "output_topic"],
)

rollup_processor_last_flush_duration_seconds = Gauge(
    "rollup_processor_last_flush_duration_seconds",
    "Rollup processor 마지막 flush+producer.flush 소요 시간",
    ["processor", "output_topic"],
)

rollup_processor_last_flush_timestamp = Gauge(
    "rollup_processor_last_flush_timestamp",
    "Rollup processor 마지막 flush Unix timestamp",
    ["processor", "output_topic"],
)

rollup_processor_last_commit_duration_seconds = Gauge(
    "rollup_processor_last_commit_duration_seconds",
    "Rollup processor 마지막 Kafka consumer commit 소요 시간",
    ["processor"],
)

rollup_processor_output_event_lag_seconds = Gauge(
    "rollup_processor_output_event_lag_seconds",
    "Rollup processor output record의 event-time 기준 wall-clock 지연",
    ["processor", "output_topic"],
)

rollup_processor_flush_batches_total = Counter(
    "rollup_processor_flush_batches_total",
    "Rollup processor flush batch 누적 수",
    ["processor", "output_topic"],
)

rollup_processor_flush_records_total = Counter(
    "rollup_processor_flush_records_total",
    "Rollup processor flush output record 누적 수",
    ["processor", "output_topic"],
)

rollup_processor_commit_errors_total = Counter(
    "rollup_processor_commit_errors_total",
    "Rollup processor Kafka consumer commit 실패 누적 수",
    ["processor"],
)

# ── Counter: 뉴스 기사 ClickHouse + Qdrant 적재 누적 수 (소스별) ──────────────────────────
# source 예시: coindesk, cointelegraph, bitcoinmagazine
news_articles_indexed_total = Counter(
    "news_articles_indexed_total",
    "뉴스 수집기가 적재한 기사 누적 수",
    ["source"],
)

news_crawl_status_total = Counter(
    "news_crawl_status_total",
    "RSS 뉴스 수집기의 소스별 crawl status 누적 수",
    ["source", "crawl_status"],
)

news_image_feed_coverage_ratio = Gauge(
    "news_image_feed_coverage_ratio",
    "마지막 RSS 응답에서 안전한 이미지 메타데이터가 포함된 기사 비율",
    ["source"],
)

news_image_backfilled_total = Counter(
    "news_image_backfilled_total",
    "RSS 이미지 메타데이터로 보완한 기존 뉴스 기사 누적 수",
    ["source"],
)

ai_enrichment_success_total = Counter(
    "ai_enrichment_success_total",
    "뉴스 AI enrichment 성공 누적 수",
    ["provider"],
)

ai_enrichment_auth_failure_total = Counter(
    "ai_enrichment_auth_failure_total",
    "뉴스 AI provider 인증 실패 누적 수",
    ["provider"],
)

ai_enrichment_fallback_total = Counter(
    "ai_enrichment_fallback_total",
    "뉴스 local fallback 적용 누적 수",
    ["reason"],
)

translation_complete_total = Counter(
    "translation_complete_total",
    "한국어 title_ko/summary_ko 생성 완료 누적 수",
    ["provider"],
)

# ── Counter: DLQ로 전송된 메시지 누적 수 ──────────────────────────────────
# dlq_type: producer_failure, ch_failure
# original_topic: 원본 토픽 이름
# error_type: connection_timeout, serialization_error, ch_connection_error, ch_schema_error 등
dlq_messages_total = Counter(
    "dlq_messages_total",
    "DLQ로 전송된 메시지 누적 수",
    ["dlq_type", "original_topic", "error_type"],
)

collector_bad_rows_total = Counter(
    "collector_bad_rows_total",
    "Collector 단계에서 격리된 bad row 누적 수",
    ["collector", "symbol", "source", "topic", "error_type"],
)

collector_schema_validation_failures_total = Counter(
    "collector_schema_validation_failures_total",
    "Collector 최소 schema validation 실패 누적 수",
    ["collector", "symbol", "source", "topic", "error_type"],
)

collector_json_serialization_failures_total = Counter(
    "collector_json_serialization_failures_total",
    "Collector JSON serialization 실패 누적 수",
    ["collector", "symbol", "source", "topic", "error_type"],
)

collector_dlq_send_failures_total = Counter(
    "collector_dlq_send_failures_total",
    "Collector bad row/DLQ 전송 실패 누적 수",
    ["collector", "symbol", "source", "topic", "error_type"],
)

# ── Gauge: DLQ 토픽 크기 (메시지 수) ──────────────────────────────────────
# dlq_topic: dlq-producer, dlq-ch
dlq_topic_size = Gauge(
    "dlq_topic_size",
    "DLQ 토픽의 현재 메시지 수 (대략적)",
    ["dlq_topic"],
)

# ── Gauge: Paper trader price source health ────────────────────────────────
paper_price_rows_total = Gauge(
    "paper_price_rows_total",
    "Paper trader가 최근 관측한 market view row 수",
    ["strategy", "symbol", "view"],
)

paper_price_fallback_rows = Gauge(
    "paper_price_fallback_rows",
    "last_price <= 0 이고 mark_price > 0 인 row 수",
    ["strategy", "symbol", "view"],
)

paper_price_fallback_ratio = Gauge(
    "paper_price_fallback_ratio",
    "최근 market view row 중 유효 가격 fallback 비율",
    ["strategy", "symbol", "view"],
)

paper_price_anomaly_active = Gauge(
    "paper_price_anomaly_active",
    "Paper trader price source anomaly 활성 여부",
    ["strategy", "symbol", "view"],
)

# ── Counter: Paper trader price source anomaly events ──────────────────────
paper_price_anomaly_events_total = Counter(
    "paper_price_anomaly_events_total",
    "Paper trader price source anomaly state transition count",
    ["strategy", "symbol", "view", "state"],
)

# ── Counter: Auto-trading funnel / control-plane observability ──────────────
# stage 예시: market_snapshot, feature_ready, raw_signal, risk_pass,
# order_intent, simulated_order, fill, close
trading_pipeline_stage_total = Counter(
    "trading_pipeline_stage_total",
    "Auto-trading funnel stage transition count",
    ["strategy", "symbol", "side", "stage", "status", "reason"],
)

trading_decision_events_total = Counter(
    "trading_decision_events_total",
    "Trading orchestrator decision events by source/action/reason",
    ["strategy", "symbol", "side", "source", "action", "severity", "reason"],
)

trading_decision_proposals_total = Counter(
    "trading_decision_proposals_total",
    "Resolved trading proposals by action/source/reason",
    ["strategy", "symbol", "side", "action", "source", "reason"],
)


_HIGH_CARDINALITY_LABEL_MARKERS = (
    "order_id",
    "position_id",
    "run_id",
    "user_id",
    "request_id",
    "trace_id",
    "payload",
    "metadata",
    "timestamp",
)


def _normalize_metric_label(value: object, *, default: str = "unknown", max_len: int = 80) -> str:
    text = str(value or "").strip()
    if not text:
        text = default
    lowered = text.lower()
    if any(marker in lowered for marker in _HIGH_CARDINALITY_LABEL_MARKERS):
        return "high_cardinality"
    return text[:max_len]


def record_trading_pipeline_stage(
    *,
    strategy: str,
    symbol: str,
    side: str,
    stage: str,
    status: str = "ok",
    reason: str = "",
    amount: float = 1.0,
) -> None:
    """Record one auto-trading funnel transition.

    Keep label values intentionally coarse. High-cardinality identifiers such as
    order ids, intent ids, timestamps, or raw exception messages should stay in
    logs/ledger rows, not Prometheus labels.
    """
    trading_pipeline_stage_total.labels(
        strategy=_normalize_metric_label(strategy),
        symbol=_normalize_metric_label(symbol).upper(),
        side=_normalize_metric_label(side).upper(),
        stage=_normalize_metric_label(stage),
        status=_normalize_metric_label(status),
        reason=_normalize_metric_label(reason, default="none"),
    ).inc(amount)


def record_trading_decision_event(
    *,
    strategy: str,
    symbol: str,
    side: str,
    source: str,
    action: str,
    severity: str = "INFO",
    reason: str = "",
    amount: float = 1.0,
) -> None:
    """Record one module-level trading decision event."""
    trading_decision_events_total.labels(
        strategy=_normalize_metric_label(strategy),
        symbol=_normalize_metric_label(symbol).upper(),
        side=_normalize_metric_label(side).upper(),
        source=_normalize_metric_label(source),
        action=_normalize_metric_label(action).upper(),
        severity=_normalize_metric_label(severity).upper(),
        reason=_normalize_metric_label(reason, default="none"),
    ).inc(amount)


def record_trading_decision_proposal(
    *,
    strategy: str,
    symbol: str,
    side: str,
    action: str,
    source: str = "",
    reason: str = "",
    amount: float = 1.0,
) -> None:
    """Record the final resolved proposal from the trading orchestrator."""
    trading_decision_proposals_total.labels(
        strategy=_normalize_metric_label(strategy),
        symbol=_normalize_metric_label(symbol).upper(),
        side=_normalize_metric_label(side).upper(),
        action=_normalize_metric_label(action).upper(),
        source=_normalize_metric_label(source),
        reason=_normalize_metric_label(reason, default="none"),
    ).inc(amount)
