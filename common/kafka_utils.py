"""Kafka Producer wrapper with DLQ support.

내부 배치 레이어 제거 — kafka-python의 기본 배치(linger_ms + batch_size)에 위임.
send()는 producer.send()를 직접 호출하여 asyncio 이벤트 루프 블로킹 최소화.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
import traceback
from typing import Any

try:
    from confluent_kafka import Producer as ConfluentProducer
except ImportError:  # pragma: no cover - optional fast-path dependency
    ConfluentProducer = None

try:
    from kafka import KafkaProducer
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency in lean test envs
    KafkaProducer = None

from common.bad_rows import (
    build_bad_row_context,
    record_bad_row_metric,
    record_dlq_send_failure_metric,
    write_bad_row_fallback,
)
from common.config import normalize_kafka_bootstrap_servers

try:
    import orjson

    def _serialize_value(value: Any) -> bytes:
        return orjson.dumps(value)

except ImportError:
    import json

    def _serialize_value(value: Any) -> bytes:
        return json.dumps(value).encode("utf-8")


logger = logging.getLogger(__name__)
DEFAULT_FLUSH_TIMEOUT_SEC = 10.0
MAX_LOG_VALUE_LEN = 300
_SECRET_PATTERN = re.compile(r"(?i)(password|passwd|secret|token|api[_-]?key)([\"']?\s*[:=]\s*[\"']?)([^\"'\s,}]+)")


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        logger.warning("Invalid %s=%r; using %d", name, os.getenv(name), default)
        return default


def _split_bootstrap_servers(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


class KafkaProducerWrapper:
    _instance = None
    _lock = threading.Lock()

    def __new__(
        cls,
        bootstrap_servers: str = "localhost:9092,localhost:9095,localhost:9094",
        dlq_topic: str = "dlq-producer",
    ):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init(bootstrap_servers, dlq_topic)
        return cls._instance

    def _init(self, bootstrap_servers: str, dlq_topic: str) -> None:
        self.dlq_topic = dlq_topic
        bootstrap_servers = normalize_kafka_bootstrap_servers(bootstrap_servers)
        self.bootstrap_servers = bootstrap_servers
        self._backend = self._resolve_backend()
        self._init_runtime_state()
        if self._backend == "confluent":
            self.producer = ConfluentProducer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "compression.type": os.getenv("KAFKA_PRODUCER_COMPRESSION_TYPE", "zstd"),
                    "batch.size": _int_env("KAFKA_PRODUCER_BATCH_SIZE", 131072),
                    "linger.ms": _int_env("KAFKA_PRODUCER_LINGER_MS", 50),
                    "acks": "all",
                    "retries": _int_env("KAFKA_PRODUCER_RETRIES", 3),
                    "queue.buffering.max.messages": _int_env("KAFKA_PRODUCER_QUEUE_MAX_MESSAGES", 500000),
                    "queue.buffering.max.kbytes": _int_env("KAFKA_PRODUCER_QUEUE_MAX_KBYTES", 1048576),
                    "socket.keepalive.enable": True,
                }
            )
        else:
            if KafkaProducer is None:
                raise RuntimeError("kafka-python is not installed")
            self.producer = KafkaProducer(
                bootstrap_servers=_split_bootstrap_servers(bootstrap_servers),
                value_serializer=_serialize_value,
                key_serializer=lambda key: key.encode("utf-8") if key else None,
                batch_size=_int_env("KAFKA_PRODUCER_BATCH_SIZE", 65536),
                linger_ms=_int_env("KAFKA_PRODUCER_LINGER_MS", 100),
                compression_type=os.getenv("KAFKA_PRODUCER_COMPRESSION_TYPE", "gzip"),
                acks="all",
                retries=_int_env("KAFKA_PRODUCER_RETRIES", 3),
            )
        logger.info(
            "Kafka Producer 생성: %s, backend=%s, DLQ 토픽: %s",
            bootstrap_servers,
            self._backend,
            dlq_topic,
        )

    def _init_runtime_state(self) -> None:
        self._closed = False
        self._close_lock = threading.Lock()
        self._in_delivery_callback = False
        self._pending_deliveries = 0
        self.send_attempt_count = 0
        self.delivery_success_count = 0
        self.delivery_failure_count = 0
        self.flush_timeout_count = 0
        self.dlq_failure_count = 0

    def _resolve_backend(self) -> str:
        requested = os.getenv("KAFKA_PRODUCER_BACKEND", "auto").strip().lower()
        if requested in {"confluent", "librdkafka"}:
            if ConfluentProducer is None:
                raise RuntimeError("KAFKA_PRODUCER_BACKEND=confluent but confluent-kafka is not installed")
            return "confluent"
        if requested in {"kafka-python", "python", "legacy"}:
            return "kafka-python"
        if ConfluentProducer is not None:
            return "confluent"
        return "kafka-python"

    def send(
        self,
        topic: str,
        value: dict,
        key: str | None = None,
        collector_name: str | None = None,
        partition: int | None = None,
        symbol: str | None = None,
        source: str | None = None,
    ):
        """Send one message to Kafka without blocking the caller on broker ack."""
        if self._closed:
            error_type = "producer_closed"
            self._record_delivery_failure(topic, collector_name, error_type, symbol, source)
            logger.warning(
                "[Kafka] Send rejected because producer is closed - topic=%s, partition=%s, key=%s",
                topic,
                partition,
                key,
            )
            return None
        self.send_attempt_count += 1
        if self._backend == "confluent":
            return self._send_confluent(topic, value, key, collector_name, partition, symbol, source)

        try:
            future = self.producer.send(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
            )
            self._increment_pending()
            self._add_delivery_callbacks(future, topic, value, key, collector_name, partition, symbol, source)
        except Exception as exc:
            logger.error(
                "[Kafka] Send failed before broker ack - topic=%s, partition=%s, key=%s, error=%s: %s",
                topic,
                partition,
                key,
                self._classify_error(exc),
                self._safe_error_message(exc),
            )
            self._record_delivery_failure(topic, collector_name, self._classify_error(exc), symbol, source)
            self._handle_delivery_failure(exc, topic, value, key, collector_name, partition)
        return None

    def _send_confluent(
        self,
        topic: str,
        value: dict,
        key: str | None,
        collector_name: str | None,
        partition: int | None,
        symbol: str | None,
        source: str | None,
    ) -> None:
        encoded_value = _serialize_value(value)
        encoded_key = key.encode("utf-8") if key else None

        def on_delivery(error, _message) -> None:
            self._in_delivery_callback = True
            try:
                if error is not None:
                    self._decrement_pending()
                    self._record_delivery_failure(
                        topic,
                        collector_name,
                        self._classify_error(error),
                        symbol,
                        source,
                    )
                    self._handle_delivery_failure(error, topic, value, key, collector_name, partition)
                else:
                    self._decrement_pending()
                    self._record_delivery_success(topic, collector_name, symbol, source)
            finally:
                self._in_delivery_callback = False

        try:
            self.producer.produce(
                topic,
                value=encoded_value,
                key=encoded_key,
                partition=-1 if partition is None else partition,
                on_delivery=on_delivery,
            )
            self._increment_pending()
            self.producer.poll(0)
        except BufferError:
            self.producer.poll(0.05)
            try:
                self.producer.produce(
                    topic,
                    value=encoded_value,
                    key=encoded_key,
                    partition=-1 if partition is None else partition,
                    on_delivery=on_delivery,
                )
                self._increment_pending()
                self.producer.poll(0)
            except Exception as exc:
                logger.error(
                    "[Kafka] Send failed after buffer retry - topic=%s, partition=%s, key=%s, error=%s: %s",
                    topic,
                    partition,
                    key,
                    self._classify_error(exc),
                    self._safe_error_message(exc),
                )
                self._record_delivery_failure(topic, collector_name, self._classify_error(exc), symbol, source)
                self._handle_delivery_failure(exc, topic, value, key, collector_name, partition)
        except Exception as exc:
            logger.error(
                "[Kafka] Send failed before broker ack - topic=%s, partition=%s, key=%s, error=%s: %s",
                topic,
                partition,
                key,
                self._classify_error(exc),
                self._safe_error_message(exc),
            )
            self._record_delivery_failure(topic, collector_name, self._classify_error(exc), symbol, source)
            self._handle_delivery_failure(exc, topic, value, key, collector_name, partition)
        return None

    def send_batch(
        self,
        topic: str,
        values: list[dict],
        key: str | None = None,
        collector_name: str | None = None,
        partition: int | None = None,
        symbol: str | None = None,
        source: str | None = None,
    ) -> None:
        """Send multiple messages to kafka-python producer."""
        for value in values:
            self.send(
                topic,
                value,
                key,
                collector_name,
                partition,
                symbol=symbol,
                source=source,
            )

    def send_bad_row(
        self,
        *,
        original_topic: str,
        payload: Any,
        error_type: str,
        reason: str,
        key: str | None = None,
        collector_name: str | None = None,
        symbol: str | None = None,
        source: str | None = None,
    ) -> None:
        """Send collector bad-row context to the existing producer DLQ.

        This path intentionally bypasses send() so a DLQ failure cannot loop
        back into DLQ handling forever.
        """
        context = build_bad_row_context(
            error_type=error_type,
            collector=collector_name,
            symbol=symbol,
            source=source,
            topic=original_topic,
            key=key,
            reason=reason,
            payload=payload,
            schema_version=(payload.get("schema_version") if isinstance(payload, dict) else None),
            original_topic=original_topic,
        )
        record_bad_row_metric(
            collector=collector_name,
            symbol=symbol,
            source=source,
            topic=original_topic,
            error_type=error_type,
        )

        if original_topic == self.dlq_topic or self._closed:
            self.dlq_failure_count += 1
            record_dlq_send_failure_metric(
                collector=collector_name,
                symbol=symbol,
                source=source,
                topic=original_topic,
                error_type=error_type,
            )
            try:
                write_bad_row_fallback(context)
            except Exception as fallback_exc:
                logger.critical(
                    "[DLQ] Local fallback write failed: %s",
                    self._safe_error_message(fallback_exc),
                )
            return

        dlq_message = {
            "original_topic": original_topic,
            "original_key": key,
            "symbol": context.get("symbol"),
            "stream": source,
            "payload_preview": context.get("payload_preview"),
            "error_type": error_type,
            "error_message": context.get("reason"),
            "timestamp": context.get("occurred_at"),
            "collector": context.get("collector"),
            "bad_row": context,
        }
        try:
            if self._backend == "confluent":
                self.producer.produce(
                    self.dlq_topic,
                    value=_serialize_value(dlq_message),
                    key=key.encode("utf-8") if key else None,
                    on_delivery=lambda err, _msg: (
                        self._record_dlq_delivery_failure(err, self.dlq_topic, key, context)
                        if err is not None
                        else self._record_dlq_metric(original_topic, error_type)
                    ),
                )
                self.producer.poll(0)
            else:
                future = self.producer.send(self.dlq_topic, value=dlq_message, key=key)
                future.add_callback(lambda _: self._record_dlq_metric(original_topic, error_type))
                future.add_errback(lambda err: self._record_dlq_delivery_failure(err, self.dlq_topic, key, context))
        except Exception as exc:
            self.dlq_failure_count += 1
            record_dlq_send_failure_metric(
                collector=collector_name,
                symbol=symbol,
                source=source,
                topic=original_topic,
                error_type=error_type,
            )
            try:
                write_bad_row_fallback(context)
            except Exception as fallback_exc:
                logger.critical(
                    "[DLQ] Local fallback write failed: %s",
                    self._safe_error_message(fallback_exc),
                )
            logger.critical(
                "[DLQ] Bad row DLQ send failed; attempted local fallback - topic=%s, key=%s, error=%s",
                original_topic,
                key,
                self._safe_error_message(exc),
            )

    def _add_delivery_callbacks(
        self,
        future,
        topic: str,
        value: dict,
        key: str | None,
        collector_name: str | None,
        partition: int | None,
        symbol: str | None,
        source: str | None,
    ) -> None:
        def on_success(_record_metadata) -> None:
            self._decrement_pending()
            self._record_delivery_success(topic, collector_name, symbol, source)

        def on_failure(exception: Exception) -> None:
            self._decrement_pending()
            self._record_delivery_failure(topic, collector_name, self._classify_error(exception), symbol, source)
            self._handle_delivery_failure(exception, topic, value, key, collector_name, partition)

        future.add_callback(on_success)
        future.add_errback(on_failure)

    def _handle_delivery_failure(
        self,
        exception: Exception,
        topic: str,
        value: dict,
        key: str | None,
        collector_name: str | None,
        partition: int | None,
    ) -> None:
        error_type = self._classify_error(exception)
        if topic == self.dlq_topic:
            self.dlq_failure_count += 1
            record_dlq_send_failure_metric(
                collector=collector_name,
                symbol=None,
                source=None,
                topic=topic,
                error_type=error_type,
            )
            logger.critical(
                "[DLQ] DLQ delivery failed; recursion blocked - topic=%s, partition=%s, key=%s, error=%s: %s",
                topic,
                partition,
                key,
                error_type,
                self._safe_error_message(exception),
            )
            return
        logger.error(
            "[DLQ] Producer 실패 - topic=%s, partition=%s, key=%s, error=%s: %s",
            topic,
            partition,
            key,
            error_type,
            self._safe_error_message(exception),
        )
        try:
            context = self._message_context(value)
            fallback_context = build_bad_row_context(
                error_type=error_type,
                collector=collector_name,
                symbol=context.get("symbol"),
                source=context.get("stream"),
                topic=topic,
                key=key,
                reason=self._safe_error_message(exception),
                payload=value,
            )
            dlq_message = {
                "original_topic": topic,
                "original_key": key,
                "original_partition": partition,
                "symbol": context.get("symbol"),
                "stream": context.get("stream"),
                "payload_preview": context.get("payload_preview"),
                "error_type": error_type,
                "error_message": self._safe_error_message(exception),
                "error_stack": self._safe_error_message(traceback.format_exc()),
                "timestamp": time.time(),
                "retry_count": 3,
                "collector": collector_name or "unknown",
            }
            if self._backend == "confluent" and self._in_delivery_callback:
                self.dlq_failure_count += 1
                record_dlq_send_failure_metric(
                    collector=collector_name,
                    symbol=context.get("symbol"),
                    source=context.get("stream"),
                    topic=topic,
                    error_type=error_type,
                )
                write_bad_row_fallback(fallback_context)
                logger.critical(
                    "[DLQ] Confluent delivery callback cannot safely produce DLQ; wrote local fallback - "
                    "topic=%s, key=%s, error=%s",
                    topic,
                    key,
                    error_type,
                )
            elif self._backend == "confluent":
                self.producer.produce(
                    self.dlq_topic,
                    value=_serialize_value(dlq_message),
                    key=key.encode("utf-8") if key else None,
                    on_delivery=lambda err, _msg: (
                        self._record_dlq_delivery_failure(err, self.dlq_topic, key, fallback_context)
                        if err is not None
                        else self._record_dlq_metric(topic, error_type)
                    ),
                )
                self.producer.poll(0)
            else:
                dlq_future = self.producer.send(self.dlq_topic, value=dlq_message, key=key)
                dlq_future.add_callback(lambda _: self._record_dlq_metric(topic, error_type))
                dlq_future.add_errback(
                    lambda err: self._record_dlq_delivery_failure(err, self.dlq_topic, key, fallback_context)
                )
        except Exception as exc:
            self.dlq_failure_count += 1
            record_dlq_send_failure_metric(
                collector=collector_name,
                symbol=None,
                source=None,
                topic=topic,
                error_type=error_type,
            )
            try:
                write_bad_row_fallback(
                    build_bad_row_context(
                        error_type=error_type,
                        collector=collector_name,
                        symbol=None,
                        source=None,
                        topic=topic,
                        key=key,
                        reason=self._safe_error_message(exc),
                        payload=value,
                    )
                )
            except Exception as fallback_exc:
                logger.critical(
                    "[DLQ] Local fallback write failed: %s",
                    self._safe_error_message(fallback_exc),
                )
            logger.critical(
                "[DLQ] DLQ 메시지 구성 실패: %s. 원본 메시지 손실 - topic=%s, key=%s",
                self._safe_error_message(exc),
                topic,
                key,
            )

    def _record_dlq_delivery_failure(
        self,
        exception: Exception,
        topic: str,
        key: str | None,
        fallback_context: dict[str, Any] | None = None,
    ) -> None:
        self.dlq_failure_count += 1
        error_type = self._classify_error(exception)
        record_dlq_send_failure_metric(
            collector=fallback_context.get("collector") if fallback_context else None,
            symbol=fallback_context.get("symbol") if fallback_context else None,
            source=fallback_context.get("source") if fallback_context else None,
            topic=fallback_context.get("topic") if fallback_context else topic,
            error_type=error_type,
        )
        if fallback_context:
            try:
                write_bad_row_fallback(fallback_context)
            except Exception as fallback_exc:
                logger.critical(
                    "[DLQ] Local fallback write failed: %s",
                    self._safe_error_message(fallback_exc),
                )
        logger.critical(
            "[DLQ] DLQ 전송도 실패; recursion blocked - topic=%s, key=%s, error=%s",
            topic,
            key,
            self._safe_error_message(exception),
        )

    def _record_dlq_metric(self, original_topic: str, error_type: str) -> None:
        try:
            from common.metrics import dlq_messages_total

            dlq_messages_total.labels(
                dlq_type="producer_failure",
                original_topic=original_topic,
                error_type=error_type,
            ).inc()
        except Exception as exc:
            logger.warning("[DLQ] 메트릭 업데이트 실패: %s", exc)

    def _classify_error(self, exception: Exception) -> str:
        error_str = str(exception).lower()
        if "timeout" in error_str or "timed out" in error_str:
            return "connection_timeout"
        if "connection" in error_str or "connect" in error_str:
            return "connection_error"
        if "serialization" in error_str or "serializable" in error_str or "encode" in error_str:
            return "serialization_error"
        if "size" in error_str or "too large" in error_str:
            return "message_too_large"
        return "unknown_error"

    def _increment_pending(self) -> None:
        self._pending_deliveries += 1
        self._record_pending_gauge()

    def _decrement_pending(self) -> None:
        self._pending_deliveries = max(self._pending_deliveries - 1, 0)
        self._record_pending_gauge()

    def _record_delivery_success(
        self,
        topic: str,
        collector_name: str | None,
        symbol: str | None = None,
        source: str | None = None,
    ) -> None:
        self.delivery_success_count += 1
        collector = collector_name or "unknown"
        try:
            from common.metrics import (
                collector_last_kafka_ack_timestamp,
                kafka_delivery_success_total,
            )

            kafka_delivery_success_total.labels(collector=collector, topic=topic).inc()
            collector_last_kafka_ack_timestamp.labels(collector=collector, topic=topic).set(time.time())
        except Exception as exc:
            logger.debug("[Kafka] delivery success metric update failed: %s", exc)

        try:
            from common.collector_health import record_kafka_send_success

            record_kafka_send_success(
                collector=collector,
                symbol=symbol or "unknown",
                source=source or "unknown",
                topic=topic,
            )
        except Exception as exc:
            logger.debug("[Kafka] collector health success metric update failed: %s", exc)

    def _record_delivery_failure(
        self,
        topic: str,
        collector_name: str | None,
        error_type: str,
        symbol: str | None = None,
        source: str | None = None,
    ) -> None:
        self.delivery_failure_count += 1
        collector = collector_name or "unknown"
        try:
            from common.metrics import kafka_delivery_failure_total

            kafka_delivery_failure_total.labels(collector=collector, topic=topic, error_type=error_type).inc()
        except Exception as exc:
            logger.debug("[Kafka] delivery failure metric update failed: %s", exc)

        try:
            from common.collector_health import record_collector_error

            record_collector_error(
                collector=collector,
                symbol=symbol or "unknown",
                source=source or "kafka_delivery",
                error_type=f"kafka_{error_type}",
                consecutive_errors=self.delivery_failure_count,
            )
        except Exception as exc:
            logger.debug("[Kafka] collector health failure metric update failed: %s", exc)

    def _record_pending_gauge(self) -> None:
        try:
            from common.metrics import kafka_producer_pending_messages

            kafka_producer_pending_messages.labels(backend=self._backend).set(self._pending_deliveries)
        except Exception as exc:
            logger.debug("[Kafka] pending metric update failed: %s", exc)

    def _message_context(self, value: dict) -> dict[str, Any]:
        data = value.get("data") if isinstance(value, dict) else None
        return {
            "symbol": value.get("symbol") if isinstance(value, dict) else None,
            "stream": value.get("stream") if isinstance(value, dict) else None,
            "payload_preview": self._truncate_value(data),
        }

    def _truncate_value(self, value: Any) -> str:
        try:
            serialized = _serialize_value(value).decode("utf-8", errors="replace")
        except Exception:
            serialized = repr(value)
        serialized = self._mask_sensitive(serialized)
        if len(serialized) > MAX_LOG_VALUE_LEN:
            return serialized[:MAX_LOG_VALUE_LEN] + "...[truncated]"
        return serialized

    def _safe_error_message(self, exception: Exception | str) -> str:
        message = self._mask_sensitive(str(exception))
        if len(message) > MAX_LOG_VALUE_LEN:
            return message[:MAX_LOG_VALUE_LEN] + "...[truncated]"
        return message

    def _mask_sensitive(self, message: str) -> str:
        return _SECRET_PATTERN.sub(r"\1\2***", message)

    def flush(self, timeout: float | None = None) -> None:
        flush_timeout = DEFAULT_FLUSH_TIMEOUT_SEC if timeout is None else float(timeout)
        try:
            remaining = self.producer.flush(timeout=flush_timeout)
        except TypeError:
            remaining = self.producer.flush()
        except Exception as exc:
            self.flush_timeout_count += 1
            logger.warning(
                "[Kafka] Producer flush failed after %.1fs: %s",
                flush_timeout,
                self._safe_error_message(exc),
            )
            return

        if self._backend == "confluent" and remaining:
            self.flush_timeout_count += 1
            logger.warning(
                "[Kafka] Producer flush timeout after %.1fs; pending=%s",
                flush_timeout,
                remaining,
            )
        elif self._backend == "kafka-python" and self._pending_deliveries > 0:
            self.flush_timeout_count += 1
            logger.warning(
                "[Kafka] Producer flush completed with pending callbacks; pending=%s timeout=%.1fs",
                self._pending_deliveries,
                flush_timeout,
            )
        self._record_pending_gauge()

    def close(self, timeout: float | None = None) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self.flush(timeout=timeout)
            if self._backend == "kafka-python":
                try:
                    self.producer.close(timeout=(DEFAULT_FLUSH_TIMEOUT_SEC if timeout is None else float(timeout)))
                except TypeError:
                    self.producer.close()
                except Exception as exc:
                    logger.warning(
                        "[Kafka] Producer close failed: %s",
                        self._safe_error_message(exc),
                    )
