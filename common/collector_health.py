"""Shared collector health metric helpers.

The helpers keep collector observability labels small and consistent across
WebSocket, REST, and Kafka delivery paths. They never include payload values,
query strings, or secret-like data in metric labels.
"""

from __future__ import annotations

import time

from common.metrics import (
    collector_consecutive_errors,
    collector_consecutive_errors_by_source,
    collector_heartbeat_timestamp,
    collector_kafka_send_success_timestamp,
    collector_last_error_timestamp,
    collector_reconnects_total,
    collector_running_state,
    collector_source_last_error_timestamp,
    collector_source_success_timestamp,
)

UNKNOWN_LABEL = "unknown"
MAX_LABEL_LENGTH = 160


def normalize_metric_label(value: object | None) -> str:
    text = str(value or UNKNOWN_LABEL).strip() or UNKNOWN_LABEL
    text = text.split("?", 1)[0]
    return text[:MAX_LABEL_LENGTH]


def record_process_heartbeat(
    *,
    collector: str,
    symbol: str,
    source: str,
    topic: str = UNKNOWN_LABEL,
    running: bool = True,
    timestamp: float | None = None,
) -> float:
    ts = time.time() if timestamp is None else float(timestamp)
    labels = {
        "collector": normalize_metric_label(collector),
        "symbol": normalize_metric_label(symbol),
        "source": normalize_metric_label(source),
        "topic": normalize_metric_label(topic),
    }
    collector_heartbeat_timestamp.labels(**labels).set(ts)
    collector_running_state.labels(
        collector=labels["collector"],
        symbol=labels["symbol"],
        source=labels["source"],
    ).set(1.0 if running else 0.0)
    return ts


def record_source_success(
    *,
    collector: str,
    symbol: str,
    source: str,
    topic: str,
    timestamp: float | None = None,
) -> float:
    ts = record_process_heartbeat(
        collector=collector,
        symbol=symbol,
        source=source,
        topic=topic,
        running=True,
        timestamp=timestamp,
    )
    collector_source_success_timestamp.labels(
        collector=normalize_metric_label(collector),
        symbol=normalize_metric_label(symbol),
        source=normalize_metric_label(source),
        topic=normalize_metric_label(topic),
    ).set(ts)
    return ts


def record_kafka_send_success(
    *,
    collector: str,
    symbol: str,
    source: str,
    topic: str,
    timestamp: float | None = None,
) -> float:
    ts = time.time() if timestamp is None else float(timestamp)
    collector_kafka_send_success_timestamp.labels(
        collector=normalize_metric_label(collector),
        symbol=normalize_metric_label(symbol),
        source=normalize_metric_label(source),
        topic=normalize_metric_label(topic),
    ).set(ts)
    return ts


def record_collector_error(
    *,
    collector: str,
    symbol: str,
    source: str,
    error_type: str,
    consecutive_errors: int,
    timestamp: float | None = None,
) -> float:
    ts = time.time() if timestamp is None else float(timestamp)
    collector_label = normalize_metric_label(collector)
    error_label = normalize_metric_label(error_type)
    collector_last_error_timestamp.labels(collector=collector_label, error_type=error_label).set(ts)
    collector_consecutive_errors.labels(collector=collector_label, error_type=error_label).set(consecutive_errors)
    collector_source_last_error_timestamp.labels(
        collector=collector_label,
        symbol=normalize_metric_label(symbol),
        source=normalize_metric_label(source),
        error_type=error_label,
    ).set(ts)
    collector_consecutive_errors_by_source.labels(
        collector=collector_label,
        symbol=normalize_metric_label(symbol),
        source=normalize_metric_label(source),
        error_type=error_label,
    ).set(consecutive_errors)
    return ts


def reset_collector_error(
    *,
    collector: str,
    symbol: str,
    source: str,
    error_type: str,
) -> None:
    collector_label = normalize_metric_label(collector)
    error_label = normalize_metric_label(error_type)
    collector_consecutive_errors.labels(collector=collector_label, error_type=error_label).set(0)
    collector_consecutive_errors_by_source.labels(
        collector=collector_label,
        symbol=normalize_metric_label(symbol),
        source=normalize_metric_label(source),
        error_type=error_label,
    ).set(0)


def record_collector_reconnect(
    *,
    collector: str,
    symbol: str,
    source: str,
    reason: str,
) -> None:
    collector_reconnects_total.labels(
        collector=normalize_metric_label(collector),
        symbol=normalize_metric_label(symbol),
        source=normalize_metric_label(source),
        reason=normalize_metric_label(reason),
    ).inc()


__all__ = [
    "UNKNOWN_LABEL",
    "normalize_metric_label",
    "record_collector_error",
    "record_collector_reconnect",
    "record_kafka_send_success",
    "record_process_heartbeat",
    "record_source_success",
    "reset_collector_error",
]
