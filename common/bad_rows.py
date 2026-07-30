"""Collector bad-row helpers.

The helpers here keep malformed collector payloads observable without changing
normal Kafka payloads or topic routing.
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

MAX_PREVIEW_LEN = 500
MAX_REASON_LEN = 300
_SECRET_KEY_PATTERN = re.compile(r"(?i)(password|passwd|secret|token|api[_-]?key)")


def default_bad_row_path() -> Path:
    raw_dir = os.getenv("COLLECTOR_BAD_ROW_DIR")
    base_dir = Path(raw_dir) if raw_dir else Path(__file__).resolve().parents[1] / "data" / "bad_rows"
    return base_dir / "collector_bad_rows.jsonl"


def _mask_payload(value: Any) -> Any:
    if isinstance(value, dict):
        masked: dict[str, Any] = {}
        for key, item in value.items():
            if _SECRET_KEY_PATTERN.search(str(key)):
                masked[str(key)] = "***"
            else:
                masked[str(key)] = _mask_payload(item)
        return masked
    if isinstance(value, list):
        return [_mask_payload(item) for item in value[:50]]
    if isinstance(value, tuple):
        return [_mask_payload(item) for item in value[:50]]
    if isinstance(value, set):
        return [_mask_payload(item) for item in list(value)[:50]]
    return value


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


def payload_preview(payload: Any, *, max_len: int = MAX_PREVIEW_LEN) -> str:
    masked = _mask_payload(payload)
    try:
        preview = json.dumps(masked, ensure_ascii=False, sort_keys=True, default=repr)
    except (TypeError, ValueError):
        preview = repr(masked)
    if len(preview) > max_len:
        return preview[:max_len] + "...[truncated]"
    return preview


def original_payload_size(payload: Any) -> int:
    try:
        return len(json.dumps(payload, ensure_ascii=False, default=repr))
    except (TypeError, ValueError):
        return len(repr(payload))


def is_json_serializable(payload: Any) -> bool:
    try:
        json.dumps(payload)
        return True
    except (TypeError, ValueError):
        return False


def build_bad_row_context(
    *,
    error_type: str,
    collector: str | None,
    symbol: str | None,
    source: str | None,
    topic: str | None,
    key: str | None,
    reason: str,
    payload: Any,
    occurred_at: float | None = None,
    schema_version: int | str | None = None,
    original_topic: str | None = None,
) -> dict[str, Any]:
    safe_reason = reason
    if len(safe_reason) > MAX_REASON_LEN:
        safe_reason = safe_reason[:MAX_REASON_LEN] + "...[truncated]"
    return {
        "schema_version": schema_version,
        "error_type": error_type,
        "collector": collector or "unknown",
        "symbol": symbol or "unknown",
        "source": source or "unknown",
        "topic": topic or "unknown",
        "original_topic": original_topic or topic or "unknown",
        "key": key,
        "reason": safe_reason,
        "occurred_at": occurred_at or time.time(),
        "payload_preview": payload_preview(payload),
        "original_payload_size": original_payload_size(payload),
    }


def write_bad_row_fallback(context: dict[str, Any], *, path: Path | None = None) -> Path:
    target = path or default_bad_row_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    safe_context = {str(key): _json_safe(value) for key, value in context.items()}
    with target.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(safe_context, ensure_ascii=False, sort_keys=True, default=repr) + "\n")
    return target


def record_bad_row_metric(
    *,
    collector: str | None,
    symbol: str | None,
    source: str | None,
    topic: str | None,
    error_type: str,
) -> None:
    try:
        from common.metrics import (
            collector_bad_rows_total,
            collector_json_serialization_failures_total,
            collector_schema_validation_failures_total,
        )

        labels = {
            "collector": collector or "unknown",
            "symbol": symbol or "unknown",
            "source": source or "unknown",
            "topic": topic or "unknown",
            "error_type": error_type,
        }
        collector_bad_rows_total.labels(**labels).inc()
        if error_type.startswith("schema_"):
            collector_schema_validation_failures_total.labels(**labels).inc()
        if "serialization" in error_type:
            collector_json_serialization_failures_total.labels(**labels).inc()
    except Exception:
        return


def record_dlq_send_failure_metric(
    *,
    collector: str | None,
    symbol: str | None,
    source: str | None,
    topic: str | None,
    error_type: str,
) -> None:
    try:
        from common.metrics import collector_dlq_send_failures_total

        collector_dlq_send_failures_total.labels(
            collector=collector or "unknown",
            symbol=symbol or "unknown",
            source=source or "unknown",
            topic=topic or "unknown",
            error_type=error_type,
        ).inc()
    except Exception:
        return
