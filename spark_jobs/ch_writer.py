# spark_jobs/ch_writer.py
"""
ClickHouse foreachBatch 핸들러.
Spark Structured Streaming의 foreachBatch에서 호출하여
배치 데이터를 ClickHouse에 저장한다.

clickhouse-connect (HTTP API) 사용.
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from pathlib import Path

import clickhouse_connect
import numpy as np
import pandas as pd
from batch_ledger_contract import (
    LEDGER_CONTRACT_VERSION,
    SHADOW_LEDGER_EVENT_TYPE,
    LedgerContractError,
    audit_event_to_ledger_row,
    build_ledger_key,
)
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Load CH credentials from centralized module at import time.
# Spark executors may have a different sys.path, so we resolve once and
# store as plain strings — no repeated imports needed at runtime.
try:
    from common.ch_credentials import get_ch_credentials as _get_ch_credentials

    _creds = _get_ch_credentials()
    CH_HOST = _creds.host
    CH_PORT = _creds.port
    CH_USER = _creds.username
    CH_PASSWORD = _creds.password
except Exception:  # fallback: read env vars directly (Spark executor path)

    def _get_required_env(key: str) -> str:
        value = os.getenv(key)
        if not value:
            raise RuntimeError(f"required environment variable is missing: {key}")
        return value

    CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CH_USER = os.getenv("CLICKHOUSE_USER", "coinwhale")
    CH_PASSWORD = _get_required_env("CLICKHOUSE_PASSWORD")

FAIL_FAST_ENABLED = os.getenv("CH_WRITER_FAIL_FAST", "0") == "1"
FAIL_FAST_CONSECUTIVE = max(1, int(os.getenv("CH_WRITER_FAIL_FAST_CONSECUTIVE", "3")))
FAIL_FAST_ERROR_TYPES_RAW = os.getenv("CH_WRITER_FAIL_FAST_ERROR_TYPES", "*")
FAIL_FAST_ERROR_TYPES = {item.strip() for item in FAIL_FAST_ERROR_TYPES_RAW.split(",") if item.strip()}
RAISE_AFTER_DLQ_ENABLED = os.getenv("CH_WRITER_RAISE_AFTER_DLQ", "0") == "1"
ASYNC_INSERT_ENABLED = os.getenv("CH_ASYNC_INSERT", "0") == "1"
ASYNC_INSERT_WAIT = os.getenv("CH_WAIT_FOR_ASYNC_INSERT", "1") != "0"
ASYNC_INSERT_BUSY_TIMEOUT_MS = max(50, int(os.getenv("CH_ASYNC_INSERT_BUSY_TIMEOUT_MS", "1000")))

# DLQ cleanup settings
DLQ_DIR = os.getenv("DLQ_DIR", "/opt/spark/work-dir/dlq")
DLQ_MAX_AGE_HOURS = max(1, int(os.getenv("DLQ_MAX_AGE_HOURS", "168")))  # 7 days default
DLQ_MAX_FILES = max(10, int(os.getenv("DLQ_MAX_FILES", "1000")))
DLQ_CLEANUP_ENABLED = os.getenv("DLQ_CLEANUP_ENABLED", "1") == "1"
_DLQ_CLEANUP_LOCK = threading.Lock()

_client = None
_client_lock = threading.RLock()
_client_last_healthcheck = 0.0
CH_CLIENT_PING_INTERVAL_SEC = max(
    10,
    int(os.getenv("CH_CLIENT_PING_INTERVAL_SEC", "60")),
)
_failure_counts = {}
_failure_lock = threading.Lock()

# Track last cleanup time per-process to avoid frequent cleanup checks
_last_cleanup_at = 0.0
_cleanup_interval_sec = 3600  # 1 hour between cleanup runs
_AUDIT_EVENT_TYPE = "spark_clickhouse_write_audit"
_SHADOW_LEDGER_FAILED_EVENT_TYPE = "spark_batch_ledger_shadow_failed"
_QUALITY_THRESHOLD_POLICY_VERSION = "spark-write-quality-v1"
_QUALITY_NULL_RATE_THRESHOLD = 0.30
_QUALITY_NAN_RATE_THRESHOLD = 0.30
_CRITICAL_COLUMNS_BY_TABLE = {
    "stream.price": ("symbol", "ts"),
    "stream.oi": ("symbol", "ts", "open_interest"),
    "stream.funding": ("symbol", "ts", "funding_rate", "mark_price"),
    "stream.cvd": ("symbol", "ts", "futures_cvd_delta", "spot_cvd_delta"),
    "stream.liquidation": ("symbol", "ts"),
    "stream.market_metrics": ("symbol", "ts"),
    "stream.ls_ratio": ("symbol", "ts", "ratio_type", "ls_ratio"),
}
_TIME_COLUMNS = (
    "ts",
    "event_time",
    "event_ts",
    "timestamp",
    "window_start",
    "window_end",
)
_CHECKSUM_COLUMNS = (
    "symbol",
    "ts",
    "event_time",
    "window_start",
    "window_end",
    "ratio_type",
    "market",
    "mark_price",
    "open_interest",
    "funding_rate",
    "futures_spread",
    "spot_spread",
    "futures_cvd_delta",
    "spot_cvd_delta",
    "liq_long_usd",
    "liq_short_usd",
)
_SENSITIVE_LOG_KEYS = (
    "password",
    "token",
    "secret",
    "api_key",
    "apikey",
    "private_key",
)


def _cleanup_old_dlq_files() -> int:
    """Remove DLQ files older than DLQ_MAX_AGE_HOURS or exceeding DLQ_MAX_FILES.

    Returns:
        Number of files deleted.
    """
    if not DLQ_CLEANUP_ENABLED:
        return 0

    dlq_path = Path(DLQ_DIR)
    if not dlq_path.exists():
        return 0

    now = time.time()
    max_age = DLQ_MAX_AGE_HOURS * 3600
    cutoff = now - max_age

    deleted = 0
    json_files = sorted(dlq_path.glob("ch_failure_*.json"), key=lambda p: p.stat().st_mtime)

    # Delete by age
    for f in json_files:
        if f.stat().st_mtime < cutoff:
            try:
                f.unlink()
                # Also delete associated _data.jsonl if exists
                data_file = f.with_name(f.name.replace(".json", "_data.jsonl"))
                if data_file.exists():
                    data_file.unlink()
                deleted += 1
            except OSError:
                pass

    # If still over limit, delete oldest files
    if deleted == 0 and len(json_files) >= DLQ_MAX_FILES:
        for f in json_files[: len(json_files) - DLQ_MAX_FILES + 1]:
            try:
                f.unlink()
                data_file = f.with_name(f.name.replace(".json", "_data.jsonl"))
                if data_file.exists():
                    data_file.unlink()
                deleted += 1
            except OSError:
                pass

    return deleted


def _new_client():
    kwargs = dict(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        # Inserts do not use temporary tables or session-scoped SET values.
        # Disabling generated sessions keeps a shared driver client safe if
        # two callback paths ever overlap.
        autogenerate_session_id=False,
    )
    if CH_PASSWORD:
        kwargs["password"] = CH_PASSWORD
    if ASYNC_INSERT_ENABLED:
        kwargs["settings"] = {
            "async_insert": 1,
            "wait_for_async_insert": 1 if ASYNC_INSERT_WAIT else 0,
            "async_insert_busy_timeout_ms": ASYNC_INSERT_BUSY_TIMEOUT_MS,
        }
    return clickhouse_connect.get_client(**kwargs)


def _discard_client() -> None:
    global _client, _client_last_healthcheck
    with _client_lock:
        stale_client = _client
        _client = None
        _client_last_healthcheck = 0.0
        if stale_client is not None:
            try:
                stale_client.close()
            except Exception:
                pass


def get_client():
    """Return one ClickHouse client per long-lived Spark driver process.

    PySpark may invoke successive foreachBatch callbacks on different Python
    threads. A thread-local cache therefore recreated the client (and its
    expensive server-capability queries) for nearly every micro-batch.
    """
    global _client, _client_last_healthcheck
    now = time.monotonic()
    with _client_lock:
        if _client is not None:
            if now - _client_last_healthcheck < CH_CLIENT_PING_INTERVAL_SEC:
                return _client
            try:
                _client.ping()
                _client_last_healthcheck = now
                return _client
            except Exception:
                try:
                    _client.close()
                except Exception:
                    pass
                _client = None

        _client = _new_client()
        _client_last_healthcheck = now
        return _client


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _short_error_message(error: Exception | str | None, max_len: int = 300) -> str | None:
    if error is None:
        return None
    message = str(error)
    for key in _SENSITIVE_LOG_KEYS:
        message = re.sub(
            rf"({re.escape(key)}\s*=\s*)[^\s&]+",
            rf"\1***",
            message,
            flags=re.IGNORECASE,
        )
    message = re.sub(r"://([^:/@\s]+):([^@\s]+)@", r"://\1:***@", message)
    for key in _SENSITIVE_LOG_KEYS:
        message = re.sub(re.escape(key), f"{key[:2]}***", message, flags=re.IGNORECASE)
    if len(message) <= max_len:
        return message
    return message[: max_len - 3] + "..."


def _sanitize_audit_metadata(value: str | None, max_len: int = 500) -> str | None:
    if value is None:
        return None
    sanitized = _short_error_message(str(value).strip(), max_len=max_len)
    return sanitized or None


def _resolve_run_id(run_id: str | None) -> str | None:
    return _sanitize_audit_metadata(run_id or os.getenv("SPARK_RUN_ID"))


def _ledger_key_metadata(
    *,
    job_name: str,
    output_table: str,
    checkpoint_path: str | None,
    batch_id,
) -> dict:
    if not checkpoint_path:
        return {"ledger_key": None, "ledger_key_status": "missing_metadata"}
    try:
        return {
            "ledger_key": build_ledger_key(
                job_name=job_name,
                output_table=output_table,
                checkpoint_path=checkpoint_path,
                batch_id=batch_id,
            ),
            "ledger_key_status": "generated",
        }
    except LedgerContractError:
        return {"ledger_key": None, "ledger_key_status": "invalid_metadata"}


def build_shadow_ledger_event(audit_event: dict) -> dict:
    ledger_row = audit_event_to_ledger_row(audit_event)
    return {
        "event_type": SHADOW_LEDGER_EVENT_TYPE,
        "ledger_contract_version": ledger_row.get("ledger_contract_version", LEDGER_CONTRACT_VERSION),
        "ledger_key": ledger_row.get("ledger_key"),
        "ledger_key_status": ledger_row.get("ledger_key_status"),
        "job_name": ledger_row.get("job_name"),
        "output_table": ledger_row.get("output_table"),
        "checkpoint_path": ledger_row.get("checkpoint_path"),
        "query_name": ledger_row.get("query_name"),
        "run_id": ledger_row.get("run_id"),
        "attempt_id": ledger_row.get("attempt_id"),
        "batch_id": ledger_row.get("batch_id"),
        "status": ledger_row.get("status"),
        "row_count": ledger_row.get("row_count"),
        "checksum": ledger_row.get("checksum"),
        "null_count": ledger_row.get("null_count"),
        "nan_count": ledger_row.get("nan_count"),
        "warning_count": ledger_row.get("warning_count"),
        "quality_warnings": (
            audit_event.get("quality_warnings") if isinstance(audit_event.get("quality_warnings"), list) else []
        ),
        "quality_warning_types": ledger_row.get("quality_warning_types"),
        "threshold_policy_version": ledger_row.get("threshold_policy_version"),
        "duration_ms": ledger_row.get("duration_ms"),
        "error_type": ledger_row.get("error_type"),
        "error_message_short": ledger_row.get("error_message_short"),
        "occurred_at": ledger_row.get("observed_at"),
    }


def _build_shadow_ledger_failed_event(audit_event: dict, error: Exception) -> dict:
    return {
        "event_type": _SHADOW_LEDGER_FAILED_EVENT_TYPE,
        "ledger_contract_version": LEDGER_CONTRACT_VERSION,
        "ledger_key": audit_event.get("ledger_key"),
        "ledger_key_status": audit_event.get("ledger_key_status") or "shadow_failed",
        "job_name": audit_event.get("job_name"),
        "output_table": audit_event.get("output_table"),
        "checkpoint_path": audit_event.get("checkpoint_path"),
        "query_name": audit_event.get("query_name"),
        "run_id": audit_event.get("run_id"),
        "attempt_id": audit_event.get("attempt_id"),
        "batch_id": audit_event.get("batch_id"),
        "status": audit_event.get("status"),
        "row_count": audit_event.get("row_count"),
        "checksum": audit_event.get("checksum"),
        "error_type": error.__class__.__name__,
        "error_message_short": _short_error_message(error),
        "occurred_at": audit_event.get("occurred_at") or _now_iso(),
    }


def log_shadow_ledger_event(audit_event: dict) -> dict | None:
    try:
        event = build_shadow_ledger_event(audit_event)
        if os.getenv("SPARK_SHADOW_LEDGER_STDOUT", "0") == "1":
            print(json.dumps(event, sort_keys=True, default=str, ensure_ascii=False))
        return event
    except Exception as exc:
        try:
            failed_event = _build_shadow_ledger_failed_event(audit_event, exc)
            if os.getenv("SPARK_SHADOW_LEDGER_STDOUT", "0") == "1":
                print(json.dumps(failed_event, sort_keys=True, default=str, ensure_ascii=False))
            return failed_event
        except Exception:
            return None


def _is_nan_value(value) -> bool:
    return isinstance(value, (float, np.floating)) and math.isnan(float(value))


def _null_nan_summary(pdf: pd.DataFrame | None) -> dict:
    if pdf is None or pdf.empty:
        return {
            "null_count": 0,
            "nan_count": 0,
            "null_columns": {},
            "nan_columns": {},
        }

    null_columns: dict[str, int] = {}
    nan_columns: dict[str, int] = {}
    for column in pdf.columns:
        series = pdf[column]
        null_count = int(series.isna().sum())
        if null_count:
            null_columns[str(column)] = null_count

        nan_count = 0
        if pd.api.types.is_numeric_dtype(series):
            nan_count = int(series.map(_is_nan_value).sum())
        if nan_count:
            nan_columns[str(column)] = nan_count

    return {
        "null_count": int(sum(null_columns.values())),
        "nan_count": int(sum(nan_columns.values())),
        "null_columns": null_columns,
        "nan_columns": nan_columns,
    }


def _critical_columns_for_table(output_table: str | None) -> tuple[str, ...]:
    if not output_table:
        return ()
    return _CRITICAL_COLUMNS_BY_TABLE.get(output_table, ())


def evaluate_write_quality(
    *,
    output_table: str,
    row_count: int,
    checksum: str | None,
    null_columns: dict[str, int],
    nan_columns: dict[str, int],
    columns: list[str] | tuple[str, ...],
) -> dict:
    """Return warn-only write quality metadata for audit logs.

    This intentionally never raises and never inspects row values. It only uses
    aggregate counts that are already safe for audit logging.
    """
    warnings: list[dict] = []
    column_set = {str(column) for column in columns}
    critical_columns = _critical_columns_for_table(output_table)
    critical_set = set(critical_columns)
    critical_columns_checked: list[str] = []

    if row_count > 0:
        for column in critical_columns:
            if column not in column_set:
                warnings.append(
                    {
                        "type": "missing_critical_column",
                        "column": column,
                    }
                )
                continue

            critical_columns_checked.append(column)
            null_count = int(null_columns.get(column, 0))
            if null_count > 0:
                warnings.append(
                    {
                        "type": "critical_nulls",
                        "column": column,
                        "count": null_count,
                        "rate": null_count / row_count,
                    }
                )

        for column, count_raw in sorted(null_columns.items()):
            if column in critical_set:
                continue
            count = int(count_raw)
            rate = count / row_count
            if rate >= _QUALITY_NULL_RATE_THRESHOLD:
                warnings.append(
                    {
                        "type": "high_null_rate",
                        "column": column,
                        "count": count,
                        "rate": rate,
                        "threshold": _QUALITY_NULL_RATE_THRESHOLD,
                    }
                )

        for column, count_raw in sorted(nan_columns.items()):
            if column in critical_set:
                continue
            count = int(count_raw)
            rate = count / row_count
            if rate >= _QUALITY_NAN_RATE_THRESHOLD:
                warnings.append(
                    {
                        "type": "high_nan_rate",
                        "column": column,
                        "count": count,
                        "rate": rate,
                        "threshold": _QUALITY_NAN_RATE_THRESHOLD,
                    }
                )

        if not checksum:
            warnings.append({"type": "missing_checksum"})

        if not any(column in column_set for column in _TIME_COLUMNS):
            warnings.append(
                {
                    "type": "missing_time_column",
                    "time_columns_considered": list(_TIME_COLUMNS),
                }
            )

    return {
        "quality_warnings": warnings,
        "warning_count": len(warnings),
        "critical_columns_checked": critical_columns_checked,
        "threshold_policy_version": _QUALITY_THRESHOLD_POLICY_VERSION,
    }


def compute_batch_checksum(pdf: pd.DataFrame | None) -> str | None:
    """Return a deterministic, order-insensitive audit checksum for a pandas batch."""
    if pdf is None or pdf.empty:
        return None

    selected_columns = [column for column in _CHECKSUM_COLUMNS if column in pdf.columns]
    if not selected_columns:
        selected_columns = sorted(pdf.columns, key=str)

    normalized = pdf[selected_columns].copy()
    for column in selected_columns:
        normalized[column] = normalized[column].map(lambda value: "<NA>" if pd.isna(value) else str(value))
    normalized = normalized.sort_values(selected_columns, kind="mergesort").reset_index(drop=True)
    row_hashes = pd.util.hash_pandas_object(normalized, index=False).astype("uint64")
    checksum = int(row_hashes.sum()) & ((1 << 64) - 1)
    return f"{checksum:016x}"


def build_write_audit_event(
    *,
    batch_id,
    output_table: str,
    job_name: str | None = None,
    checkpoint_path: str | None = None,
    query_name: str | None = None,
    run_id: str | None = None,
    attempt_id: str | None = None,
    pdf: pd.DataFrame | None = None,
    status: str,
    duration_ms: int | None = None,
    error_type: str | None = None,
    error: Exception | str | None = None,
) -> dict:
    summary = _null_nan_summary(pdf)
    row_count = int(len(pdf)) if pdf is not None else 0
    checksum = compute_batch_checksum(pdf)
    resolved_job_name = job_name or output_table
    sanitized_checkpoint_path = _sanitize_audit_metadata(checkpoint_path)
    sanitized_query_name = _sanitize_audit_metadata(query_name or resolved_job_name)
    sanitized_run_id = _resolve_run_id(run_id)
    sanitized_attempt_id = _sanitize_audit_metadata(attempt_id)
    ledger = _ledger_key_metadata(
        job_name=resolved_job_name,
        output_table=output_table,
        checkpoint_path=sanitized_checkpoint_path,
        batch_id=batch_id,
    )
    quality = evaluate_write_quality(
        output_table=output_table,
        row_count=row_count,
        checksum=checksum,
        null_columns=summary["null_columns"],
        nan_columns=summary["nan_columns"],
        columns=list(pdf.columns) if pdf is not None else [],
    )
    return {
        "event_type": _AUDIT_EVENT_TYPE,
        "job_name": resolved_job_name,
        "batch_id": batch_id,
        "output_table": output_table,
        "checkpoint_path": sanitized_checkpoint_path,
        "query_name": sanitized_query_name,
        "run_id": sanitized_run_id,
        "attempt_id": sanitized_attempt_id,
        "ledger_key": ledger["ledger_key"],
        "ledger_key_status": ledger["ledger_key_status"],
        "row_count": row_count,
        "checksum": checksum,
        "null_count": summary["null_count"],
        "nan_count": summary["nan_count"],
        "null_columns": summary["null_columns"],
        "nan_columns": summary["nan_columns"],
        "quality_warnings": quality["quality_warnings"],
        "warning_count": quality["warning_count"],
        "critical_columns_checked": quality["critical_columns_checked"],
        "threshold_policy_version": quality["threshold_policy_version"],
        "duration_ms": duration_ms,
        "status": status,
        "error_type": error_type,
        "error_message_short": _short_error_message(error),
        "occurred_at": _now_iso(),
    }


def log_write_audit_event(**kwargs) -> dict:
    event = build_write_audit_event(**kwargs)
    if os.getenv("SPARK_WRITE_AUDIT_STDOUT", "0") == "1":
        print(json.dumps(event, sort_keys=True, default=str, ensure_ascii=False))
    log_shadow_ledger_event(event)
    return event


def write_to_clickhouse(
    batch_df,
    batch_id,
    table_name,
    original_topic=None,
    job_name=None,
    checkpoint_path=None,
    query_name=None,
    run_id=None,
    attempt_id=None,
):
    """
    foreachBatch 콜백용 ClickHouse 저장 함수.

    pandas 2.x에서 TimestampType → datetime64 단위 미지정 에러를 피하기 위해
    timestamp 컬럼을 문자열로 변환 후 toPandas(), 이후 다시 datetime으로 변환.

    저장 실패 시 DLQ로 전송 (파일 저장 후 별도 프로세스가 DLQ 토픽으로 전송).

    Args:
        batch_df: 저장할 DataFrame
        batch_id: Spark 배치 ID
        table_name: ClickHouse 테이블 이름 (예: "stream.cvd")
        original_topic: 원본 Kafka 토픽 (DLQ 메시지에 포함, optional)
    """
    pdf = None
    started_at = time.monotonic()
    try:
        # TimestampType 컬럼을 string으로 캐스팅 (pandas 2.x 호환)
        ts_cols = [f.name for f in batch_df.schema.fields if isinstance(f.dataType, TimestampType)]
        df = batch_df
        for c in ts_cols:
            df = df.withColumn(c, F.col(c).cast("string"))

        # count() 대신 toPandas() 후 len() — Spark Action 1회로 줄여 재연산 방지
        pdf = df.toPandas()
        if len(pdf) == 0:
            log_write_audit_event(
                job_name=job_name,
                batch_id=batch_id,
                output_table=table_name,
                checkpoint_path=checkpoint_path,
                query_name=query_name,
                run_id=run_id,
                attempt_id=attempt_id,
                pdf=pdf,
                status="skipped_empty",
                duration_ms=int((time.monotonic() - started_at) * 1000),
            )
            return

        # string → datetime 복원 (ClickHouse DateTime 컬럼에 맞게)
        for c in ts_cols:
            pdf[c] = pd.to_datetime(pdf[c])

        log_write_audit_event(
            job_name=job_name,
            batch_id=batch_id,
            output_table=table_name,
            checkpoint_path=checkpoint_path,
            query_name=query_name,
            run_id=run_id,
            attempt_id=attempt_id,
            pdf=pdf,
            status="started",
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )
        # A clickhouse-connect client session must not be used concurrently.
        # foreachBatch is normally serial, while the lock also protects future
        # multi-query driver layouts and the periodic health check above.
        with _client_lock:
            client = get_client()
            client.insert_df(table_name, pdf)
        log_write_audit_event(
            job_name=job_name,
            batch_id=batch_id,
            output_table=table_name,
            checkpoint_path=checkpoint_path,
            query_name=query_name,
            run_id=run_id,
            attempt_id=attempt_id,
            pdf=pdf,
            status="success",
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )
        _reset_failure_count(table_name)

    except Exception as e:
        error_type = _classify_ch_error(e)
        if error_type in {"ch_connection_error", "ch_timeout"}:
            _discard_client()
        error_message = str(e)
        row_count = len(pdf) if pdf is not None else 0  # toPandas 전 실패 시 0
        consecutive_failures = _record_failure(table_name)
        log_write_audit_event(
            job_name=job_name,
            batch_id=batch_id,
            output_table=table_name,
            checkpoint_path=checkpoint_path,
            query_name=query_name,
            run_id=run_id,
            attempt_id=attempt_id,
            pdf=pdf,
            status="failed",
            duration_ms=int((time.monotonic() - started_at) * 1000),
            error_type=error_type,
            error=e,
        )

        if _is_spark_operational_error(error_type):
            print(
                f"[SPARK-OPS] Spark batch action failed - table={table_name}, batch={batch_id}, "
                f"rows={row_count}, consecutive_failures={consecutive_failures}, "
                f"error={error_type}: {error_message}"
            )
            raise

        # ClickHouse 저장 실패 시 DLQ 처리

        print(
            f"[DLQ-CH] 저장 실패 - table={table_name}, batch={batch_id}, "
            f"rows={row_count}, consecutive_failures={consecutive_failures}, "
            f"error={error_type}: {error_message}"
        )

        try:
            # 실패한 배치를 파일로 저장 (나중에 DLQ로 재전송)
            os.makedirs(DLQ_DIR, exist_ok=True)

            # DLQ 메타데이터
            dlq_metadata = {
                "original_topic": original_topic or "unknown",
                "target_table": table_name,
                "batch_id": batch_id,
                "error_type": error_type,
                "error_message": error_message,
                "timestamp": time.time(),
                "row_count": row_count,
            }

            # 메타데이터 저장
            metadata_file = os.path.join(
                DLQ_DIR,
                f"ch_failure_{table_name.replace('.', '_')}_{batch_id}_{int(time.time())}.json",
            )
            with open(metadata_file, "w") as f:
                json.dump(dlq_metadata, f, indent=2)

            # 실패한 배치 데이터 저장 (JSON Lines 형식)
            data_file = metadata_file.replace(".json", "_data.jsonl")
            if pdf is not None:
                pdf_dict = pdf.to_dict(orient="records")
                with open(data_file, "w") as f:
                    for record in pdf_dict:
                        # datetime을 문자열로 변환
                        for k, v in record.items():
                            if pd.isna(v):
                                record[k] = None
                            elif isinstance(v, pd.Timestamp):
                                record[k] = v.isoformat()
                        f.write(json.dumps(record) + "\n")
                print(f"[DLQ-CH] 실패 배치 저장 완료 - metadata={metadata_file}, data={data_file}")
            else:
                print(f"[DLQ-CH] pdf 없음 (toPandas 전 실패) - metadata만 저장")
                print(f"[DLQ-CH] 실패 배치 저장 완료 - metadata={metadata_file}")
            dlq_persisted = True

            # DLQ 파일 정리 (성공 후 throttled cleanup)
            global _last_cleanup_at
            if time.time() - _last_cleanup_at >= _cleanup_interval_sec:
                with _DLQ_CLEANUP_LOCK:
                    if time.time() - _last_cleanup_at >= _cleanup_interval_sec:
                        _last_cleanup_at = time.time()
                        deleted = _cleanup_old_dlq_files()
                        if deleted > 0:
                            print(f"[DLQ-CH] cleanup: {deleted} old files removed")

            # DLQ 메트릭 업데이트 (가능한 경우)
            try:
                # Spark 환경에서는 Prometheus 메트릭 직접 업데이트 어려움
                # 별도 메트릭 수집 스크립트가 필요
                pass
            except Exception as metric_error:
                print(f"[DLQ-CH] 메트릭 업데이트 실패 (무시): {metric_error}")

        except Exception as dlq_error:
            print(f"[DLQ-CH] DLQ 저장도 실패: {dlq_error}. 배치 데이터 손실 가능성!")
            # 최소한 에러 로그는 남김
            import traceback

            print(f"[DLQ-CH] Traceback:\n{traceback.format_exc()}")

        if _should_raise_after_dlq(error_type, dlq_persisted):
            print(
                f"[DLQ-CH] strict retry 모드 - table={table_name}, batch={batch_id}, "
                f"dlq_persisted={dlq_persisted}, checkpoint advance 방지를 위해 예외 재전파"
            )
            raise

        if _should_fail_fast(error_type, consecutive_failures):
            print(
                f"[DLQ-CH] fail-fast 발동 - table={table_name}, batch={batch_id}, "
                f"consecutive_failures={consecutive_failures}, threshold={FAIL_FAST_CONSECUTIVE}"
            )
            raise


def _classify_ch_error(exception: Exception) -> str:
    """ClickHouse/Spark 에러 타입 분류"""
    error_str = str(exception).lower()
    checkpoint_markers = (
        "hdfsbackedstatestoreprovider",
        "state store",
        "statestore",
        "checkpointfilemanager",
        "hdfsmetadatalog",
        "offsetseqlog",
        "/checkpoints/",
        ".delta does not exist",
        ".tmp.crc does not exist",
        "error reading delta file",
    )
    if any(marker in error_str for marker in checkpoint_markers):
        return "spark_checkpoint_error"
    if "sparkcontext" in error_str or "sparkexception" in error_str:
        return "spark_action_error"
    if "collecttopython" in error_str and ("org.apache.spark" in error_str or "py4j" in error_str):
        return "spark_action_error"
    if "connection" in error_str or "connect" in error_str or "refused" in error_str:
        return "ch_connection_error"
    elif "timeout" in error_str or "timed out" in error_str:
        return "ch_timeout"
    elif "schema" in error_str or "column" in error_str or "type" in error_str:
        return "ch_schema_error"
    elif "duplicate" in error_str or "unique" in error_str:
        return "ch_constraint_error"
    elif "memory" in error_str or "out of memory" in error_str:
        return "ch_memory_error"
    else:
        return "ch_unknown_error"


def _is_spark_operational_error(error_type: str) -> bool:
    return error_type.startswith("spark_")


def _record_failure(table_name: str) -> int:
    with _failure_lock:
        count = _failure_counts.get(table_name, 0) + 1
        _failure_counts[table_name] = count
        return count


def _reset_failure_count(table_name: str) -> None:
    with _failure_lock:
        previous = _failure_counts.pop(table_name, 0)
    if previous:
        print(f"[DLQ-CH] 저장 복구 - table={table_name}, previous_consecutive_failures={previous}")


def _should_fail_fast(error_type: str, consecutive_failures: int) -> bool:
    if not FAIL_FAST_ENABLED:
        return False
    if consecutive_failures < FAIL_FAST_CONSECUTIVE:
        return False
    return "*" in FAIL_FAST_ERROR_TYPES or error_type in FAIL_FAST_ERROR_TYPES


def _should_raise_after_dlq(error_type: str, _dlq_persisted: bool) -> bool:
    if not RAISE_AFTER_DLQ_ENABLED:
        return False
    return not _is_spark_operational_error(error_type)
