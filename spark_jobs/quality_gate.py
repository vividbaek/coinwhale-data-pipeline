"""Spark data quality gate for Silver streaming inputs.

The gate is intentionally expression-only so it can run before aggregation in
Structured Streaming jobs. Invalid rows are quarantined in shadow mode and are
never fed into Silver aggregates.
"""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_GATE_MODES = {"shadow", "observe", "strict"}
DEFAULT_GATE_MODE = "shadow"
DEFAULT_LATE_EVENT_MS = 10 * 60 * 1000
DEFAULT_FUTURE_EVENT_MS = 60 * 1000
QUALITY_EVENT_TABLE = "stream.pipeline_quality_events"
QUALITY_FALLBACK_DIR = "/opt/spark/work-dir/dlq/quarantine/pipeline_quality_events"
QUARANTINE_ROOT = "/opt/spark/work-dir/dlq/quarantine/data_quality"
QUALITY_EVENT_COLUMNS = [
    "pipeline",
    "stage",
    "topic",
    "symbol",
    "checkpoint",
    "batch_id",
    "schema_version",
    "input_rows",
    "parsed_rows",
    "dropped_rows",
    "null_critical_count",
    "parse_error_count",
    "event_lag_p95_ms",
    "event_lag_p99_ms",
    "write_rows",
    "checksum",
    "error_type",
    "sample_payload",
]
_quality_event_client = None
_quality_event_client_lock = threading.RLock()
_quality_event_client_last_healthcheck = 0.0
_QUALITY_EVENT_CLIENT_PING_INTERVAL_SEC = max(
    10,
    int(os.getenv("CH_CLIENT_PING_INTERVAL_SEC", "60")),
)
_QUALITY_ARTIFACT_CLEANUP_LOCK = threading.Lock()
_quality_artifact_last_cleanup_at = 0.0
_QUALITY_ARTIFACT_CLEANUP_INTERVAL_SEC = max(
    60,
    int(os.getenv("SPARK_DQ_CLEANUP_INTERVAL_SEC", "3600")),
)


def _path_token(value: str | None) -> str:
    text = str(value or "unknown")
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in text)[:120] or "unknown"


_QUALITY_EVENT_DEFAULTS: dict[str, Any] = {
    "pipeline": "",
    "stage": "",
    "topic": "",
    "symbol": "",
    "checkpoint": "",
    "batch_id": 0,
    "schema_version": 1,
    "input_rows": 0,
    "parsed_rows": 0,
    "dropped_rows": 0,
    "null_critical_count": 0,
    "parse_error_count": 0,
    "event_lag_p95_ms": 0,
    "event_lag_p99_ms": 0,
    "write_rows": 0,
    "checksum": "",
    "error_type": "",
    "sample_payload": "",
}


def _quality_event_value(event: dict[str, Any], column: str) -> Any:
    value = event.get(column)
    if value is None:
        value = _QUALITY_EVENT_DEFAULTS.get(column, "")
    if column in {"event_lag_p95_ms", "event_lag_p99_ms"}:
        return max(0, int(value or 0))
    return value


HOT_PATH_TOPICS = {
    "binance-trade",
    "spot-trade",
    "binance-bookticker",
    "spot-bookticker",
    "binance-markprice",
    "binance-openinterest",
    "rollup-trade-cvd-1s",
    "rollup-bookticker-1s",
    "binance-ticker",
}

CRITICAL_COLUMNS_BY_TOPIC = {
    "binance-trade": ("symbol", "price", "quantity", "event_time_sec"),
    "spot-trade": ("symbol", "price", "quantity", "event_time_sec"),
    "binance-bookticker": ("symbol", "bid_price", "ask_price", "event_time_sec"),
    "spot-bookticker": ("symbol", "bid_price", "ask_price", "event_time_sec"),
    "binance-markprice": (
        "symbol",
        "mark_price",
        "funding_rate",
        "index_price",
        "event_time_sec",
    ),
    "binance-openinterest": ("symbol", "open_interest", "event_time_sec"),
    "rollup-trade-cvd-1s": ("symbol", "event_time_sec"),
    "rollup-bookticker-1s": ("symbol", "bid_price", "ask_price", "event_time_sec"),
    "binance-ticker": ("symbol", "event_time_sec"),
}

NON_NEGATIVE_COLUMNS_BY_TOPIC = {
    "binance-trade": ("price", "quantity"),
    "spot-trade": ("price", "quantity"),
    "binance-bookticker": ("bid_price", "ask_price"),
    "spot-bookticker": ("bid_price", "ask_price"),
    "binance-markprice": ("mark_price", "index_price"),
    "binance-openinterest": ("open_interest",),
    "rollup-trade-cvd-1s": (
        "futures_taker_buy_qty",
        "futures_taker_sell_qty",
        "futures_trade_count",
        "spot_taker_buy_qty",
        "spot_taker_sell_qty",
        "spot_trade_count",
        "whale_buy_count",
        "whale_sell_count",
        "whale_buy_qty",
        "whale_sell_qty",
    ),
    "rollup-bookticker-1s": ("bid_price", "ask_price", "message_count"),
}


def gate_mode() -> str:
    mode = os.getenv("SPARK_DQ_GATE_MODE", DEFAULT_GATE_MODE).strip().lower()
    if mode not in VALID_GATE_MODES:
        print(f"[DQ] unsupported SPARK_DQ_GATE_MODE={mode!r}; falling back to {DEFAULT_GATE_MODE}")
        return DEFAULT_GATE_MODE
    return mode


def strict_max_invalid_ratio() -> float | None:
    raw = os.getenv("SPARK_DQ_STRICT_MAX_INVALID_RATIO")
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except ValueError:
        print(f"[DQ] invalid SPARK_DQ_STRICT_MAX_INVALID_RATIO={raw!r}; strict failure disabled")
        return None


def is_hot_path_topic(topic: str | None) -> bool:
    if not topic:
        return False
    return topic in HOT_PATH_TOPICS


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"[DQ] invalid {name}={raw!r}; falling back to {default}")
        return default


def _existing_columns(df: DataFrame, names: tuple[str, ...]) -> tuple[str, ...]:
    columns = set(df.columns)
    return tuple(name for name in names if name in columns)


def _and_all(expressions: list[Any]) -> Any:
    if not expressions:
        return F.lit(True)
    result = expressions[0]
    for expr in expressions[1:]:
        result = result & expr
    return result


def _or_all(expressions: list[Any]) -> Any:
    if not expressions:
        return F.lit(False)
    result = expressions[0]
    for expr in expressions[1:]:
        result = result | expr
    return result


def _schema_version_to_int(value: Any) -> int:
    if value in (None, ""):
        return 1
    try:
        return int(value)
    except (TypeError, ValueError):
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        return int(digits) if digits else 1


def apply_quality_gate(
    df: DataFrame,
    *,
    topic: str,
    schema_version: str | None = None,
    now_ms: int | None = None,
) -> DataFrame:
    """Attach fixed DQ metadata columns and classify rows."""

    columns = set(df.columns)
    if now_ms is not None:
        now_col = F.lit(int(now_ms))
    else:
        now_col = (F.unix_timestamp(F.current_timestamp()) * F.lit(1000)).cast("long")

    gated = df
    if "_raw_value" not in columns:
        raw_col = (
            F.col("value").cast("string")
            if "value" in columns
            else F.to_json(F.struct(*[F.col(c) for c in df.columns]))
        )
        gated = gated.withColumn("_raw_value", raw_col)
        columns.add("_raw_value")
    if "_schema_version" not in columns:
        if schema_version is not None:
            schema_col = F.lit(schema_version)
        elif "schema_version" in columns:
            schema_col = F.col("schema_version").cast("string")
        else:
            schema_col = F.lit(None).cast("string")
        gated = gated.withColumn("_schema_version", schema_col)
        columns.add("_schema_version")
    if "_parse_status" not in columns:
        parsed_ok = F.lit(True)
        if "_j" in columns:
            parsed_ok = F.col("_j").isNotNull()
        gated = gated.withColumn(
            "_parse_status",
            F.when(parsed_ok, F.lit("ok")).otherwise(F.lit("parse_error")),
        )
        columns.add("_parse_status")
    if "_parse_error" not in columns:
        gated = gated.withColumn(
            "_parse_error",
            F.when(F.col("_parse_status") == F.lit("parse_error"), F.lit("malformed_json")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        columns.add("_parse_error")

    if "event_time_sec" in columns:
        event_ms = (F.col("event_time_sec").cast("double") * F.lit(1000.0)).cast("long")
    elif "event_time_ms" in columns:
        event_ms = F.col("event_time_ms").cast("long")
    else:
        event_ms = F.lit(None).cast("long")

    gated = gated.withColumn("_event_lag_ms", now_col - event_ms)

    critical_nulls = [
        F.col(name).isNull() for name in _existing_columns(gated, CRITICAL_COLUMNS_BY_TOPIC.get(topic, ()))
    ]
    non_negative_bad = [
        F.col(name).cast("double") < F.lit(0.0)
        for name in _existing_columns(gated, NON_NEGATIVE_COLUMNS_BY_TOPIC.get(topic, ()))
    ]
    bid_ask_bad = []
    if {"bid_price", "ask_price"}.issubset(set(gated.columns)):
        bid_ask_bad.append(F.col("bid_price").cast("double") > F.col("ask_price").cast("double"))

    late_ms = _int_env("SPARK_DQ_MAX_EVENT_LAG_MS", DEFAULT_LATE_EVENT_MS)
    future_ms = _int_env("SPARK_DQ_MAX_FUTURE_EVENT_MS", DEFAULT_FUTURE_EVENT_MS)
    lag_bad = event_ms.isNotNull() & (
        (F.col("_event_lag_ms") > F.lit(late_ms)) | (F.col("_event_lag_ms") < F.lit(-future_ms))
    )

    invalid_reasons = [
        (F.col("_parse_status") != F.lit("ok"), None),
        (_or_all(critical_nulls), "null_critical"),
        (_or_all(non_negative_bad), "negative_value"),
        (_or_all(bid_ask_bad), "bid_ask_inverted"),
        (lag_bad, "event_time_lag"),
    ]

    error_expr = F.col("_parse_error")
    for condition, reason in reversed(invalid_reasons):
        reason_expr = F.lit(reason) if reason is not None else F.coalesce(error_expr, F.lit("parse_error"))
        error_expr = F.when(condition, reason_expr).otherwise(error_expr)

    is_valid = _and_all([~condition for condition, _reason in invalid_reasons])
    return gated.withColumn("_parse_error", error_expr).withColumn("_dq_is_valid", is_valid)


def valid_rows(df: DataFrame) -> DataFrame:
    if "_dq_is_valid" not in df.columns:
        return df
    return df.filter(F.col("_dq_is_valid"))


def invalid_rows(df: DataFrame) -> DataFrame:
    if "_dq_is_valid" not in df.columns:
        return df.limit(0)
    return df.filter(~F.col("_dq_is_valid"))


def drop_quality_columns(df: DataFrame) -> DataFrame:
    quality_cols = {
        "_dq_is_valid",
        "_parse_status",
        "_parse_error",
        "_event_lag_ms",
        "_schema_version",
        "_raw_value",
        "_j",
    }
    return df.select(*[F.col(c) for c in df.columns if c not in quality_cols])


def build_quality_event(
    *,
    batch_df: DataFrame,
    output_df: DataFrame | None,
    pipeline: str,
    topic: str,
    checkpoint: str,
    batch_id: int,
    stage: str = "silver_pre_aggregate",
    write_rows: int | None = None,
) -> dict[str, Any]:
    """Collect a compact batch quality summary for ClickHouse/local fallback."""

    has_valid_flag = "_dq_is_valid" in batch_df.columns
    has_parse_status = "_parse_status" in batch_df.columns
    has_parse_error = "_parse_error" in batch_df.columns
    has_event_lag = "_event_lag_ms" in batch_df.columns
    has_schema_version = "_schema_version" in batch_df.columns
    has_symbol = "symbol" in batch_df.columns

    def count_if(condition: Any) -> Any:
        return F.coalesce(F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))), F.lit(0)).cast("long")

    summary = batch_df.agg(
        F.count(F.lit(1)).alias("input_rows"),
        count_if(~F.col("_dq_is_valid") if has_valid_flag else F.lit(False)).alias("dropped_rows"),
        count_if(F.col("_parse_status") == F.lit("parse_error") if has_parse_status else F.lit(False)).alias(
            "parse_error_count"
        ),
        count_if(F.col("_parse_error") == F.lit("null_critical") if has_parse_error else F.lit(False)).alias(
            "null_critical_count"
        ),
        (F.expr("percentile_approx(_event_lag_ms, 0.95)") if has_event_lag else F.max(F.lit(None).cast("long"))).alias(
            "event_lag_p95_ms"
        ),
        (F.expr("percentile_approx(_event_lag_ms, 0.99)") if has_event_lag else F.max(F.lit(None).cast("long"))).alias(
            "event_lag_p99_ms"
        ),
        (
            F.first(F.col("_schema_version"), ignorenulls=True)
            if has_schema_version
            else F.first(F.lit(None).cast("string"), ignorenulls=True)
        ).alias("schema_version"),
        (
            F.first(F.col("symbol"), ignorenulls=True)
            if has_symbol
            else F.first(F.lit(None).cast("string"), ignorenulls=True)
        ).alias("symbol"),
    ).collect()[0]

    input_rows = int(summary["input_rows"] or 0)
    dropped_rows = int(summary["dropped_rows"] or 0)
    parse_error_count = int(summary["parse_error_count"] or 0)
    parsed_rows = input_rows - parse_error_count
    null_critical_count = int(summary["null_critical_count"] or 0)

    sample_payload = None
    error_type = None
    if dropped_rows:
        invalid_df = invalid_rows(batch_df)
        sample = invalid_df.select("_parse_error", "_raw_value").limit(1).collect()
        if sample:
            error_type = sample[0]["_parse_error"]
            sample_payload = sample[0]["_raw_value"]

    if write_rows is None and output_df is not None:
        write_rows = output_df.count()

    return {
        "pipeline": pipeline,
        "stage": stage,
        "topic": topic,
        "symbol": summary["symbol"] or "",
        "checkpoint": checkpoint,
        "batch_id": int(batch_id),
        "schema_version": _schema_version_to_int(summary["schema_version"]),
        "input_rows": int(input_rows),
        "parsed_rows": int(parsed_rows),
        "dropped_rows": int(dropped_rows),
        "null_critical_count": int(null_critical_count),
        "parse_error_count": int(parse_error_count),
        "event_lag_p95_ms": (int(summary["event_lag_p95_ms"]) if summary["event_lag_p95_ms"] is not None else 0),
        "event_lag_p99_ms": (int(summary["event_lag_p99_ms"]) if summary["event_lag_p99_ms"] is not None else 0),
        "write_rows": int(write_rows or 0),
        "checksum": "",
        "error_type": error_type or "",
        "sample_payload": sample_payload or "",
    }


def _new_quality_event_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "coinwhale"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        autogenerate_session_id=False,
    )


def _get_quality_event_client():
    global _quality_event_client, _quality_event_client_last_healthcheck
    now = time.monotonic()
    with _quality_event_client_lock:
        if _quality_event_client is not None:
            if now - _quality_event_client_last_healthcheck < _QUALITY_EVENT_CLIENT_PING_INTERVAL_SEC:
                return _quality_event_client
            try:
                _quality_event_client.ping()
                _quality_event_client_last_healthcheck = now
                return _quality_event_client
            except Exception:
                try:
                    _quality_event_client.close()
                except Exception:
                    pass
                _quality_event_client = None

        _quality_event_client = _new_quality_event_client()
        _quality_event_client_last_healthcheck = now
        return _quality_event_client


def _discard_quality_event_client() -> None:
    global _quality_event_client, _quality_event_client_last_healthcheck
    with _quality_event_client_lock:
        stale_client = _quality_event_client
        _quality_event_client = None
        _quality_event_client_last_healthcheck = 0.0
        if stale_client is not None:
            try:
                stale_client.close()
            except Exception:
                pass


def cleanup_quality_artifacts(
    *,
    quarantine_root: str | Path | None = None,
    fallback_dir: str | Path | None = None,
    now_epoch: float | None = None,
    max_age_hours: int | None = None,
    max_fallback_files: int | None = None,
) -> dict[str, int]:
    """Bound DQ quarantine directories and quality-event fallback files.

    Quarantine paths are UTC day-partitioned, so whole expired days can be
    removed without walking every Spark part file. Recent evidence is retained
    and current-day directories are never partially pruned by age.
    """
    if os.getenv("SPARK_DQ_CLEANUP_ENABLED", "0") != "1":
        return {"expired_day_dirs": 0, "fallback_files": 0}

    now_value = time.time() if now_epoch is None else float(now_epoch)
    age_hours = max(
        1,
        int(
            max_age_hours
            if max_age_hours is not None
            else os.getenv("SPARK_DQ_QUARANTINE_MAX_AGE_HOURS", "168")
        ),
    )
    fallback_limit = (
        max(1, int(max_fallback_files))
        if max_fallback_files is not None
        else max(10, int(os.getenv("SPARK_DQ_FALLBACK_MAX_FILES", "10000")))
    )
    cutoff = datetime.fromtimestamp(now_value, timezone.utc) - timedelta(hours=age_hours)
    quarantine_path = Path(quarantine_root or os.getenv("SPARK_DQ_QUARANTINE_DIR") or QUARANTINE_ROOT)
    fallback_path = Path(fallback_dir or os.getenv("SPARK_DQ_QUALITY_EVENT_FALLBACK_DIR", QUALITY_FALLBACK_DIR))

    expired_day_dirs = 0
    if quarantine_path.exists():
        for day_dir in quarantine_path.glob("[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]"):
            try:
                day_start = datetime(
                    int(day_dir.parts[-3]),
                    int(day_dir.parts[-2]),
                    int(day_dir.parts[-1]),
                    tzinfo=timezone.utc,
                )
            except (TypeError, ValueError):
                continue
            if day_start + timedelta(days=1) <= cutoff:
                try:
                    shutil.rmtree(day_dir)
                    expired_day_dirs += 1
                except FileNotFoundError:
                    pass

    fallback_files_deleted = 0
    if fallback_path.exists():
        fallback_file_entries: list[tuple[float, Path]] = []
        for path in fallback_path.rglob("*.jsonl"):
            try:
                if path.is_file():
                    fallback_file_entries.append((path.stat().st_mtime, path))
            except FileNotFoundError:
                pass
        fallback_files = [path for _mtime, path in sorted(fallback_file_entries)]
        retained: list[Path] = []
        for path in fallback_files:
            try:
                if path.stat().st_mtime < cutoff.timestamp():
                    path.unlink()
                    fallback_files_deleted += 1
                else:
                    retained.append(path)
            except FileNotFoundError:
                pass
        excess = max(0, len(retained) - fallback_limit)
        for path in retained[:excess]:
            try:
                path.unlink()
                fallback_files_deleted += 1
            except FileNotFoundError:
                pass

    return {
        "expired_day_dirs": expired_day_dirs,
        "fallback_files": fallback_files_deleted,
    }


def _maybe_cleanup_quality_artifacts() -> None:
    global _quality_artifact_last_cleanup_at
    now_value = time.time()
    if now_value - _quality_artifact_last_cleanup_at < _QUALITY_ARTIFACT_CLEANUP_INTERVAL_SEC:
        return
    with _QUALITY_ARTIFACT_CLEANUP_LOCK:
        if now_value - _quality_artifact_last_cleanup_at < _QUALITY_ARTIFACT_CLEANUP_INTERVAL_SEC:
            return
        _quality_artifact_last_cleanup_at = now_value
        try:
            cleanup_quality_artifacts(now_epoch=now_value)
        except Exception as exc:
            print(f"[DQ] artifact cleanup failed: {type(exc).__name__}: {exc}")


def write_quality_event(event: dict[str, Any]) -> None:
    try:
        row = [_quality_event_value(event, column) for column in QUALITY_EVENT_COLUMNS]
        with _quality_event_client_lock:
            client = _get_quality_event_client()
            client.insert(QUALITY_EVENT_TABLE, [row], column_names=QUALITY_EVENT_COLUMNS)
    except Exception as exc:
        _discard_quality_event_client()
        fallback_quality_event(event, exc)
    finally:
        _maybe_cleanup_quality_artifacts()


def fallback_quality_event(event: dict[str, Any], error: Exception | None = None) -> None:
    path = Path(os.getenv("SPARK_DQ_QUALITY_EVENT_FALLBACK_DIR", QUALITY_FALLBACK_DIR))
    path.mkdir(parents=True, exist_ok=True)
    record = dict(event)
    if error is not None:
        record["quality_event_write_error"] = str(error)
    with (path / f"pipeline_quality_{int(time.time() * 1000)}_{event.get('batch_id', 0)}.jsonl").open(
        "a", encoding="utf-8"
    ) as handle:
        handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def quarantine_invalid_batch(df: DataFrame, *, topic: str, pipeline: str, batch_id: int) -> None:
    if "_dq_is_valid" not in df.columns:
        return
    invalid = invalid_rows(df)
    if invalid.rdd.isEmpty():
        return
    root = Path(os.getenv("SPARK_DQ_QUARANTINE_DIR") or QUARANTINE_ROOT)
    now = time.gmtime()
    path = (
        root
        / f"{now.tm_year:04d}"
        / f"{now.tm_mon:02d}"
        / f"{now.tm_mday:02d}"
        / _path_token(pipeline)
        / _path_token(topic)
        / f"batch_{int(batch_id)}_{int(time.time() * 1000)}"
    )
    path_str = str(path)
    try:
        (
            invalid.withColumn("_dq_topic", F.lit(topic))
            .withColumn("_dq_pipeline", F.lit(pipeline))
            .withColumn("_dq_batch_id", F.lit(int(batch_id)))
            .write.mode("append")
            .json(path_str)
        )
    except Exception as exc:
        fallback_quality_event(
            {
                "pipeline": pipeline,
                "stage": "silver_pre_aggregate_quarantine",
                "topic": topic,
                "batch_id": int(batch_id),
                "error_type": "dq_quarantine_write_failed",
                "sample_payload": f"path={path_str}; error={exc}",
            },
            exc,
        )
