"""Collector-side schema and hot-path data quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from common.topic_contracts import get_topic_contract

DEFAULT_SCHEMA_VERSION = 1
MAX_FUTURE_EVENT_MS = 60_000


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    error_type: str | None = None
    message: str = ""


def _has_any(payload: dict[str, Any], keys: tuple[str, ...] | list[str]) -> bool:
    return any(payload.get(key) not in (None, "") for key in keys)


def _is_number_like(value: Any) -> bool:
    if isinstance(value, bool) or value in (None, ""):
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _is_int_like(value: Any) -> bool:
    if isinstance(value, bool) or value in (None, ""):
        return False
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False


def _coerce_timestamp_ms(value: Any) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        ts = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.replace(".", "", 1).isdigit():
            ts = int(float(stripped))
        else:
            try:
                parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
            except ValueError:
                return None
            ts = int(parsed.timestamp() * 1000)
    else:
        return None

    if ts <= 0:
        return None
    if ts < 10_000_000_000:
        ts *= 1000
    return ts


def _is_timestamp_like(value: Any) -> bool:
    return _coerce_timestamp_ms(value) is not None


def infer_event_time_ms(payload: dict[str, Any], topic: str | None = None) -> int | None:
    contract = get_topic_contract(topic or "") or {}
    keys = contract.get("event_time_fields") or (
        "E",
        "T",
        "event_time",
        "eventTime",
        "trade_time",
        "time",
        "timestamp",
        "ts",
        "nextFundingTime",
    )
    for key in keys:
        ts = _coerce_timestamp_ms(payload.get(key))
        if ts is not None:
            return ts
    return None


def _validate_optional_timestamps(
    payload: dict[str, Any], keys: tuple[str, ...] | list[str]
) -> ValidationResult | None:
    for key in keys:
        if payload.get(key) not in (None, "") and not _is_timestamp_like(payload.get(key)):
            return ValidationResult(False, "schema_timestamp_invalid", f"invalid timestamp:{key}")
    return None


def _validate_type(payload: dict[str, Any], key: str, expected_type: str) -> ValidationResult | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    if expected_type == "number" and not _is_number_like(value):
        return ValidationResult(False, f"schema_{key}_invalid", f"{key} must be numeric")
    if expected_type == "integer" and not _is_int_like(value):
        return ValidationResult(False, f"schema_{key}_invalid", f"{key} must be integer")
    if expected_type == "timestamp" and not _is_timestamp_like(value):
        return ValidationResult(False, f"schema_{key}_invalid", f"{key} must be timestamp-like")
    return None


def _field_error_type(topic: str, key: str) -> str:
    if topic.endswith("trade"):
        if key == "p":
            return "schema_trade_price_invalid"
        if key == "q":
            return "schema_trade_quantity_invalid"
        if key in {"a", "t", "trade_id", "agg_trade_id"}:
            return "schema_trade_id_invalid"
    if topic.endswith("bookticker"):
        if key == "b":
            return "schema_bookticker_bid_invalid"
        if key == "a":
            return "schema_bookticker_ask_invalid"
    if topic.endswith("markprice"):
        if key == "p":
            return "schema_markprice_invalid"
        if key == "r":
            return "schema_funding_rate_invalid"
    if topic.endswith("openinterest") and key in {"openInterest", "open_interest"}:
        return "schema_openinterest_invalid"
    return f"schema_{key}_invalid"


def _validate_contract(
    *, topic: str, payload: dict[str, Any], ts_ms: int, contract: dict[str, Any]
) -> ValidationResult | None:
    timestamp_error = _validate_optional_timestamps(payload, contract.get("event_time_fields", ()))
    if timestamp_error:
        return timestamp_error

    event_time_ms = infer_event_time_ms(payload, topic)
    if contract.get("require_event_time") and event_time_ms is None:
        return ValidationResult(False, "schema_trade_time_missing", "trade event time is required")
    if event_time_ms is not None and event_time_ms > ts_ms + MAX_FUTURE_EVENT_MS:
        return ValidationResult(False, "schema_event_time_future", "event time is too far in the future")

    for key, expected_type in contract.get("required_fields", {}).items():
        if payload.get(key) in (None, ""):
            return ValidationResult(False, _field_error_type(topic, key), f"{key} is required")
        error = _validate_type(payload, key, expected_type)
        if error:
            return ValidationResult(False, _field_error_type(topic, key), error.message)

    for keys in contract.get("required_any", []):
        if not _has_any(payload, keys):
            return ValidationResult(
                False,
                "schema_required_any_missing",
                f"one of {','.join(keys)} is required",
            )

    for key, expected_type in contract.get("field_types", {}).items():
        error = _validate_type(payload, key, expected_type)
        if error:
            return ValidationResult(False, _field_error_type(topic, key), error.message)

    for key, expected_type in contract.get("optional_fields", {}).items():
        error = _validate_type(payload, key, expected_type)
        if error:
            return ValidationResult(False, _field_error_type(topic, key), error.message)

    for rule in contract.get("quality_rules", []):
        parts = rule.split(":")
        name = parts[0]
        fields = parts[1:]
        if name in {"positive_price", "positive_quantity"}:
            value = float(payload.get(fields[0]))
            if value <= 0:
                return ValidationResult(
                    False,
                    f"schema_{fields[0]}_non_positive",
                    f"{fields[0]} must be positive",
                )
        elif name == "bid_lte_ask":
            bid = float(payload.get(fields[0]))
            ask = float(payload.get(fields[1]))
            if bid > ask:
                return ValidationResult(
                    False,
                    "schema_bid_ask_crossed",
                    "bid must be less than or equal to ask",
                )
        elif name == "non_negative_any":
            values = [payload.get(field) for field in fields if payload.get(field) not in (None, "")]
            if values and all(float(value) < 0 for value in values):
                return ValidationResult(False, "schema_negative_value", "value must be non-negative")
    return None


def validate_collector_event(
    *,
    topic: str,
    stream_name: str,
    symbol: str,
    payload: Any,
    ts_ms: int,
) -> ValidationResult:
    if not topic:
        return ValidationResult(False, "schema_topic_missing", "topic is required")
    if not symbol:
        return ValidationResult(False, "schema_symbol_missing", "symbol is required")
    if not isinstance(symbol, str):
        return ValidationResult(False, "schema_symbol_invalid", "symbol must be a string")
    if not isinstance(ts_ms, int) or ts_ms <= 0:
        return ValidationResult(False, "schema_timestamp_invalid", "positive integer ts is required")
    if not isinstance(payload, dict):
        return ValidationResult(False, "schema_payload_invalid", "payload must be an object")

    contract = get_topic_contract(topic)
    if contract:
        error = _validate_contract(topic=topic, payload=payload, ts_ms=ts_ms, contract=contract)
        return error or ValidationResult(True)

    stream_lower = stream_name.lower()
    timestamp_error = _validate_optional_timestamps(
        payload,
        (
            "E",
            "T",
            "event_time",
            "eventTime",
            "trade_time",
            "time",
            "timestamp",
            "ts",
            "nextFundingTime",
        ),
    )
    if timestamp_error:
        return timestamp_error

    if topic.endswith("depth"):
        has_short_depth = isinstance(payload.get("b"), list) and isinstance(payload.get("a"), list)
        has_named_depth = isinstance(payload.get("bids"), list) and isinstance(payload.get("asks"), list)
        if not (has_short_depth or has_named_depth):
            return ValidationResult(False, "schema_depth_levels_missing", "depth bids/asks are required")
        return ValidationResult(True)

    if topic.endswith("kline") or "kline" in stream_lower:
        kline = payload.get("k")
        if isinstance(kline, dict):
            timestamp_error = _validate_optional_timestamps(kline, ("t", "T", "E", "timestamp", "time", "ts"))
            if timestamp_error:
                return timestamp_error
            for key in ("o", "h", "l", "c", "v"):
                if not _is_number_like(kline.get(key)):
                    return ValidationResult(
                        False,
                        f"schema_kline_{key}_invalid",
                        f"kline {key} must be numeric",
                    )
            return ValidationResult(True)
        for key in ("o", "h", "l", "c", "v"):
            if not _is_number_like(payload.get(key)):
                return ValidationResult(False, f"schema_kline_{key}_invalid", f"kline {key} must be numeric")
        return ValidationResult(True)

    if "ls-ratio" in topic or "ls-account" in topic or "ls-position" in topic:
        if _has_any(payload, ("longShortRatio", "buySellRatio")):
            return ValidationResult(True)
        return ValidationResult(False, "schema_ratio_missing", "ratio field is required")

    if topic.endswith("liquidation"):
        order = payload.get("o")
        if not isinstance(order, dict):
            return ValidationResult(
                False,
                "schema_liquidation_order_missing",
                "liquidation order object is required",
            )
        if not _has_any(order, ("S", "s")):
            return ValidationResult(False, "schema_liquidation_side_missing", "liquidation side is required")
        return ValidationResult(True)

    return ValidationResult(True)
