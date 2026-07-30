"""Pure row hash policy helpers for stream_shadow design fixtures.

This module is intentionally not wired into Spark writers or ClickHouse inserts.
It fixes a review-time contract for future stream_shadow row comparison tests.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

NULL_SENTINEL = "__NULL__"
ROW_HASH_POLICY_VERSION = "clickhouse-stream-row-hash-v1"
FLOAT_SCALE = Decimal("0.000000000001")


class RowHashPolicyError(ValueError):
    """Raised when a row cannot be hashed under the fixed policy."""


@dataclass(frozen=True)
class RowHashColumns:
    required: tuple[str, ...]
    optional: tuple[str, ...] = ()


_TABLE_HASH_COLUMNS: dict[str, RowHashColumns] = {
    "stream_shadow.price": RowHashColumns(
        required=(
            "symbol",
            "ts",
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
        ),
        optional=("futures_mid", "spot_mid", "spread_bps", "market_spread_bps"),
    ),
    "stream_shadow.oi": RowHashColumns(
        required=("symbol", "ts", "open_interest", "oi_change_pct"),
        optional=("oi_change",),
    ),
    "stream_shadow.cvd": RowHashColumns(
        required=(
            "symbol",
            "ts",
            "futures_taker_buy_vol",
            "futures_taker_sell_vol",
            "futures_cvd_delta",
            "futures_trade_count",
            "spot_taker_buy_vol",
            "spot_taker_sell_vol",
            "spot_cvd_delta",
            "spot_trade_count",
            "whale_buy_count",
            "whale_sell_count",
            "whale_buy_vol",
            "whale_sell_vol",
        ),
        optional=(
            "futures_buy_volume",
            "futures_sell_volume",
            "spot_buy_volume",
            "spot_sell_volume",
            "market",
            "source",
        ),
    ),
    "stream_shadow.funding": RowHashColumns(
        required=(
            "symbol",
            "ts",
            "funding_rate",
            "mark_price",
            "index_price",
            "next_funding_time",
            "mark_premium_pct",
        ),
    ),
    "stream_shadow.liquidation": RowHashColumns(
        required=(
            "symbol",
            "ts",
            "liq_long_count",
            "liq_long_vol",
            "liq_long_usd",
            "liq_short_count",
            "liq_short_vol",
            "liq_short_usd",
        ),
        optional=("side", "source_event_id", "liquidation_side", "liquidation_value"),
    ),
    "stream_shadow.market_metrics": RowHashColumns(
        required=(
            "symbol",
            "ts",
            "price_change_pct",
            "weighted_avg_price",
            "last_price",
            "volume_24h",
            "quote_volume_24h",
            "high_24h",
            "low_24h",
            "open_price_24h",
        ),
        optional=("composite_price",),
    ),
    "stream_shadow.ls_ratio": RowHashColumns(
        required=(
            "symbol",
            "ts",
            "ratio_type",
            "ls_ratio",
            "long_ratio",
            "short_ratio",
        ),
        optional=("period", "long_account", "short_account", "long_short_ratio"),
    ),
}

_TIMESTAMP_COLUMNS = {
    "ts",
    "next_funding_time",
    "window_start",
    "window_end",
    "event_time",
    "event_ts",
    "timestamp",
}
_STRING_COLUMNS = {
    "symbol",
    "ratio_type",
    "market",
    "source",
    "side",
    "source_event_id",
    "period",
}


def _canonical_table_name(table_name: str) -> str:
    name = str(table_name or "").strip()
    if name in _TABLE_HASH_COLUMNS:
        return name
    if name.startswith("stream."):
        shadow_name = "stream_shadow." + name.split(".", 1)[1]
        if shadow_name in _TABLE_HASH_COLUMNS:
            return shadow_name
    raise RowHashPolicyError(f"unknown row_hash table: {table_name!r}")


def get_row_hash_columns(table_name: str) -> RowHashColumns:
    """Return the canonical required/optional columns for a shadow table."""
    return _TABLE_HASH_COLUMNS[_canonical_table_name(table_name)]


def _is_nullish(value: Any) -> bool:
    if value is None:
        return True
    if type(value).__name__ in {"NAType", "NaTType"}:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, Decimal):
        return value.is_nan()
    return False


def _normalize_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.min)
    elif isinstance(value, str):
        raw = value.strip()
        parse_value = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            dt = datetime.fromisoformat(parse_value)
        except ValueError:
            return raw
    else:
        return str(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    millis = (dt.microsecond // 1000) * 1000
    dt = dt.replace(microsecond=millis)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _normalize_decimal(value: Any) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value)
    if number.is_nan():
        return NULL_SENTINEL
    if number == 0:
        return "0"
    quantized = number.quantize(FLOAT_SCALE, rounding=ROUND_HALF_UP)
    text = format(quantized.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def normalize_hash_value(value: Any, value_type: str | None = None) -> str:
    """Normalize one scalar value for row_hash calculation."""
    if _is_nullish(value):
        return NULL_SENTINEL
    if value_type == "timestamp":
        return _normalize_timestamp(value)
    if value_type == "number":
        return _normalize_decimal(value)
    return str(value)


def _column_value_type(column: str) -> str | None:
    if column in _TIMESTAMP_COLUMNS:
        return "timestamp"
    if column in _STRING_COLUMNS:
        return "string"
    return "number"


def build_row_hash(table_name: str, row_dict: dict[str, Any]) -> str:
    """Build a SHA-256 row hash from table-specific canonical columns.

    Required columns must be present. Optional columns are included only when
    present in the row, which lets fixtures cover future candidate fields
    without forcing today's Spark output schema to change.
    """
    columns = get_row_hash_columns(table_name)
    missing = [column for column in columns.required if column not in row_dict]
    if missing:
        raise RowHashPolicyError(
            f"missing required row_hash columns for {_canonical_table_name(table_name)}: {missing}"
        )

    canonical_items = []
    for column in columns.required + tuple(column for column in columns.optional if column in row_dict):
        canonical_items.append(
            [
                column,
                normalize_hash_value(row_dict.get(column), value_type=_column_value_type(column)),
            ]
        )

    payload = {
        "policy_version": ROW_HASH_POLICY_VERSION,
        "table": _canonical_table_name(table_name),
        "columns": canonical_items,
    }
    serialized = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
