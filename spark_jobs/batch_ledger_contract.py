"""Pure helpers for the shadow Spark batch ledger contract.

This module does not write to ClickHouse and is not wired into the runtime
writer path. It only keeps the proposed contract testable.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

LEDGER_CONTRACT_VERSION = "spark-batch-ledger-shadow-v1"
SHADOW_LEDGER_EVENT_TYPE = "spark_batch_ledger_shadow"
LEDGER_KEY_FIELDS = ("job_name", "output_table", "checkpoint_path", "batch_id")
VALID_AUDIT_STATUSES = {"started", "success", "failed", "skipped_empty"}
TERMINAL_STATUSES = {"success", "failed", "skipped_empty"}


class LedgerContractError(ValueError):
    """Raised when an audit event cannot be mapped to a ledger row."""


def _require_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LedgerContractError(f"missing required ledger field: {field}")
    return value.strip()


def _require_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise LedgerContractError(f"invalid integer ledger field: {field}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise LedgerContractError(f"invalid integer ledger field: {field}") from exc


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _warning_types(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    warning_types = []
    for warning in value:
        if isinstance(warning, dict) and warning.get("type"):
            warning_types.append(str(warning["type"]))
    return sorted(set(warning_types))


def build_ledger_key(
    *,
    job_name: str,
    output_table: str,
    checkpoint_path: str,
    batch_id: int,
) -> str:
    key_payload = {
        "job_name": _require_non_empty_string(job_name, "job_name"),
        "output_table": _require_non_empty_string(output_table, "output_table"),
        "checkpoint_path": _require_non_empty_string(checkpoint_path, "checkpoint_path"),
        "batch_id": _require_int(batch_id, "batch_id"),
    }
    encoded = json.dumps(key_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def audit_event_to_ledger_row(
    audit_event: dict[str, Any],
    *,
    checkpoint_path: str | None = None,
    run_id: str | None = None,
    attempt_id: str | None = None,
) -> dict[str, Any]:
    status = _require_non_empty_string(audit_event.get("status"), "status")
    if status not in VALID_AUDIT_STATUSES:
        raise LedgerContractError(f"unsupported audit status: {status}")

    job_name = _require_non_empty_string(audit_event.get("job_name"), "job_name")
    output_table = _require_non_empty_string(audit_event.get("output_table"), "output_table")
    checkpoint_path_value = checkpoint_path or audit_event.get("checkpoint_path")
    checkpoint_path = str(checkpoint_path_value).strip() if checkpoint_path_value else None
    batch_id = _require_int(audit_event.get("batch_id"), "batch_id")
    row_count = _require_int(audit_event.get("row_count", 0), "row_count")
    warning_count = _require_int(audit_event.get("warning_count", 0), "warning_count")
    run_id = run_id if run_id is not None else audit_event.get("run_id")
    attempt_id = attempt_id if attempt_id is not None else audit_event.get("attempt_id")
    if checkpoint_path:
        ledger_key = build_ledger_key(
            job_name=job_name,
            output_table=output_table,
            checkpoint_path=checkpoint_path,
            batch_id=batch_id,
        )
        ledger_key_status = audit_event.get("ledger_key_status") or "generated"
    else:
        ledger_key = audit_event.get("ledger_key")
        ledger_key_status = audit_event.get("ledger_key_status") or "missing_metadata"

    return {
        "ledger_contract_version": LEDGER_CONTRACT_VERSION,
        "ledger_key": ledger_key,
        "ledger_key_status": ledger_key_status,
        "job_name": job_name,
        "output_table": output_table,
        "checkpoint_path": checkpoint_path,
        "query_name": audit_event.get("query_name"),
        "batch_id": batch_id,
        "run_id": run_id,
        "attempt_id": attempt_id,
        "status": status,
        "row_count": row_count,
        "checksum": audit_event.get("checksum"),
        "null_count": _require_int(audit_event.get("null_count", 0), "null_count"),
        "nan_count": _require_int(audit_event.get("nan_count", 0), "nan_count"),
        "warning_count": warning_count,
        "quality_warning_types": _warning_types(audit_event.get("quality_warnings")),
        "critical_columns_checked": _string_list(audit_event.get("critical_columns_checked")),
        "threshold_policy_version": audit_event.get("threshold_policy_version"),
        "duration_ms": audit_event.get("duration_ms"),
        "error_type": audit_event.get("error_type"),
        "error_message_short": audit_event.get("error_message_short"),
        "observed_at": audit_event.get("occurred_at"),
    }


def classify_ledger_observation(
    existing_row: dict[str, Any] | None,
    incoming_row: dict[str, Any],
) -> dict[str, Any]:
    """Classify a ledger observation without mutating storage."""
    if existing_row is None:
        return {"action": "insert_new", "issues": []}

    if existing_row.get("ledger_key") != incoming_row.get("ledger_key"):
        return {"action": "reject_key_mismatch", "issues": ["ledger_key_mismatch"]}

    issues: list[str] = []
    existing_checksum = existing_row.get("checksum")
    incoming_checksum = incoming_row.get("checksum")
    if existing_checksum and incoming_checksum and existing_checksum != incoming_checksum:
        issues.append("checksum_mismatch")

    if existing_row.get("row_count") != incoming_row.get("row_count"):
        issues.append("row_count_mismatch")

    existing_status = existing_row.get("status")
    incoming_status = incoming_row.get("status")

    if issues:
        return {"action": "quarantine_conflict", "issues": issues}

    if existing_status == incoming_status:
        if incoming_status in TERMINAL_STATUSES:
            return {"action": "duplicate_terminal_same_payload", "issues": []}
        return {"action": "duplicate_in_progress", "issues": []}

    if existing_status == "started" and incoming_status in TERMINAL_STATUSES:
        return {"action": "advance_status", "issues": []}

    if existing_status == "failed" and incoming_status == "started":
        return {"action": "record_retry_attempt", "issues": []}

    if existing_status == "success" and incoming_status in {"started", "failed"}:
        return {"action": "keep_success_mark_retry", "issues": ["retry_after_success"]}

    if existing_status == "skipped_empty" and incoming_status != "skipped_empty":
        return {"action": "quarantine_conflict", "issues": ["skipped_empty_changed"]}

    return {"action": "record_status_observation", "issues": []}
