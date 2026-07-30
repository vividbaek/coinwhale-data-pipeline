"""Shared Kafka topic contracts for collector and parser quality checks."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

CONTRACT_PATH = Path(__file__).resolve().parents[1] / "config" / "topic_contracts.json"


@lru_cache(maxsize=1)
def load_topic_contracts() -> dict[str, dict[str, Any]]:
    raw = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    contracts: dict[str, dict[str, Any]] = raw.get("contracts", {})
    resolved: dict[str, dict[str, Any]] = {}

    def resolve(topic: str) -> dict[str, Any]:
        if topic in resolved:
            return resolved[topic]
        contract = dict(contracts[topic])
        parent_name = contract.pop("extends", None)
        if parent_name:
            parent = resolve(parent_name)
            merged = dict(parent)
            for key, value in contract.items():
                if isinstance(value, dict) and isinstance(merged.get(key), dict):
                    merged[key] = {**merged[key], **value}
                else:
                    merged[key] = value
            contract = merged
        resolved[topic] = contract
        return contract

    for topic in contracts:
        resolve(topic)
    return resolved


def get_topic_contract(topic: str) -> dict[str, Any] | None:
    return load_topic_contracts().get(topic)


def contract_topics() -> set[str]:
    return set(load_topic_contracts())
