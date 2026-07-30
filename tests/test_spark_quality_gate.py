import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

ROOT = Path(__file__).resolve().parents[1]
SPARK_JOBS = ROOT / "spark_jobs"
if str(SPARK_JOBS) not in sys.path:
    sys.path.insert(0, str(SPARK_JOBS))

import quality_gate as dq
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("spark-quality-gate-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _rows(df):
    return [row.asDict(recursive=True) for row in df.collect()]


def test_quality_gate_marks_valid_trade_row(spark):
    df = spark.createDataFrame(
        [("BTCUSDT", 100.0, 1.5, 1_770_000_000.0, "1", '{"schema_version":1}')],
        "symbol string, price double, quantity double, event_time_sec double, _schema_version string, _raw_value string",
    )

    out = dq.apply_quality_gate(df, topic="binance-trade", now_ms=1_770_000_005_000)
    row = _rows(out)[0]

    assert row["_dq_is_valid"] is True
    assert row["_parse_status"] == "ok"
    assert row["_event_lag_ms"] == 5000


@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        (("BTCUSDT", None, 1.0, 1_770_000_000.0), "null_critical"),
        (("BTCUSDT", -1.0, 1.0, 1_770_000_000.0), "negative_value"),
    ],
)
def test_quality_gate_rejects_null_and_negative_trade_values(spark, payload, expected_error):
    df = spark.createDataFrame(
        [payload],
        "symbol string, price double, quantity double, event_time_sec double",
    )

    row = _rows(dq.apply_quality_gate(df, topic="binance-trade", now_ms=1_770_000_005_000))[0]

    assert row["_dq_is_valid"] is False
    assert row["_parse_error"] == expected_error


def test_quality_gate_rejects_bid_ask_inversion(spark):
    df = spark.createDataFrame(
        [("BTCUSDT", 101.0, 100.0, 1_770_000_000.0)],
        "symbol string, bid_price double, ask_price double, event_time_sec double",
    )

    row = _rows(dq.apply_quality_gate(df, topic="binance-bookticker", now_ms=1_770_000_005_000))[0]

    assert row["_dq_is_valid"] is False
    assert row["_parse_error"] == "bid_ask_inverted"


def test_quality_gate_rejects_late_event(spark, monkeypatch):
    monkeypatch.setenv("SPARK_DQ_MAX_EVENT_LAG_MS", "1000")
    df = spark.createDataFrame(
        [("BTCUSDT", 100.0, 1.0, 1_770_000_000.0)],
        "symbol string, price double, quantity double, event_time_sec double",
    )

    row = _rows(dq.apply_quality_gate(df, topic="binance-trade", now_ms=1_770_000_005_000))[0]

    assert row["_dq_is_valid"] is False
    assert row["_parse_error"] == "event_time_lag"


def test_quality_gate_preserves_malformed_parse_status(spark):
    df = spark.createDataFrame(
        [("BTCUSDT", None, 1.0, 1_770_000_000.0, "parse_error", "malformed_json", "{bad")],
        "symbol string, price double, quantity double, event_time_sec double, _parse_status string, _parse_error string, _raw_value string",
    )

    row = _rows(dq.apply_quality_gate(df, topic="binance-trade", now_ms=1_770_000_005_000))[0]

    assert row["_dq_is_valid"] is False
    assert row["_parse_status"] == "parse_error"
    assert row["_parse_error"] == "malformed_json"


def test_build_quality_event_mixed_batch(spark):
    df = spark.createDataFrame(
        [
            ("BTCUSDT", 100.0, 1.0, 1_770_000_000.0),
            ("BTCUSDT", -1.0, 1.0, 1_770_000_000.0),
        ],
        "symbol string, price double, quantity double, event_time_sec double",
    )
    gated = dq.apply_quality_gate(df, topic="binance-trade", now_ms=1_770_000_005_000)

    event = dq.build_quality_event(
        batch_df=gated,
        output_df=None,
        pipeline="UnitPipeline",
        topic="binance-trade",
        checkpoint="/tmp/checkpoint",
        batch_id=7,
    )

    assert event["input_rows"] == 2
    assert event["dropped_rows"] == 1
    assert event["parsed_rows"] == 2
    assert event["error_type"] == "negative_value"
    assert event["schema_version"] == 1
    assert event["checksum"] == ""


def test_schema_version_string_coerces_to_uint():
    assert dq._schema_version_to_int("v12") == 12
    assert dq._schema_version_to_int("bad") == 1


def test_quality_event_writer_reuses_driver_process_client(monkeypatch):
    client = Mock()
    get_client = Mock(return_value=client)
    monkeypatch.setitem(sys.modules, "clickhouse_connect", SimpleNamespace(get_client=get_client))
    monkeypatch.setattr(dq, "_quality_event_client", None)
    monkeypatch.setattr(dq, "_quality_event_client_last_healthcheck", 0.0)
    event = dict(dq._QUALITY_EVENT_DEFAULTS)

    dq.write_quality_event(event)
    dq.write_quality_event(event)

    get_client.assert_called_once()
    assert client.insert.call_count == 2


def test_quality_event_writer_discards_failed_client(monkeypatch, tmp_path):
    client = Mock()
    client.insert.side_effect = RuntimeError("connection closed")
    get_client = Mock(return_value=client)
    monkeypatch.setitem(sys.modules, "clickhouse_connect", SimpleNamespace(get_client=get_client))
    monkeypatch.setattr(dq, "_quality_event_client", None)
    monkeypatch.setattr(dq, "_quality_event_client_last_healthcheck", 0.0)
    monkeypatch.setattr(dq, "QUALITY_FALLBACK_DIR", str(tmp_path))

    dq.write_quality_event(dict(dq._QUALITY_EVENT_DEFAULTS))

    client.close.assert_called_once()
    assert dq._quality_event_client is None
    assert list(tmp_path.glob("pipeline_quality_*.jsonl"))


def test_quality_artifact_cleanup_is_disabled_by_default(monkeypatch, tmp_path):
    monkeypatch.delenv("SPARK_DQ_CLEANUP_ENABLED", raising=False)
    quarantine = tmp_path / "quarantine"
    expired = quarantine / "2026" / "07" / "01"
    expired.mkdir(parents=True)

    result = dq.cleanup_quality_artifacts(
        quarantine_root=quarantine,
        fallback_dir=tmp_path / "fallback",
        now_epoch=datetime(2026, 7, 16, 12, tzinfo=timezone.utc).timestamp(),
    )

    assert result == {"expired_day_dirs": 0, "fallback_files": 0}
    assert expired.exists()


def test_quality_artifact_cleanup_removes_only_expired_days_and_bounds_fallback(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("SPARK_DQ_CLEANUP_ENABLED", "1")
    quarantine = tmp_path / "quarantine"
    fallback = tmp_path / "fallback"
    expired = quarantine / "2026" / "07" / "01" / "PriceInsight" / "topic" / "batch_1"
    recent = quarantine / "2026" / "07" / "15" / "PriceInsight" / "topic" / "batch_2"
    expired.mkdir(parents=True)
    recent.mkdir(parents=True)
    (expired / "part.json").write_text("{}", encoding="utf-8")
    (recent / "part.json").write_text("{}", encoding="utf-8")
    fallback.mkdir()
    now_epoch = datetime(2026, 7, 16, 12, tzinfo=timezone.utc).timestamp()
    for index in range(4):
        path = fallback / f"event_{index}.jsonl"
        path.write_text("{}\n", encoding="utf-8")
        os.utime(path, (now_epoch - (index * 60), now_epoch - (index * 60)))

    result = dq.cleanup_quality_artifacts(
        quarantine_root=quarantine,
        fallback_dir=fallback,
        now_epoch=now_epoch,
        max_age_hours=168,
        max_fallback_files=2,
    )

    assert result == {"expired_day_dirs": 1, "fallback_files": 2}
    assert not (quarantine / "2026" / "07" / "01").exists()
    assert recent.exists()
    assert len(list(fallback.glob("*.jsonl"))) == 2
