import importlib
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPARK_JOBS = PROJECT_ROOT / "spark_jobs"


def _install_dependency_stubs() -> None:
    clickhouse_connect = types.ModuleType("clickhouse_connect")
    clickhouse_connect.get_client = Mock()
    sys.modules["clickhouse_connect"] = clickhouse_connect

    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_functions = types.ModuleType("pyspark.sql.functions")
    pyspark_types = types.ModuleType("pyspark.sql.types")

    class _Column:
        def __init__(self, name):
            self.name = name

        def cast(self, _target):
            return self

    class TimestampType:
        pass

    pyspark_functions.col = lambda name: _Column(name)
    pyspark_types.TimestampType = TimestampType
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_functions
    sys.modules["pyspark.sql.types"] = pyspark_types


def _load_ch_writer():
    _install_dependency_stubs()
    sys.path.insert(0, str(SPARK_JOBS))
    os.environ.setdefault("CLICKHOUSE_PASSWORD", "CHANGE_ME_TEST")
    sys.modules.pop("ch_writer", None)
    return importlib.import_module("ch_writer")


class _FakeSchema:
    fields = []


class _FakeDataFrame:
    schema = _FakeSchema()

    def __init__(self, pdf: pd.DataFrame):
        self._pdf = pdf

    def toPandas(self):
        return self._pdf.copy()


class SparkWriteAuditTests(unittest.TestCase):
    def setUp(self):
        self._audit_env = patch.dict(
            os.environ,
            {
                "SPARK_WRITE_AUDIT_STDOUT": "1",
                "SPARK_SHADOW_LEDGER_STDOUT": "1",
            },
        )
        self._audit_env.start()
        self.ch_writer = _load_ch_writer()

    def tearDown(self):
        self._audit_env.stop()

    def test_clickhouse_client_is_reused_across_foreach_batch_threads(self):
        fake_client = Mock()
        self.ch_writer.clickhouse_connect.get_client.return_value = fake_client
        clients = []

        def resolve_client():
            clients.append(self.ch_writer.get_client())

        threads = [self.ch_writer.threading.Thread(target=resolve_client) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(clients, [fake_client] * 4)
        self.ch_writer.clickhouse_connect.get_client.assert_called_once()

    def test_clickhouse_client_is_recreated_after_failed_healthcheck(self):
        first = Mock()
        second = Mock()
        first.ping.side_effect = RuntimeError("connection closed")
        self.ch_writer.clickhouse_connect.get_client.side_effect = [first, second]

        self.assertIs(self.ch_writer.get_client(), first)
        self.ch_writer._client_last_healthcheck = (
            self.ch_writer.time.monotonic()
            - self.ch_writer.CH_CLIENT_PING_INTERVAL_SEC
            - 1
        )
        self.assertIs(self.ch_writer.get_client(), second)

        first.close.assert_called_once()
        self.assertEqual(self.ch_writer.clickhouse_connect.get_client.call_count, 2)

    def test_checksum_is_deterministic_for_same_rows(self):
        first = pd.DataFrame(
            [
                {"ts": "2026-05-17 00:00:00", "symbol": "BTCUSDT", "mark_price": 100.0},
                {"ts": "2026-05-17 00:00:05", "symbol": "ETHUSDT", "mark_price": 200.0},
            ]
        )
        second = first.iloc[::-1].reset_index(drop=True)

        self.assertEqual(
            self.ch_writer.compute_batch_checksum(first),
            self.ch_writer.compute_batch_checksum(second),
        )

    def test_audit_event_counts_null_and_nan_without_payload_values(self):
        secret_value = "super-secret-value"
        pdf = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "ts": "2026-05-17 00:00:00",
                    "mark_price": np.nan,
                },
                {"symbol": None, "ts": "2026-05-17 00:00:05", "mark_price": 10.0},
            ]
        )
        pdf["password"] = secret_value

        event = self.ch_writer.build_write_audit_event(
            job_name="PriceInsight",
            batch_id=7,
            output_table="stream.price",
            pdf=pdf,
            status="started",
        )

        self.assertEqual(event["event_type"], "spark_clickhouse_write_audit")
        self.assertEqual(event["row_count"], 2)
        self.assertEqual(event["null_count"], 2)
        self.assertEqual(event["nan_count"], 1)
        self.assertEqual(event["null_columns"]["symbol"], 1)
        self.assertEqual(event["null_columns"]["mark_price"], 1)
        self.assertEqual(event["nan_columns"]["mark_price"], 1)
        self.assertNotIn(secret_value, str(event))

    def test_checkpoint_path_generates_ledger_key(self):
        checkpoint_path = "/opt/spark/work-dir/checkpoints/stream-oi"
        event = self.ch_writer.build_write_audit_event(
            job_name="OIInsight",
            batch_id=42,
            output_table="stream.oi",
            checkpoint_path=checkpoint_path,
            query_name="OIInsight",
            pdf=pd.DataFrame(
                [
                    {
                        "symbol": "BTCUSDT",
                        "ts": "2026-05-17 00:00:00",
                        "open_interest": 1.0,
                    }
                ]
            ),
            status="success",
        )

        self.assertEqual(event["checkpoint_path"], checkpoint_path)
        self.assertEqual(event["query_name"], "OIInsight")
        self.assertEqual(event["ledger_key_status"], "generated")
        self.assertIsNotNone(event["ledger_key"])
        self.assertEqual(len(event["ledger_key"]), 64)

    def test_missing_checkpoint_path_leaves_ledger_key_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            event = self.ch_writer.build_write_audit_event(
                job_name="OIInsight",
                batch_id=42,
                output_table="stream.oi",
                pdf=pd.DataFrame(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "ts": "2026-05-17 00:00:00",
                            "open_interest": 1.0,
                        }
                    ]
                ),
                status="success",
            )

        self.assertIsNone(event["checkpoint_path"])
        self.assertIsNone(event["run_id"])
        self.assertIsNone(event["ledger_key"])
        self.assertEqual(event["ledger_key_status"], "missing_metadata")

    def test_ledger_key_is_deterministic_and_checkpoint_sensitive(self):
        pdf = pd.DataFrame(
            [{"symbol": "BTCUSDT", "ts": "2026-05-17 00:00:00", "open_interest": 1.0}]
        )
        first = self.ch_writer.build_write_audit_event(
            job_name="OIInsight",
            batch_id=42,
            output_table="stream.oi",
            checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
            pdf=pdf,
            status="success",
        )
        second = self.ch_writer.build_write_audit_event(
            job_name="OIInsight",
            batch_id=42,
            output_table="stream.oi",
            checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
            pdf=pdf,
            status="success",
        )
        different_checkpoint = self.ch_writer.build_write_audit_event(
            job_name="OIInsight",
            batch_id=42,
            output_table="stream.oi",
            checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi-replay",
            pdf=pdf,
            status="success",
        )

        self.assertEqual(first["ledger_key"], second["ledger_key"])
        self.assertNotEqual(first["ledger_key"], different_checkpoint["ledger_key"])

    def test_run_id_uses_env_when_present_without_being_required(self):
        with patch.dict(os.environ, {"SPARK_RUN_ID": "run-20260517"}, clear=False):
            event = self.ch_writer.build_write_audit_event(
                job_name="OIInsight",
                batch_id=42,
                output_table="stream.oi",
                checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
                pdf=pd.DataFrame(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "ts": "2026-05-17 00:00:00",
                            "open_interest": 1.0,
                        }
                    ]
                ),
                status="success",
            )

        self.assertEqual(event["run_id"], "run-20260517")
        self.assertEqual(event["ledger_key_status"], "generated")

    def test_ledger_key_does_not_include_row_values_or_secret_like_metadata(self):
        secret_value = "do-not-log-this"
        event = self.ch_writer.build_write_audit_event(
            job_name="OIInsight",
            batch_id=42,
            output_table="stream.oi",
            checkpoint_path="s3://user:super-secret-password@bucket/checkpoints?api_key=abc123",
            run_id="password=hidden-run-token",
            pdf=pd.DataFrame(
                [
                    {
                        "symbol": "BTCUSDT",
                        "ts": "2026-05-17 00:00:00",
                        "open_interest": 1.0,
                        "payload": secret_value,
                    }
                ]
            ),
            status="success",
        )

        rendered = str(event)
        self.assertEqual(event["ledger_key_status"], "generated")
        self.assertNotIn(secret_value, rendered)
        self.assertNotIn("super-secret-password", rendered)
        self.assertNotIn("abc123", rendered)
        self.assertNotIn("hidden-run-token", rendered)

    def test_critical_column_null_count_creates_quality_warning(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="OIInsight",
            batch_id=1,
            output_table="stream.oi",
            pdf=pd.DataFrame(
                [
                    {
                        "symbol": "BTCUSDT",
                        "ts": "2026-05-17 00:00:00",
                        "open_interest": 10.0,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "ts": "2026-05-17 00:00:05",
                        "open_interest": None,
                    },
                ]
            ),
            status="started",
        )

        self.assertEqual(event["threshold_policy_version"], "spark-write-quality-v1")
        self.assertIn("open_interest", event["critical_columns_checked"])
        self.assertIn(
            {
                "type": "critical_nulls",
                "column": "open_interest",
                "count": 1,
                "rate": 0.5,
            },
            event["quality_warnings"],
        )
        self.assertEqual(event["warning_count"], 1)

    def test_missing_critical_column_creates_quality_warning(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="FundingInsight",
            batch_id=2,
            output_table="stream.funding",
            pdf=pd.DataFrame(
                [{"symbol": "BTCUSDT", "ts": "2026-05-17 00:00:00", "mark_price": 1.0}]
            ),
            status="started",
        )

        self.assertIn(
            {"type": "missing_critical_column", "column": "funding_rate"},
            event["quality_warnings"],
        )

    def test_noncritical_null_rate_at_threshold_creates_warning(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="MarketMetricsInsight",
            batch_id=3,
            output_table="stream.market_metrics",
            pdf=pd.DataFrame(
                [
                    {
                        "symbol": "BTCUSDT",
                        "ts": "2026-05-17 00:00:00",
                        "composite_price": None,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "ts": "2026-05-17 00:00:05",
                        "composite_price": 1.0,
                    },
                    {
                        "symbol": "SOLUSDT",
                        "ts": "2026-05-17 00:00:10",
                        "composite_price": 2.0,
                    },
                ]
            ),
            status="started",
        )

        self.assertIn(
            {
                "type": "high_null_rate",
                "column": "composite_price",
                "count": 1,
                "rate": 1 / 3,
                "threshold": 0.3,
            },
            event["quality_warnings"],
        )

    def test_noncritical_null_rate_below_threshold_has_no_warning(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="MarketMetricsInsight",
            batch_id=4,
            output_table="stream.market_metrics",
            pdf=pd.DataFrame(
                [
                    {
                        "symbol": f"S{i}USDT",
                        "ts": "2026-05-17 00:00:00",
                        "composite_price": None if i == 0 else 1.0,
                    }
                    for i in range(4)
                ]
            ),
            status="started",
        )

        warning_types = {
            (warning["type"], warning.get("column"))
            for warning in event["quality_warnings"]
        }
        self.assertNotIn(("high_null_rate", "composite_price"), warning_types)

    def test_nan_rate_at_threshold_creates_warning(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="PriceInsight",
            batch_id=5,
            output_table="stream.price",
            pdf=pd.DataFrame(
                [
                    {
                        "symbol": "BTCUSDT",
                        "ts": "2026-05-17 00:00:00",
                        "basis_pct": np.nan,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "ts": "2026-05-17 00:00:05",
                        "basis_pct": 0.1,
                    },
                    {
                        "symbol": "SOLUSDT",
                        "ts": "2026-05-17 00:00:10",
                        "basis_pct": 0.2,
                    },
                ]
            ),
            status="started",
        )

        self.assertIn(
            {
                "type": "high_nan_rate",
                "column": "basis_pct",
                "count": 1,
                "rate": 1 / 3,
                "threshold": 0.3,
            },
            event["quality_warnings"],
        )

    def test_nonempty_batch_without_checksum_creates_warning(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="UnknownWriter",
            batch_id=6,
            output_table="stream.unknown",
            pdf=pd.DataFrame([{}]),
            status="started",
        )

        self.assertIn({"type": "missing_checksum"}, event["quality_warnings"])

    def test_nonempty_batch_without_time_column_creates_warning(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="UnknownWriter",
            batch_id=7,
            output_table="stream.unknown",
            pdf=pd.DataFrame([{"symbol": "BTCUSDT", "value": 1.0}]),
            status="started",
        )

        warning_types = [warning["type"] for warning in event["quality_warnings"]]
        self.assertIn("missing_time_column", warning_types)

    def test_empty_dataframe_write_logs_skipped_empty(self):
        pdf = pd.DataFrame(columns=["ts", "symbol", "mark_price"])
        batch_df = _FakeDataFrame(pdf)

        with patch("builtins.print"), patch.object(
            self.ch_writer,
            "log_write_audit_event",
            wraps=self.ch_writer.log_write_audit_event,
        ) as audit:
            self.ch_writer.write_to_clickhouse(
                batch_df,
                batch_id=42,
                table_name="stream.price",
                job_name="PriceInsight",
                checkpoint_path="/opt/spark/work-dir/checkpoints/stream-price",
                query_name="PriceInsight",
            )

        self.assertEqual(audit.call_count, 1)
        self.assertEqual(audit.call_args.kwargs["status"], "skipped_empty")
        self.assertEqual(audit.call_args.kwargs["output_table"], "stream.price")
        self.assertEqual(
            audit.call_args.kwargs["checkpoint_path"],
            "/opt/spark/work-dir/checkpoints/stream-price",
        )
        self.assertEqual(audit.call_args.kwargs["query_name"], "PriceInsight")

        event = self.ch_writer.build_write_audit_event(
            job_name="PriceInsight",
            batch_id=42,
            output_table="stream.price",
            pdf=pdf,
            status="skipped_empty",
        )
        self.assertEqual(event["quality_warnings"], [])
        self.assertEqual(event["warning_count"], 0)

    def test_log_write_audit_event_also_logs_shadow_ledger_event(self):
        with patch("builtins.print") as printed:
            self.ch_writer.log_write_audit_event(
                job_name="OIInsight",
                batch_id=44,
                output_table="stream.oi",
                checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
                query_name="OIInsight",
                pdf=pd.DataFrame(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "ts": "2026-05-17 00:00:00",
                            "open_interest": None,
                        }
                    ]
                ),
                status="success",
            )

        events = [json.loads(call.args[0]) for call in printed.call_args_list]
        self.assertEqual(events[0]["event_type"], "spark_clickhouse_write_audit")
        self.assertEqual(events[1]["event_type"], "spark_batch_ledger_shadow")
        self.assertEqual(events[1]["ledger_key_status"], "generated")
        self.assertEqual(events[1]["job_name"], "OIInsight")
        self.assertEqual(events[1]["status"], "success")
        self.assertEqual(events[1]["row_count"], 1)
        self.assertEqual(events[1]["warning_count"], events[0]["warning_count"])
        self.assertEqual(events[1]["quality_warnings"], events[0]["quality_warnings"])
        self.assertNotEqual(events[0]["event_type"], events[1]["event_type"])

    def test_shadow_ledger_event_logs_failed_and_skipped_empty_statuses(self):
        for status in ("failed", "skipped_empty"):
            with self.subTest(status=status), patch("builtins.print") as printed:
                self.ch_writer.log_write_audit_event(
                    job_name="PriceInsight",
                    batch_id=45,
                    output_table="stream.price",
                    checkpoint_path="/opt/spark/work-dir/checkpoints/stream-price",
                    query_name="PriceInsight",
                    pdf=pd.DataFrame(columns=["ts", "symbol", "mark_price"]),
                    status=status,
                    error_type="ch_timeout" if status == "failed" else None,
                    error="timeout" if status == "failed" else None,
                )

            shadow = json.loads(printed.call_args_list[1].args[0])
            self.assertEqual(shadow["event_type"], "spark_batch_ledger_shadow")
            self.assertEqual(shadow["status"], status)
            self.assertEqual(shadow["ledger_key_status"], "generated")

    def test_shadow_ledger_event_logs_missing_metadata(self):
        with patch("builtins.print") as printed:
            self.ch_writer.log_write_audit_event(
                job_name="UnknownWriter",
                batch_id=46,
                output_table="stream.unknown",
                pdf=pd.DataFrame(
                    [{"symbol": "BTCUSDT", "ts": "2026-05-17 00:00:00", "value": 1.0}]
                ),
                status="success",
            )

        shadow = json.loads(printed.call_args_list[1].args[0])
        self.assertEqual(shadow["event_type"], "spark_batch_ledger_shadow")
        self.assertIsNone(shadow["ledger_key"])
        self.assertEqual(shadow["ledger_key_status"], "missing_metadata")

    def test_shadow_ledger_conversion_failure_does_not_raise(self):
        with patch("builtins.print") as printed, patch.object(
            self.ch_writer,
            "audit_event_to_ledger_row",
            side_effect=RuntimeError("password=shadow-secret"),
        ):
            event = self.ch_writer.log_write_audit_event(
                job_name="OIInsight",
                batch_id=47,
                output_table="stream.oi",
                checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
                pdf=pd.DataFrame(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "ts": "2026-05-17 00:00:00",
                            "open_interest": 1.0,
                        }
                    ]
                ),
                status="success",
            )

        events = [json.loads(call.args[0]) for call in printed.call_args_list]
        self.assertEqual(event["event_type"], "spark_clickhouse_write_audit")
        self.assertEqual(events[0]["event_type"], "spark_clickhouse_write_audit")
        self.assertEqual(events[1]["event_type"], "spark_batch_ledger_shadow_failed")
        self.assertNotIn("shadow-secret", str(events[1]))

    def test_shadow_ledger_event_does_not_include_payload_values(self):
        secret_value = "row-payload-secret"
        with patch("builtins.print") as printed:
            self.ch_writer.log_write_audit_event(
                job_name="OIInsight",
                batch_id=48,
                output_table="stream.oi",
                checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
                pdf=pd.DataFrame(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "ts": "2026-05-17 00:00:00",
                            "open_interest": 1.0,
                            "payload": secret_value,
                        }
                    ]
                ),
                status="success",
            )

        rendered = "\n".join(call.args[0] for call in printed.call_args_list)
        shadow = json.loads(printed.call_args_list[1].args[0])
        self.assertEqual(shadow["event_type"], "spark_batch_ledger_shadow")
        self.assertNotIn(secret_value, rendered)

    def test_quality_warning_does_not_block_insert_or_drop_rows(self):
        pdf = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "ts": "2026-05-17 00:00:00",
                    "open_interest": None,
                },
                {
                    "symbol": "ETHUSDT",
                    "ts": "2026-05-17 00:00:05",
                    "open_interest": 20.0,
                },
            ]
        )
        batch_df = _FakeDataFrame(pdf)
        fake_client = Mock()

        with patch("builtins.print"), patch.object(
            self.ch_writer, "get_client", return_value=fake_client
        ):
            self.ch_writer.write_to_clickhouse(
                batch_df,
                batch_id=43,
                table_name="stream.oi",
                job_name="OIInsight",
                checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
                query_name="OIInsight",
            )

        fake_client.insert_df.assert_called_once()
        inserted_table, inserted_pdf = fake_client.insert_df.call_args.args
        self.assertEqual(inserted_table, "stream.oi")
        self.assertEqual(len(inserted_pdf), 2)

    def test_clickhouse_failure_defaults_to_dlq_without_raising(self):
        pdf = pd.DataFrame(
            [{"symbol": "BTCUSDT", "ts": "2026-05-17 00:00:00", "open_interest": 10.0}]
        )
        batch_df = _FakeDataFrame(pdf)
        fake_client = Mock()
        fake_client.insert_df.side_effect = RuntimeError("connection refused")

        with tempfile.TemporaryDirectory() as tmpdir, patch("builtins.print"), patch.object(
            self.ch_writer, "get_client", return_value=fake_client
        ), patch.object(self.ch_writer, "DLQ_DIR", tmpdir), patch.object(
            self.ch_writer, "RAISE_AFTER_DLQ_ENABLED", False
        ):
            self.ch_writer.write_to_clickhouse(
                batch_df,
                batch_id=44,
                table_name="stream.oi",
                original_topic="binance-openinterest",
                job_name="OIInsight",
                checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
                query_name="OIInsight",
            )

            metadata_files = list(Path(tmpdir).glob("ch_failure_stream_oi_44_*.json"))
            data_files = list(Path(tmpdir).glob("ch_failure_stream_oi_44_*_data.jsonl"))

        self.assertEqual(len(metadata_files), 1)
        self.assertEqual(len(data_files), 1)

    def test_raise_after_dlq_mode_rethrows_to_prevent_checkpoint_advance(self):
        pdf = pd.DataFrame(
            [{"symbol": "BTCUSDT", "ts": "2026-05-17 00:00:00", "open_interest": 10.0}]
        )
        batch_df = _FakeDataFrame(pdf)
        fake_client = Mock()
        fake_client.insert_df.side_effect = RuntimeError("connection refused")

        with tempfile.TemporaryDirectory() as tmpdir, patch("builtins.print"), patch.object(
            self.ch_writer, "get_client", return_value=fake_client
        ), patch.object(self.ch_writer, "DLQ_DIR", tmpdir), patch.object(
            self.ch_writer, "RAISE_AFTER_DLQ_ENABLED", True
        ):
            with self.assertRaises(RuntimeError):
                self.ch_writer.write_to_clickhouse(
                    batch_df,
                    batch_id=45,
                    table_name="stream.oi",
                    original_topic="binance-openinterest",
                    job_name="OIInsight",
                    checkpoint_path="/opt/spark/work-dir/checkpoints/stream-oi",
                    query_name="OIInsight",
                )

            self.assertEqual(len(list(Path(tmpdir).glob("ch_failure_stream_oi_45_*.json"))), 1)

    def test_failed_audit_event_masks_error_message_length(self):
        event = self.ch_writer.build_write_audit_event(
            job_name="FundingInsight",
            batch_id=9,
            output_table="stream.funding",
            pdf=pd.DataFrame([{"symbol": "BTCUSDT", "funding_rate": 0.0001}]),
            status="failed",
            error_type="ch_connection_error",
            error="password=" + ("x" * 500),
        )

        self.assertEqual(event["status"], "failed")
        self.assertEqual(event["error_type"], "ch_connection_error")
        self.assertLessEqual(len(event["error_message_short"]), 300)
        self.assertNotIn("password", event["error_message_short"])

    def test_direct_price_and_oi_writers_use_common_audit_helper(self):
        price_source = (SPARK_JOBS / "silver" / "price.py").read_text()
        oi_source = (SPARK_JOBS / "silver" / "oi.py").read_text()

        for source in (price_source, oi_source):
            self.assertIn("_cw.log_write_audit_event(", source)
            self.assertIn("output_table=output_table", source)
            self.assertIn("checkpoint_path=self.CHECKPOINT", source)
            self.assertIn("query_name=self.__class__.__name__", source)


if __name__ == "__main__":
    unittest.main()
