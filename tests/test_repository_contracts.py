from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _stream_table_columns() -> dict[str, set[str]]:
    sql = (ROOT / "database/init.sql").read_text(encoding="utf-8")
    tables: dict[str, set[str]] = {}
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS stream\.(?P<table>[a-z_]+)\s*"
        r"\((?P<body>.*?)\)\s*"
        r"ENGINE",
        re.DOTALL,
    )
    for match in pattern.finditer(sql):
        columns: set[str] = set()
        for raw_line in match.group("body").splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            columns.add(line.split()[0])
        tables[match.group("table")] = columns
    return tables


def _assigned_string_list(path: Path, name: str) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            continue
        value = ast.literal_eval(node.value)
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return value
    raise AssertionError(f"{name} was not found as a literal string list in {path}")


def test_spark_quality_event_writer_matches_clickhouse_schema() -> None:
    tables = _stream_table_columns()
    writer_columns = set(
        _assigned_string_list(ROOT / "spark_jobs/quality_gate.py", "QUALITY_EVENT_COLUMNS")
    )

    assert "pipeline_quality_events" in tables
    assert writer_columns <= tables["pipeline_quality_events"]


def test_dbt_staging_models_match_public_stream_schema() -> None:
    tables = _stream_table_columns()
    contracts = {
        "funding": {
            "ts",
            "symbol",
            "mark_price",
            "index_price",
            "funding_rate",
            "next_funding_time",
            "mark_premium_pct",
        },
        "market_metrics": {
            "ts",
            "symbol",
            "last_price",
            "price_change_pct",
            "weighted_avg_price",
            "volume_24h",
            "quote_volume_24h",
            "high_24h",
            "low_24h",
            "open_price_24h",
            "composite_price",
        },
        "oi": {"ts", "symbol", "open_interest", "oi_change_pct"},
    }

    for table, required_columns in contracts.items():
        model = (
            ROOT / f"dbt/models/staging/stg_stream_{table}.sql"
        ).read_text(encoding="utf-8")
        assert required_columns <= tables[table]
        for column in required_columns:
            assert re.search(rf"\b{re.escape(column)}\b", model)

    combined = "\n".join(
        (ROOT / f"dbt/models/staging/stg_stream_{table}.sql").read_text(encoding="utf-8")
        for table in contracts
    )
    legacy_columns = {
        "estimated_settle_price",
        "premium_index",
        "price_change_pct_24h",
        "high_price_24h",
        "low_price_24h",
        "trade_count_24h",
        "open_interest_value",
        "open_interest_change_5m",
    }
    assert not any(re.search(rf"\b{column}\b", combined) for column in legacy_columns)


def test_spark_runtime_and_kafka_connector_versions_match() -> None:
    dockerfile = (ROOT / "docker/spark/Dockerfile").read_text(encoding="utf-8")
    reader = (ROOT / "spark_jobs/kafka_reader.py").read_text(encoding="utf-8")
    submit = (ROOT / "scripts/run_spark.sh").read_text(encoding="utf-8")
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")

    image_version = re.search(r"^FROM spark:(\d+\.\d+\.\d+)-python3$", dockerfile, re.MULTILINE)
    connector_default = re.search(
        r'os\.getenv\("SPARK_VERSION", "(\d+\.\d+\.\d+)"\)',
        reader,
    )
    submit_default = re.search(r"\$\{SPARK_VERSION:-(\d+\.\d+\.\d+)\}", submit)
    env_version = re.search(r"^SPARK_VERSION=(\d+\.\d+\.\d+)$", env_example, re.MULTILINE)

    assert image_version
    assert connector_default
    assert submit_default
    assert env_version
    assert {
        image_version.group(1),
        connector_default.group(1),
        submit_default.group(1),
        env_version.group(1),
    } == {image_version.group(1)}
    assert "--packages" in submit
    assert "spark.jars.ivy=/tmp/.ivy2" in submit
    assert submit.count("log4j.configurationFile=") == 2


def test_host_kafka_port_does_not_change_container_listener_port() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "127.0.0.1:${KAFKA_HOST_PORT:-9092}:9092" in compose
    assert "EXTERNAL://0.0.0.0:9092" in compose
    assert "EXTERNAL://localhost:${KAFKA_HOST_PORT:-9092}" in compose


def test_named_ten_second_mart_uses_ten_second_bucket() -> None:
    model = (
        ROOT / "dbt/models/marts/market/market_insights_10s.sql"
    ).read_text(encoding="utf-8")
    assert "INTERVAL 10 SECOND" in model
    assert "INTERVAL 5 SECOND" not in model


def test_topic_setup_contains_every_default_silver_input() -> None:
    setup = (ROOT / "scripts/create_topics.sh").read_text(encoding="utf-8")
    expected_topics = {
        "binance-trade",
        "spot-trade",
        "binance-bookticker",
        "spot-bookticker",
        "binance-liquidation",
        "binance-markprice",
        "binance-openinterest",
        "binance-ticker",
        "binance-ls-ratio",
        "binance-top-ls-account",
        "binance-top-ls-position",
        "binance-taker-ls-ratio",
    }
    for topic in expected_topics:
        assert re.search(rf"\b{re.escape(topic)}\b", setup)
