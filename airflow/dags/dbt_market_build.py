"""Minimal Airflow example for dbt source freshness and market marts."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dbt_market_build",
    description="Validate stream freshness and build ClickHouse market marts",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["data-engineering", "dbt", "clickhouse"],
) as dag:
    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command="dbt source freshness --project-dir /opt/pipeline/dbt --profiles-dir /opt/pipeline/dbt",
    )
    build_market = BashOperator(
        task_id="build_market",
        bash_command="dbt build --project-dir /opt/pipeline/dbt --profiles-dir /opt/pipeline/dbt --select tag:gold_core",
    )

    source_freshness >> build_market
