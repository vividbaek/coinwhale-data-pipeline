.PHONY: test test-spark compile dbt-parse dbt-build compose-check verify

PYTHON ?= python3
DBT ?= dbt

test:
	$(PYTHON) -m pytest -q

test-spark:
	$(PYTHON) -m pytest -q tests/test_spark_quality_gate.py

compile:
	$(PYTHON) -m compileall -q collectors common spark_jobs airflow/dags

dbt-parse:
	CLICKHOUSE_DBT_PASSWORD="$${CLICKHOUSE_DBT_PASSWORD:-parse-only-placeholder}" \
		$(DBT) parse --project-dir dbt --profiles-dir dbt

dbt-build:
	$(DBT) build --project-dir dbt --profiles-dir dbt

compose-check:
	docker compose --env-file .env.example config --quiet

verify: test compile compose-check
