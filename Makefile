.PHONY: test compile dbt-parse compose-check verify

PYTHON ?= python3

test:
	$(PYTHON) -m pytest -q

compile:
	$(PYTHON) -m compileall -q collectors common spark_jobs airflow/dags

dbt-parse:
	dbt parse --project-dir dbt --profiles-dir dbt

compose-check:
	docker compose --env-file .env.example config --quiet

verify: test compile compose-check
