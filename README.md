# CoinWhale Data Pipeline

A compact reference implementation of a real-time market-data pipeline:

```text
Binance WebSocket / REST
          |
          v
   Python collectors
          |
          v
      Kafka topics
          |
          v
Spark Structured Streaming
          |
          v
 ClickHouse stream.* tables
          |
          v
   dbt Gold market marts

Airflow: orchestration example
Prometheus: collector and Kafka metrics
```

이 저장소는 CoinWhale 운영 코드에서 데이터 엔지니어링 학습에 필요한 부분만
분리한 공개용 프로젝트입니다. 에이전트, RAG, 자동매매, 백테스트, 모델 artifact,
내부 운영 보고서는 포함하지 않습니다.

## What is included

| Layer | Path | What to inspect |
| --- | --- | --- |
| Ingestion | `collectors/` | WebSocket reconnect, REST polling, multi-symbol collection |
| Contracts | `config/`, `common/` | topic schemas, partition routing, DLQ and delivery callbacks |
| Streaming | `spark_jobs/` | event-time windows, watermarking, checkpoints, `foreachBatch` |
| Storage | `database/init.sql` | ClickHouse `stream.*` and `hist.*` schemas |
| Analytics | `dbt/` | sources, freshness, staging quality flags, Gold marts |
| Orchestration | `airflow/dags/` | minimal dbt freshness/build DAG |
| Observability | `infra/prometheus.yml` | Kafka lag and collector metrics |
| Verification | `tests/` | dependency-light contracts and Spark writer behavior |

## Local quick start

Requirements:

- Docker Compose v2
- Python 3.12
- approximately 6 GB of free memory for the demo stack

Create local configuration:

```bash
cp .env.example .env
```

Replace both `CHANGE_ME` values in `.env` with a local-only password, then validate
the Compose model:

```bash
docker compose config --quiet
```

Start Kafka, ClickHouse, Spark, Kafka Exporter and Prometheus:

```bash
docker compose up -d
./scripts/create_topics.sh
```

Install the collector dependencies and start ingestion:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m collectors.run_all
```

In another terminal, start the Spark Silver aggregation jobs:

```bash
set -a
source .env
set +a
./scripts/run_spark.sh
```

Useful endpoints:

| Service | Local URL |
| --- | --- |
| Spark master UI | <http://localhost:8080> |
| ClickHouse HTTP | <http://localhost:8123> |
| Prometheus | <http://localhost:9090> |
| Kafka Exporter | <http://localhost:9308/metrics> |
| Collector metrics | <http://localhost:8889/metrics> |

Stop the stack without deleting volumes:

```bash
docker compose down
```

Delete local Kafka and ClickHouse volumes only when intentional:

```bash
docker compose down --volumes
```

## dbt

The dbt project treats Spark-written `stream.*` tables as sources. It builds
source-conformed staging views and the `market_insights_5s` / `market_insights_10s`
Gold marts.

```bash
python3 -m venv .venv-dbt
source .venv-dbt/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dbt.txt

set -a
source .env
set +a
cp dbt/profiles.yml.example dbt/profiles.yml

dbt source freshness --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt --select tag:gold_core
```

## Verification

Dependency-light tests:

```bash
python -m pip install -r requirements-dev.txt
make test
make compile
```

Configuration checks:

```bash
make compose-check
make dbt-parse
```

CI runs tests, compilation, Compose validation, dbt parse, and secret scanning.

## Production differences

The public demo intentionally uses one Kafka broker and small Spark resource
defaults. The private operating environment uses separate capacity, recovery,
retention and access-control policies. Before production use, add at minimum:

- multi-broker replication and failure testing;
- TLS/SASL and network policy;
- external secret management;
- durable object storage and restore drills;
- capacity tests and consumer-lag SLOs;
- schema compatibility and deployment gates.

This repository demonstrates implementation patterns. It does not claim a
specific throughput, uptime, data scale, or trading outcome.

## Security and data use

- Never commit `.env`, API keys, passwords, webhooks, checkpoints or raw data.
- Binance and other data-provider terms and rate limits still apply.
- Serialized model files and production data are intentionally excluded.
- Report vulnerabilities through the process in [SECURITY.md](SECURITY.md).

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution checks.
