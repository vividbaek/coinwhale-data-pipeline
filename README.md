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

```bash
git clone https://github.com/vividbaek/coinwhale-data-pipeline.git
cd coinwhale-data-pipeline
```

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

## Suggested reading order

1. `collectors/run_all.py` and `collectors/base_collector.py`
2. `common/config.py`, `common/kafka_utils.py`, and `config/topic_contracts.json`
3. `spark_jobs/silver_aggregator.py`, `spark_jobs/silver/base.py`, and the plugins
4. `spark_jobs/ch_writer.py` and `database/init.sql`
5. `dbt/`
6. `airflow/dags/dbt_market_build.py`

## Data contract map

| Kafka input | Silver plugin | ClickHouse output |
| --- | --- | --- |
| `binance-trade`, `spot-trade` | `cvd.py` | `stream.cvd` |
| `binance-liquidation` | `liquidation.py` | `stream.liquidation` |
| `binance-markprice` | `funding.py` | `stream.funding` |
| `binance-markprice`, futures/spot book ticker | `price.py` | `stream.price` |
| `binance-openinterest` | `oi.py` | `stream.oi` |
| `binance-ticker` | `market_metrics.py` | `stream.market_metrics` |
| four long/short-ratio topics | `ls_ratio.py` | `stream.ls_ratio` |

The full topic list lives in `scripts/create_topics.sh`; the core payload requirements
live in `config/topic_contracts.json`.

## End-to-end local run

### Requirements

- Docker Compose v2
- Python 3.12
- approximately 6 GB of free memory for the demo stack

### 1. Configure

```bash
cp .env.example .env
```

Replace both `CHANGE_ME` values in `.env` with the same local-only password; the demo
uses the same ClickHouse account for Spark and dbt. Do not commit real credentials.

```bash
docker compose config --quiet
```

### 2. Start the infrastructure

Start Kafka, ClickHouse, Spark, Kafka Exporter, and Prometheus:

```bash
docker compose up -d
./scripts/create_topics.sh
```

Confirm that the services and topics are ready:

```bash
docker compose ps
docker compose exec -T kafka-1 \
  kafka-topics --bootstrap-server kafka-1:29092 --list
curl --fail --silent --show-error http://localhost:8123/ping
```

ClickHouse automatically initializes `stream.*`, `hist.*`, and pipeline-audit tables
from `database/init.sql`.

### 3. Start ingestion

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m collectors.run_all
```

Collectors run continuously in the foreground and publish public Binance data. Keep
this terminal open.

### 4. Start Spark Silver processing

Open another terminal in the repository root:

```bash
set -a
source .env
set +a
./scripts/run_spark.sh
```

The helper installs Python runtime dependencies on both Spark containers, then submits
the Silver application. The first run may take a few minutes while Spark resolves the
Kafka connector packages.

### 5. Verify the data flow

Check that a collector event reached Kafka:

```bash
docker compose exec -T kafka-1 \
  kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic binance-trade \
  --max-messages 1 \
  --timeout-ms 15000
```

Then inspect row counts in the ClickHouse stream tables:

```bash
set -a
source .env
set +a

curl --fail --silent --show-error \
  --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  --data-binary \
  "SELECT table, total_rows
   FROM system.tables
   WHERE database = 'stream'
   ORDER BY table
   FORMAT PrettyCompact" \
  http://localhost:8123
```

`total_rows` should begin increasing after both the collectors and Spark application
are running.

Useful endpoints:

| Service | Local URL |
| --- | --- |
| Spark master UI | <http://localhost:8080> |
| ClickHouse HTTP | <http://localhost:8123> |
| Prometheus | <http://localhost:9090> |
| Kafka Exporter | <http://localhost:9308/metrics> |
| Collector metrics | <http://localhost:8889/metrics> |

### 6. Stop

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

dbt source freshness --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt --select tag:gold_core
```

## Airflow example

`airflow/dags/dbt_market_build.py` demonstrates scheduled dbt orchestration. Airflow is
not part of the default Compose stack. Copy or mount the DAG into an Airflow deployment
that has this repository and its dbt dependencies available.

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

CI repeats the contract tests, Python compilation, Compose validation, dbt parsing,
and secret scanning.

## Troubleshooting

- `docker compose config` reports a missing variable: recreate `.env` from
  `.env.example` and replace every `CHANGE_ME`.
- A port is already allocated: stop the conflicting local service or change the
  published host port in `docker-compose.yml`.
- Kafka has data but ClickHouse does not: confirm the application is visible in the
  Spark UI, then run
  `docker compose logs --tail=100 spark-master spark-worker`.
- The Kafka sample command times out: confirm `python -m collectors.run_all` is still
  running and that the host can reach Binance.
- ClickHouse returns `Authentication failed`: source `.env` in the current shell
  before running the verification query.

For a broader snapshot:

```bash
docker compose logs --tail=100 \
  kafka-1 clickhouse spark-master spark-worker
```

## Production differences

The public demo intentionally uses one Kafka broker and small Spark resource defaults.
The private operating environment uses separate capacity, recovery, retention, and
access-control policies. Before production use, add at minimum:

- multi-broker replication and failure testing;
- TLS/SASL and network policy;
- external secret management;
- durable object storage and restore drills;
- capacity tests and consumer-lag SLOs;
- schema compatibility and deployment gates.

This repository demonstrates implementation patterns. It does not claim a specific
throughput, uptime, data scale, or trading outcome.

## Security and data use

- Never commit `.env`, API keys, passwords, webhooks, checkpoints, or raw data.
- Binance and other data-provider terms and rate limits still apply.
- Serialized model files and production data are intentionally excluded.
- Report vulnerabilities through the process in [SECURITY.md](SECURITY.md).

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution checks.

## License

No reuse license has been selected yet. The source is publicly visible, but reuse is
not granted until a license file is added.
