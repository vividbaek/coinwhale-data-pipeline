# Local Runbook

The commands below operate only on this repository's Compose project. They do
not require CoinWhale's private services or data.

## Clean start

```bash
cp .env.example .env
```

Replace both `CHANGE_ME` values with the same local-only password, then:

```bash
docker compose config --quiet
docker compose up -d --build
./scripts/create_topics.sh
docker compose ps
```

Create and activate the collector environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m collectors.run_all
```

In a second terminal:

```bash
./scripts/run_spark.sh
```

After rows begin flowing:

```bash
./scripts/check_pipeline.sh
```

Use `REQUIRE_DATA=0 ./scripts/check_pipeline.sh` to validate infrastructure and
schema before collectors and Spark have produced their first row.

## dbt

```bash
python3 -m venv .venv-dbt
source .venv-dbt/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dbt.txt

dbt debug --project-dir dbt --profiles-dir dbt
dbt source freshness --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt
```

Source freshness is expected to fail on an empty or stopped stream. `dbt build`
validates all staging models, marts, and data tests against the running
ClickHouse schema.

## Stage-by-stage diagnosis

### Collector to Kafka

```bash
docker compose exec -T kafka-1 \
  kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic binance-trade \
  --max-messages 1 \
  --timeout-ms 15000
```

If this times out, confirm the collector process is still running and the host
can reach Binance. Review provider terms and rate limits before increasing the
symbol list or polling frequency.

### Kafka to Spark

```bash
docker compose logs --tail=100 spark-master spark-worker
```

Confirm the application is visible in the Spark UI and that the Kafka connector
package resolved. A corrupt or incompatible checkpoint should be preserved for
inspection before any reset.

### Spark to ClickHouse

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
  "http://127.0.0.1:${CLICKHOUSE_HTTP_PORT:-8123}"
```

Inspect recent quality events without printing sample payloads:

```sql
SELECT
    event_time,
    pipeline,
    topic,
    input_rows,
    dropped_rows,
    error_type
FROM stream.pipeline_quality_events
ORDER BY event_time DESC
LIMIT 20;
```

## Port isolation

Every published port can be overridden in `.env`. This is useful when another
Kafka, Spark, ClickHouse, or Prometheus stack is already running.

```dotenv
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_HOST_PORT=19092
CLICKHOUSE_PORT=18123
CLICKHOUSE_HTTP_PORT=18123
CLICKHOUSE_NATIVE_PORT=19000
SPARK_UI_PORT=18080
SPARK_MASTER_PORT=17077
KAFKA_EXPORTER_PORT=19308
PROMETHEUS_PORT=19090
```

Use a separate Compose project name for complete isolation:

```bash
COMPOSE_PROJECT_NAME=coinwhale-public-demo docker compose up -d --build
```

Pass the same `COMPOSE_PROJECT_NAME` to topic, Spark, health-check, and teardown
commands.

## Stop and reset

Stop containers while keeping Kafka and ClickHouse volumes:

```bash
docker compose down
```

The following removes this Compose project's Kafka and ClickHouse volumes. Run
it only when deleting local demo data is intentional:

```bash
docker compose down --volumes
```
