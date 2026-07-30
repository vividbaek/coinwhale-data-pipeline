# Architecture

This repository is a compact, local reference pipeline. It keeps the complete
data path visible without publishing CoinWhale's private runtime, trading,
research, or operational evidence.

## Data path

```text
Binance WebSocket / REST
          |
          v
Python collectors
  - payload validation
  - reconnect/backoff
  - deterministic Kafka routing
  - delivery callbacks and DLQ
          |
          v
Kafka raw topics
          |
          v
Spark Structured Streaming
  - event-time parsing
  - watermark and window aggregation
  - invalid-row quarantine
  - micro-batch quality summaries
          |
          v
ClickHouse stream.*
  - 5-second Silver tables
  - 90-day TTL
  - write-audit logs
  - pipeline_quality_events
          |
          v
dbt gold.*
  - source freshness
  - staging quality flags
  - 5-second and 10-second market marts

Airflow schedules dbt freshness/build tasks.
Prometheus scrapes collector and Kafka metrics.
```

## Event envelope

WebSocket collectors publish a stable outer envelope and preserve the
provider payload under `data`.

```json
{
  "schema_version": 1,
  "event_time_ms": 1770000000000,
  "ingested_at_ms": 1770000000123,
  "source": "aggTrade",
  "collector": "DepthKlineAggTradeCollector",
  "symbol": "BTCUSDT",
  "topic": "binance-trade",
  "stream": "aggTrade",
  "data": {},
  "ts": 1770000000123
}
```

REST pollers use the same `symbol`, `stream`, `data`, and `ts` boundary. The
topic contract in `config/topic_contracts.json` validates the highest-value
payload fields before publish. Additional depth, kline, liquidation, and
long/short-ratio checks live in `common/collector_schema.py`.

## Delivery and replay semantics

- Collector delivery is asynchronous. Kafka delivery callbacks update success
  and failure metrics; invalid or failed records use `pipeline-dlq` and a
  bounded local fallback.
- Hot topics use deterministic symbol-aware partition routing. This preserves
  a stable routing boundary while allowing more than one lane per symbol.
- Spark checkpoints Kafka offsets in the `spark_checkpoints` Docker volume,
  mounted at `/opt/spark/work-dir/checkpoints`.
- ClickHouse writes use `foreachBatch`. The public tables use
  `ReplacingMergeTree` keyed by symbol and event time, but this is not a claim
  of end-to-end exactly-once delivery.
- OI percentage change and price book snapshots use driver-local state.
  Restarting the application rebuilds that state from new events; a production
  design should persist or reconstruct it when strict continuity is required.
- Historical table DDL is included as a compatible target shape. A historical
  loader and production backfill controller are intentionally not published in
  this focused repository.

## Data-quality boundaries

1. Collector validation rejects malformed provider payloads before Kafka.
2. Spark applies expression-based critical-field, range, and event-time checks.
3. Invalid Spark rows can be quarantined in the `spark_dlq` runtime volume.
4. Batch summaries are written to `stream.pipeline_quality_events`; if the
   table is unavailable, bounded local JSONL fallback files are used.
5. dbt staging models expose quality flags and data tests validate accepted
   flag values and non-null keys.

Neither quarantined payloads nor runtime quality-event exports belong in Git.

## Local versus production

The demo intentionally uses one Kafka broker, local Docker volumes, plaintext
internal traffic, and one Spark worker. A production design needs independent
decisions for replication, TLS/SASL, secrets, object storage, restore drills,
schema compatibility, capacity, SLOs, and deployment gates. The repository
does not publish CoinWhale's private values for those controls.
