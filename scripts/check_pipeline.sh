#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

required_services=(kafka-1 clickhouse spark-master spark-worker kafka-exporter prometheus)
running_services="$(docker compose ps --status running --services)"

for service in "${required_services[@]}"; do
  if ! grep -qx "$service" <<<"$running_services"; then
    echo "FAIL: service is not running: $service" >&2
    exit 1
  fi
done

topic_list="$(
  docker compose exec -T kafka-1 \
    kafka-topics --bootstrap-server kafka-1:29092 --list
)"
for topic in binance-trade spot-trade binance-markprice binance-openinterest pipeline-dlq; do
  if ! grep -qx "$topic" <<<"$topic_list"; then
    echo "FAIL: Kafka topic is missing: $topic" >&2
    exit 1
  fi
done

clickhouse_http_port="${CLICKHOUSE_HTTP_PORT:-8123}"
clickhouse_user="${CLICKHOUSE_USER:-coinwhale}"
clickhouse_password="${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}"
clickhouse_url="http://127.0.0.1:${clickhouse_http_port}"

curl --fail --silent --show-error "${clickhouse_url}/ping" >/dev/null

table_count="$(
  curl --fail --silent --show-error \
    --user "${clickhouse_user}:${clickhouse_password}" \
    --data-binary \
    "SELECT count()
     FROM system.tables
     WHERE database = 'stream'
       AND name IN (
         'cvd', 'liquidation', 'price', 'oi',
         'market_metrics', 'funding', 'ls_ratio',
         'pipeline_quality_events'
       )
     FORMAT TSVRaw" \
    "$clickhouse_url"
)"
if [[ "$table_count" != "8" ]]; then
  echo "FAIL: expected 8 public stream tables, found ${table_count}" >&2
  exit 1
fi

stream_rows="$(
  curl --fail --silent --show-error \
    --user "${clickhouse_user}:${clickhouse_password}" \
    --data-binary \
    "SELECT coalesce(sum(total_rows), 0)
     FROM system.tables
     WHERE database = 'stream'
       AND name IN (
         'cvd', 'liquidation', 'price', 'oi',
         'market_metrics', 'funding', 'ls_ratio'
       )
     FORMAT TSVRaw" \
    "$clickhouse_url"
)"

if [[ "${REQUIRE_DATA:-1}" == "1" && "$stream_rows" == "0" ]]; then
  echo "FAIL: stream tables exist but contain no rows" >&2
  exit 1
fi

curl --fail --silent --show-error \
  "http://127.0.0.1:${KAFKA_EXPORTER_PORT:-9308}/metrics" >/dev/null
curl --fail --silent --show-error \
  "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/ready" >/dev/null

echo "PASS: services, Kafka topics, ClickHouse schema, and monitoring endpoints are ready"
echo "stream_rows=${stream_rows}"
