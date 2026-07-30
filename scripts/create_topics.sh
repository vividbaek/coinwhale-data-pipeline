#!/usr/bin/env bash
set -euo pipefail

# This command runs inside the Kafka container, so it must use the internal
# listener even when host collectors use localhost:${KAFKA_HOST_PORT:-9092}.
broker="${KAFKA_INTERNAL_BOOTSTRAP_SERVERS:-kafka-1:29092}"
partitions="${KAFKA_PARTITIONS:-3}"
hot_partitions="${KAFKA_HOT_TOPIC_PARTITIONS:-6}"

hot_topics=(
  binance-trade binance-depth binance-bookticker
  spot-trade spot-depth spot-bookticker
)
regular_topics=(
  binance-kline binance-liquidation binance-markprice
  binance-openinterest binance-ticker binance-composite-index
  binance-ls-ratio binance-top-ls-account binance-top-ls-position
  binance-taker-ls-ratio spot-kline spot-ticker pipeline-dlq
)

create_topic() {
  local topic="$1" count="$2"
  docker compose exec -T kafka-1 kafka-topics \
    --bootstrap-server "$broker" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$count" \
    --replication-factor 1
}

for topic in "${hot_topics[@]}"; do create_topic "$topic" "$hot_partitions"; done
for topic in "${regular_topics[@]}"; do create_topic "$topic" "$partitions"; done
