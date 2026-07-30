#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T spark-master bash -lc \
  'pip install -q clickhouse-connect numpy pandas &&
   /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --conf spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=kafka-1:29092 \
     --conf spark.executorEnv.CLICKHOUSE_HOST=clickhouse \
     --conf spark.executorEnv.CLICKHOUSE_PORT=8123 \
     --conf spark.executorEnv.CLICKHOUSE_USER="$CLICKHOUSE_USER" \
     --conf spark.executorEnv.CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
     /opt/spark/work-dir/silver_aggregator.py'
