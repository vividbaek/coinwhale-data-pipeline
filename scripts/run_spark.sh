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

# Connection values are already injected into both Spark containers by Compose.
# Keeping them out of spark-submit arguments avoids exposing credentials there.
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:/opt/spark/work-dir/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=file:/opt/spark/work-dir/log4j2.properties" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION:-3.5.8}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-1g}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES:-1}" \
  --conf "spark.cores.max=${SPARK_CORES_MAX:-2}" \
  /opt/spark/work-dir/silver_aggregator.py
