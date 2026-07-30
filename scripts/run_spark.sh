#!/usr/bin/env bash
set -euo pipefail

# The driver runs on spark-master, while Python tasks can run on spark-worker.
docker compose exec -T spark-master \
  pip install -q -r /opt/spark/work-dir/requirements.txt
docker compose exec -T spark-worker \
  pip install -q -r /opt/spark/work-dir/requirements.txt

# Connection values are already injected into both Spark containers by Compose.
# Keeping them out of spark-submit arguments avoids exposing credentials there.
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/silver_aggregator.py
