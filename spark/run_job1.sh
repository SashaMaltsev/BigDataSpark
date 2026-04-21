#!/usr/bin/env bash
set -euo pipefail

docker-compose run --rm spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/spark-apps/job1_load_star_postgres.py
