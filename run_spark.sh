#!/bin/bash

# Usage: ./run_spark.sh <jar_name> <catalog> <resource> <start_date>
JAR_NAME=${1:-"etl-pipeline-assembly-1.0.0.jar"}
CATALOG=${2:-"my_orders_job"}
RESOURCE=${3:-"my_orders_job"}
START_DATE=${4:-"2024-01-15"}

BASE_DIR=/home/administrator/etl-pipeline

docker run --rm \
  --network analytics_bridge \
  -v "$BASE_DIR/target/scala-2.13/:/app/" \
  -v "$BASE_DIR/catalogs:/opt/spark/work-dir/catalogs" \
  -v "$BASE_DIR/configs:/opt/spark/work-dir/configs" \
  -v "$BASE_DIR/credentials:/opt/spark/work-dir/credentials" \
  -w /opt/spark/work-dir \
  apache/spark:4.0.2-scala2.13-java17-ubuntu \
  /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --class etl_pipeline.Main \
  /app/"$JAR_NAME" \
  --env prod \
  --catalog "$CATALOG" \
  --resource "$RESOURCE" \
  --start_date "$START_DATE"
