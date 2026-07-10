#!/bin/bash
# Usage: ./run_spark.sh <jar_name> <catalog> <resource> <start_date>
JAR_NAME=${1:-"etl-pipeline-assembly-1.0.0.jar"}
CATALOG=${2:-"my_orders_job"}
RESOURCE=${3:-"my_orders_job"}
START_DATE=${4:-"$(date +%Y-%m-%d)"}
BASE_DIR=/home/bai/farmagitechs/etl_project/etl-pipeline

echo "Running pipeline: $CATALOG | date=$START_DATE"
docker run --rm \
  --network analytics_bridge \
  -v "$BASE_DIR/target/scala-2.13/:/app/" \
  -v "$BASE_DIR/catalogs:/opt/spark/work-dir/catalogs" \
  -v "$BASE_DIR/configs:/opt/spark/work-dir/configs" \
  -v "$BASE_DIR/credentials:/opt/spark/work-dir/credentials" \
  -w /opt/spark/work-dir \
  apache/spark:4.0.2-scala2.13-java17-ubuntu \
  /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --deploy-mode client \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 1 \
  --driver-memory 4g \
  --class etl_pipeline.Main \
  /app/"$JAR_NAME" \
  --env uat \
  --catalog "$CATALOG" \
  --resource "$RESOURCE" \
  --start_date "$START_DATE"

EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  echo "Failed: $CATALOG | date=$START_DATE (code=$EXIT_CODE)"
  exit $EXIT_CODE
fi
echo "Done: $CATALOG | date=$START_DATE"