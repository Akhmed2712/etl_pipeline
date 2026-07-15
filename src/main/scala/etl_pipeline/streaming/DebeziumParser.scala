package etl_pipeline.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DebeziumParser {

  /**
    * Parse Debezium CDC message.
    *
    * @param kafkaDf DataFrame hasil KafkaConsumer
    * @param valueSchema Schema tabel (schema after/before)
    */
  def parse(
      kafkaDf: DataFrame,
      valueSchema: StructType
  ): DataFrame = {

    val debeziumSchema =
      StructType(
        Seq(
          StructField(
            "payload",
            StructType(
              Seq(
                StructField(
                  "before",
                  valueSchema,
                  true
                ),
                StructField(
                  "after",
                  valueSchema,
                  true
                ),
                StructField(
                  "op",
                  StringType,
                  true
                ),
                StructField(
                  "ts_ms",
                  LongType,
                  true
                ),
                StructField(
                  "source",
                  StructType(
                    Seq(
                      StructField("db", StringType, true),
                      StructField("table", StringType, true),
                      StructField("file", StringType, true),
                      StructField("pos", LongType, true),
                      StructField("snapshot", StringType, true)
                    )
                  ),
                  true
                )
              )
            ),
            true
          )
        )
      )

    kafkaDf
      .withColumn(
        "json",
        from_json(
          col("kafka_value"),
          debeziumSchema
        )
      )
      .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),

        col("json.payload.before").alias("before"),
        col("json.payload.after").alias("after"),

        col("json.payload.op").alias("op"),

        col("json.payload.ts_ms").alias("event_ts"),

        col("json.payload.source.db").alias("database"),

        col("json.payload.source.table").alias("table"),

        col("json.payload.source.file").alias("binlog_file"),

        col("json.payload.source.pos").alias("binlog_position"),

        col("json.payload.source.snapshot")
          .alias("snapshot")
      )
  }

}