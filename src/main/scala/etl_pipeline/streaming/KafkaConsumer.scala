package etl_pipeline.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaConsumer {

  def readStream(
      spark: SparkSession,
      bootstrapServers: String,
      topics: String,
      startingOffsets: String = "latest",
      failOnDataLoss: Boolean = "false"
  ): DataFrame = {

    spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        bootstrapServers
      )
      .option(
        "subscribe",
        topics
      )
      .option(
        "startingOffsets",
        startingOffsets
      )
      .option(
        "failOnDataLoss",
        failOnDataLoss
      )
      .load()
      .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "topic",
        "partition",
        "offset",
        "timestamp"
      )
  }

}