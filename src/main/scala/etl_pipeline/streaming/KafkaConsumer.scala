package etl_pipeline.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaConsumer {

  /**
    * @param subscribePattern regex nama topic, mis. "^_\\.farmagitechs\\..*$"
    *   Dipakai supaya tabel baru yang di-tambahkan ke Debezium
    *   table.include.list otomatis ke-pickup tanpa restart job
    *   dengan config baru.
    */
  def readStream(
      spark: SparkSession,
      bootstrapServers: String,
      subscribePattern: String,
      startingOffsets: String = "latest",
      failOnDataLoss: String = "false"
  ): DataFrame = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribePattern", subscribePattern)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", failOnDataLoss)
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