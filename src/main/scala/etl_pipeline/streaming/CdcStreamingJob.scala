package etl_pipeline.streaming

import org.apache.spark.sql.SparkSession
import etl_pipeline.utils.CredentialsLoader

object CdcStreamingJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CDC Streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schemas = CdcSchemaLoader.loadAll()

    // TODO Tahap 5: generalisasi ke subscribePattern kalau tabel > 1
    val rawKafka = KafkaConsumer.readStream(
    spark = spark,
    bootstrapServers = "kafka:9092",
    subscribePattern = "^simrs\\.farmagitechs\\..*$"
    )

    val parsed = DebeziumParser.parse(rawKafka)

    val query = parsed.writeStream
      .foreachBatch { (batchDf, batchId: Long) =>

        val perTable = CdcRouter.route(batchDf, schemas)

        perTable.foreach { case (tableName, tableDf) =>
          val tableSchema = schemas(tableName)

          val jdbcConfig = CredentialsLoader.loadWriteCredentials(
            Map(
              "write_credentials_key" -> tableSchema.writeCredentialsKey,
              "write_database"        -> tableSchema.writeDatabase,
              "table"                 -> tableSchema.writeTable
            )
          )

          MysqlSink.sink(tableDf, jdbcConfig, tableSchema.writeTable, tableSchema.primaryKey)
        }
      }
      .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/cdc")
      .start()

    query.awaitTermination()
  }
}