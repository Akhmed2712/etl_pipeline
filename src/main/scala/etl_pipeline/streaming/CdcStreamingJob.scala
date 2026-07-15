package etl_pipeline.streaming

import org.apache.spark.sql.SparkSession

object CdcStreamingJob {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder()
        .appName("CDC Streaming")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rawKafka =
      KafkaConsumer.read(
        spark = spark,
        brokers = "kafka:9092",
        topic = "farmagitechs.farmagitechs"
      )

    val parsed =
      DebeziumParser.parse(rawKafka)

    val routes =
      CdcRouter.route(parsed)

    routes.foreach {

      case (tableName, tableDf) =>

        MysqlSink.sink(
          df = tableDf,
          jdbcUrl = "jdbc:mysql://mysql-analytics:3306/mart",
          username = "root",
          password = "root",
          table = tableName,
          primaryKey = "id"
        )

    }

    spark.streams.awaitAnyTermination()

  }

}