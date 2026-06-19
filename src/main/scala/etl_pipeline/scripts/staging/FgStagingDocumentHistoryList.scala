package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgStagingDocumentHistoryList extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
    spark: SparkSession,
    env: Map[String, Any],
    jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    // =========================================================================
    // 1. Source Config dari read_conf
    // =========================================================================

    val docHistoryConf =
      env("document_history_list")
        .asInstanceOf[Map[String, Any]]

    val props = new Properties()
    jdbcConfig.props.foreach {
      case (k, v) => props.setProperty(k, v)
    }

    // =========================================================================
    // 2. Read dari Postgres
    // =========================================================================

    val dfDocHistory = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${docHistoryConf("database")}.${docHistoryConf("table")}",
        props
      )
      .filter(docHistoryConf("filter").toString)
      .select(
        col("document_history_list_id").as("id"),
        col("created_time")
      )

    // =========================================================================
    // 3. Transform
    // =========================================================================

    val baseDate = to_date(col("created_time"))

    val windowLab = Window
      .partitionBy(baseDate)
      .orderBy(col("created_time").asc_nulls_last)

    val dfDocHistoryRanked = dfDocHistory
      .withColumn("urutan_lab", row_number().over(windowLab))

    val dfFinal = dfDocHistoryRanked
      .withColumnRenamed("document_history_list_id", "id")
      .withColumn("partition_date", baseDate)
      .withColumn("tahun",
          year(baseDate))
      .withColumn("triwulan",
          concat(lit("Q"), quarter(baseDate)))
      .withColumn("nama_bulan",
          when(month(baseDate) === 1,  "Januari")
          .when(month(baseDate) === 2,  "Februari")
          .when(month(baseDate) === 3,  "Maret")
          .when(month(baseDate) === 4,  "April")
          .when(month(baseDate) === 5,  "Mei")
          .when(month(baseDate) === 6,  "Juni")
          .when(month(baseDate) === 7,  "Juli")
          .when(month(baseDate) === 8,  "Agustus")
          .when(month(baseDate) === 9,  "September")
          .when(month(baseDate) === 10, "Oktober")
          .when(month(baseDate) === 11, "November")
          .otherwise("Desember")
      )
      .select(
        col("id"),
        col("created_time"),
        col("urutan_lab"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"Output row count: ${dfFinal.count()}")
    dfFinal
  }
}