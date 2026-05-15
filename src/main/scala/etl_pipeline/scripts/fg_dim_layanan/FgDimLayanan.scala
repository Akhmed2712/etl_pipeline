package etl_pipeline.scripts.fg_dim_layanan

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgDimLayanan extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcSpesialisasi = env("dc_spesialisasi").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfSpesialisasi = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcSpesialisasi("database")}.${dcSpesialisasi("table")}", props)

    log.info(s"Source row count dc_penjamin: ${dfSpesialisasi.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfResult = dfSpesialisasi
      .select(
        "id", "nama"
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}