package etl_pipeline.scripts.fg_fact_retur_barang

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgFactReturBarang extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcRetur = env("dc_retur_penjualan").asInstanceOf[Map[String, Any]]
    val dcDetur = env("dc_detur_penjualan").asInstanceOf[Map[String, Any]]
    val dcLayananPendaftaran = env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfRetur = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcRetur("database")}.${dcRetur("table")}", props)
      .filter(dcRetur("filter").toString)

    val dfDetur = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcDetur("database")}.${dcDetur("table")}", props)
      .filter(dcDetur("filter").toString)

    val dfLayananPendaftaran = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcLayananPendaftaran("database")}.${dcLayananPendaftaran("table")}", props)
      //.filter(dcLayananPendaftaran("filter").toString)

    log.info(s"Source row count dc_retur: ${dfRetur.count()}")
    log.info(s"Source row count dc_detur: ${dfDetur.count()}")
    log.info(s"Source row count dc_layanan_pendaftaran: ${dfLayananPendaftaran.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfJoin = dfRetur.join(
        dfDetur,
        dfRetur("id").equalTo(dfDetur("id_retur_penjualan")),
        "inner"
    ).drop(dfRetur("id"), dfRetur("waktu")).join(
      dfLayananPendaftaran,
      dfDetur("id_layanan_pendaftaran").equalTo(dfLayananPendaftaran("id")),
      "left"
    ).drop(dfLayananPendaftaran("id"), dfLayananPendaftaran("waktu"))

    val dfResult = dfJoin.filter(col("id").isNotNull)
      .withColumn("partition_date", substring(col("waktu"), 1, 10))
      .withColumnRenamed("qty", "jumlah").select(
        "id", "waktu", "id_layanan_pendaftaran",  "id_kemasan", "jumlah", "partition_date"
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}