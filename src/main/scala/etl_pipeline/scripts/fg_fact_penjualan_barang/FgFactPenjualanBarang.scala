package etl_pipeline.scripts.fg_fact_penjualan_barang

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgFactPenjualanBarang extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcPenjualan = env("dc_penjualan").asInstanceOf[Map[String, Any]]
    val dcDetailPenjualan = env("dc_detail_penjualan").asInstanceOf[Map[String, Any]]
    val dcLayananPendaftaran = env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]
    val fgDimPenjamin = env("fg_dim_penjamin").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfPenjualan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPenjualan("database")}.${dcPenjualan("table")}", props)
      .filter(dcPenjualan("filter").toString)

    val dfDetailPenjualan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcDetailPenjualan("database")}.${dcDetailPenjualan("table")}", props)
      .filter(dcDetailPenjualan("filter").toString)

    val dfLayananPendaftaran = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcLayananPendaftaran("database")}.${dcLayananPendaftaran("table")}", props)

    val dfDimPenjamin = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgDimPenjamin("database")}.${fgDimPenjamin("table")}", props)

    log.info(s"Source row count dc_penjualan: ${dfPenjualan.count()}")
    log.info(s"Source row count dc_detail_penjualan: ${dfDetailPenjualan.count()}")
    log.info(s"Source row count dc_layanan_pendaftaran: ${dfLayananPendaftaran.count()}")
    log.info(s"Source row count fg_dim_penjamin: ${dfDimPenjamin.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfJoin = dfPenjualan
      .join(
        dfDetailPenjualan,
        dfPenjualan("id").equalTo(dfDetailPenjualan("id_penjualan")),
        "inner"
      )
      .join(
        dfLayananPendaftaran,
        dfPenjualan("id_layanan_pendaftaran").equalTo(dfLayananPendaftaran("id")),
        "left"
      )
      .drop(
        dfLayananPendaftaran("id"),
        dfLayananPendaftaran("waktu"),
        dfPenjualan("id_penjamin"),
        dfPenjualan("id"),
        dfPenjualan("waktu"),
        dfPenjualan("jenis")
      )
      .join(
        dfDimPenjamin,
        dfLayananPendaftaran("id_penjamin").equalTo(dfDimPenjamin("id")),
        "left"
      )
      .withColumn("cara_bayar",
        when(lower(col("cara_bayar")).isNotNull &&
        trim(col("cara_bayar")) =!= "",
         dfLayananPendaftaran("cara_bayar"))
        .otherwise(dfDimPenjamin("jenis"))
      )
      .drop(
        dfDimPenjamin("id"),
        dfDimPenjamin("jenis")
      )

    val dfResult = dfJoin.filter(col("id").isNotNull)
      .withColumnRenamed("qty", "jumlah")
      .withColumnRenamed("jenis", "jenis_layanan")
      .withColumn("partition_date", substring(col("waktu"), 1, 10))
      .select(
        "id", "waktu", "id_layanan_pendaftaran", "jenis_layanan", "id_unit_layanan", "cara_bayar", "id_penjamin", "id_kemasan", "id_barang_paket", "jumlah", "harga_jual", "hpp", "partition_date"
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}