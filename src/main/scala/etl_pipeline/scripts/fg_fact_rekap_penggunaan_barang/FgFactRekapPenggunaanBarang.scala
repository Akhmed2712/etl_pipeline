package etl_pipeline.scripts.fg_fact_rekap_penggunaan_barang

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgFactRekapPenggunaanBarang extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val fgFactPenjualanBarang = env("fg_fact_penjualan_barang").asInstanceOf[Map[String, Any]]
    val fgFactReturBarang = env("fg_fact_retur_barang").asInstanceOf[Map[String, Any]]
    val fgDimPengemasanBarang = env("fg_dim_pengemasan_barang").asInstanceOf[Map[String, Any]]
    val fgDimPenjamin = env("fg_dim_penjamin").asInstanceOf[Map[String, Any]]
    val fgDimLayanan = env("fg_dim_layanan").asInstanceOf[Map[String, Any]]
    val fgDimWaktu = env("fg_dim_waktu").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfPenjualanBarang = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgFactPenjualanBarang("database")}.${fgFactPenjualanBarang("table")}", props)
      .filter(fgFactPenjualanBarang("filter").toString)
      .groupBy("waktu", "id_layanan_pendaftaran", "id_kemasan", "id_barang_paket", "id_penjamin", "id_unit_layanan", "hpp", "harga_jual", "jenis_layanan", "cara_bayar")
      .agg(sum("jumlah").as("jumlah"),
      first("partition_date").as("partition_date"))

    val dfReturBarang = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgFactReturBarang("database")}.${fgFactReturBarang("table")}", props)
      .groupBy("id_layanan_pendaftaran", "id_kemasan")
      .agg(sum("jumlah").as("jumlah_retur"))

    val dfPengemasanBarang = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgDimPengemasanBarang("database")}.${fgDimPengemasanBarang("table")}", props)

    val dfPenjamin = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgDimPenjamin("database")}.${fgDimPenjamin("table")}", props)
      .withColumnRenamed("nama", "penjamin")

    val dfLayanan = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgDimLayanan("database")}.${fgDimLayanan("table")}", props)
      .withColumnRenamed("nama", "klinik")

    val dfDimWaktu = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgDimWaktu("database")}.${fgDimWaktu("table")}", props)

    log.info(s"Source row count fg_fact_penjualan_barang_non_paket: ${dfPenjualanBarang.count()}")
    log.info(s"Source row count fg_fact_retur_barang: ${dfReturBarang.count()}")
    log.info(s"Source row count fg_dim_pengemasan_barang: ${dfPengemasanBarang.count()}")
    log.info(s"Source row count fg_dim_penjamin: ${dfPenjamin.count()}")
    log.info(s"Source row count fg_dim_layanan: ${dfLayanan.count()}")
    log.info(s"Source row count fg_dim_waktu: ${dfDimWaktu.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfJoin = dfPenjualanBarang
      .join(
        dfPengemasanBarang,
        when(
          dfPenjualanBarang("id_kemasan").isNotNull,
          dfPenjualanBarang("id_kemasan").equalTo(dfPengemasanBarang("id"))
        ).otherwise(
          dfPenjualanBarang("id_barang_paket").equalTo(dfPengemasanBarang("id_barang_paket"))
        ),
        "inner"
      )
      .join(
        dfPenjamin,
        dfPenjualanBarang("id_penjamin").equalTo(dfPenjamin("id")),
        "left"
      )
      .join(
        dfLayanan,
        dfPenjualanBarang("id_unit_layanan").equalTo(dfLayanan("id")),
        "left"
      )
      .join(
        dfReturBarang,
        Seq("id_kemasan", "id_layanan_pendaftaran"),
        "left"
      )
      .drop(dfPengemasanBarang("id"), dfPenjamin("id"), dfLayanan("id"))
      .join(
        dfDimWaktu,
        dfPenjualanBarang("partition_date").equalTo(dfDimWaktu("tanggal"))
      ).drop(dfDimWaktu("id"))

    val dfResult = dfJoin
      .withColumn("id", 
        coalesce(
          concat(
            regexp_replace(coalesce(col("partition_date"), lit("19700101")), "-", ""), 
            monotonically_increasing_id().cast("string")
          ).cast("long"),
          monotonically_increasing_id() 
        )
      )
      .withColumn("jumlah_retur", coalesce(col("jumlah_retur"), lit(0)))
      .withColumn("jumlah_pakai", col("jumlah") - col("jumlah_retur"))
      .withColumn("subtotal_hpp", col("hpp") * col("jumlah_pakai"))
      .withColumn("subtotal_harga_jual", col("harga_jual") * col("jumlah_pakai"))
      .withColumn("triwulan", concat(lit("Q"), col("triwulan")))
      .select(
        "id", "waktu", "kategori", "nama_barang", "jenis_layanan", 
        "klinik", "cara_bayar", "penjamin", "jumlah", "jumlah_retur", "jumlah_pakai", "hpp", "harga_jual",
        "subtotal_hpp", "subtotal_harga_jual", "tahun", "triwulan", "partition_date"
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}