package etl_pipeline.scripts.fact

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, col}
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
    val dcResep = env("dc_resep").asInstanceOf[Map[String, Any]]
    val dcResepBebas = env("dc_resep_bebas").asInstanceOf[Map[String, Any]]
    val dcLayananPendaftaran = env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcPendaftaran = env("dc_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcPenjamin = env("dc_penjamin").asInstanceOf[Map[String, Any]]
    val fgDimPengemasanBarang = env("fg_dim_pengemasan_barang").asInstanceOf[Map[String, Any]]
    val dcStok = env("dc_stok").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfPenjualan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPenjualan("database")}.${dcPenjualan("table")}", props)
      .filter(dcPenjualan("filter").toString)
      .drop(col("id_pasien"))

    val dfDetailPenjualan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcDetailPenjualan("database")}.${dcDetailPenjualan("table")}", props)
      .filter(dcDetailPenjualan("filter").toString)

    val dfResep = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcResep("database")}.${dcResep("table")}", props)
      .filter(dcResep("filter").toString)
      .select("id", "waktu")

    val dfResepBebas = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcResepBebas("database")}.${dcResepBebas("table")}", props)
      .filter(dcResepBebas("filter").toString)
      .select("id", "waktu")

    val dfLayananPendaftaran = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcLayananPendaftaran("database")}.${dcLayananPendaftaran("table")}", props)

    val dfPendaftaran = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPendaftaran("database")}.${dcPendaftaran("table")}", props)
      .filter(dcPendaftaran("filter").toString)
      .select("id", "id_pasien")

    val dfDimPenjamin = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPenjamin("database")}.${dcPenjamin("table")}", props)

    val dfPengemasanBarang = spark.read
      .jdbc(url = jdbcConfig.url, s"${fgDimPengemasanBarang("database")}.${fgDimPengemasanBarang("table")}", props)
      .select("id", "id_barang")

    val dfStok = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcStok("database")}.${dcStok("table")}", props)
      .filter(dcDetailPenjualan("filter").toString)
      .select("id", "id_penjualan", "nobatch", "id_unit", "id_barang", "waktu", "keluar")
      .withColumn(
        "nobatch",
        when(
          trim(coalesce(col("nobatch"), lit(""))) === "",
          lit("nobatch kosong")
        ).otherwise(col("nobatch"))
      )
      .withColumn(
        "rn",
        row_number().over(
          Window
            .partitionBy("id_penjualan", "id_barang")
            .orderBy(col("id").desc)
        )
      )

    log.info(s"Source row count dc_penjualan: ${dfPenjualan.count()}")
    log.info(s"Source row count dc_detail_penjualan: ${dfDetailPenjualan.count()}")
    log.info(s"Source row count dc_resep: ${dfResep.count()}")
    log.info(s"Source row count dc_resep_bebas: ${dfResepBebas.count()}")
    log.info(s"Source row count dc_layanan_pendaftaran: ${dfLayananPendaftaran.count()}")
    log.info(s"Source row count dc_pendaftaran: ${dfPendaftaran.count()}")
    log.info(s"Source row count dc_penjamin: ${dfDimPenjamin.count()}")
    log.info(s"Source row count fg_dim_pengemasan_barang: ${dfPengemasanBarang.count()}")
    log.info(s"Source row count dc_stok: ${dfStok.count()}")

    // -- Transformation logic -------------------------------------------------
    // After the dfResep left join, add dfResepBebas join
  val dfJoinSatu = dfPenjualan
    .join(
      dfDetailPenjualan,
      dfPenjualan("id").equalTo(dfDetailPenjualan("id_penjualan")),
      "inner"
    )
    .join(
      dfResep,
      dfResep("id").equalTo(dfPenjualan("id_resep")),
      "left"
    )
    .join(
      dfResepBebas,
      dfPenjualan("id_resep").isNull &&
      dfResepBebas("id").equalTo(dfPenjualan("id_resep_bebas")),  // <-- new join
      "left"
    )
    .join(
      dfPengemasanBarang,
      dfDetailPenjualan("id_kemasan").equalTo(dfPengemasanBarang("id")),
      "inner"
    )
    .join(
      dfLayananPendaftaran,
      dfPenjualan("id_layanan_pendaftaran").equalTo(dfLayananPendaftaran("id")),
      "left"
    )
    .join(
      dfPendaftaran,
      dfPendaftaran("id").equalTo(dfLayananPendaftaran("id_pendaftaran")),
      "left"
    )
    .drop(
      dfLayananPendaftaran("id"),
      dfLayananPendaftaran("waktu"),
      dfPenjualan("id_penjamin"),
      dfPenjualan("waktu"),
      dfPenjualan("jenis"),
      dfPengemasanBarang("id"),
      dfResep("id"),
      dfResep("waktu"),
      dfResepBebas("id"),      // <-- drop to avoid ambiguity
      dfResepBebas("waktu"),   // <-- drop resep bebas waktu too
      dfPendaftaran("id"),
      dfPenjualan("id"),
      dfPenjualan("id_unit"),
      dfDetailPenjualan("id_unit"),
    )
    // ... rest unchanged

    val dfRank = dfJoinSatu.withColumn("rn",
      row_number().over(
        Window
          .partitionBy("id_penjualan", "id_barang")
          .orderBy(col("id").desc)
      )
    )

    val dfJoin = dfRank.join(
      dfStok,
      dfRank("id_penjualan").equalTo(dfStok("id_penjualan")) &&
      dfRank("id_barang").equalTo(dfStok("id_barang")) &&
      dfRank("rn").equalTo(dfStok("rn")),
      "left"
    ).drop(
      dfStok("id"),
      dfStok("waktu"),
      dfStok("id_penjualan")
    )

    val dfResult = dfJoin.filter(col("id").isNotNull)
      .withColumnRenamed("qty", "jumlah")
      .withColumnRenamed("jenis", "jenis_layanan")
      .withColumn("partition_date", substring(col("waktu"), 1, 10))
      .select(
      "id", "waktu", "id_layanan_pendaftaran", "id_pasien", "id_dokter", "id_penjualan",
      "jenis_layanan", "id_unit_layanan", "cara_bayar", "id_penjamin",
      "id_kemasan", "id_barang_paket", "id_resep", "id_resep_bebas",
      "nobatch", "id_unit", "jumlah", "hna", "hpp", "harga_jual", "partition_date"
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}