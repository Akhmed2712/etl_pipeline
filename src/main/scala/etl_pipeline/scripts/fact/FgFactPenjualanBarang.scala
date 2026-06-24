package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactPenjualanBarang extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    val dcPenjualan =
      env("dc_penjualan").asInstanceOf[Map[String, Any]]

    val dcDetailPenjualan =
      env("dc_detail_penjualan").asInstanceOf[Map[String, Any]]

    val dcResep =
      env("dc_resep").asInstanceOf[Map[String, Any]]

    val dcResepBebas =
      env("dc_resep_bebas").asInstanceOf[Map[String, Any]]

    val dcLayananPendaftaran =
      env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]

    val dcPendaftaran =
      env("dc_pendaftaran").asInstanceOf[Map[String, Any]]

    val fgDimPengemasanBarang =
      env("fg_dim_pengemasan_barang").asInstanceOf[Map[String, Any]]

    val dcStok =
      env("dc_stok").asInstanceOf[Map[String, Any]]

    val props = new Properties()

    jdbcConfig.props.foreach {
      case (k, v) => props.setProperty(k, v)
    }

    val dfPenjualan = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcPenjualan("database")}.${dcPenjualan("table")}",
        props
      )
      .filter(dcPenjualan("filter").toString)
      .drop("id_pasien")

    val dfDetailPenjualan = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcDetailPenjualan("database")}.${dcDetailPenjualan("table")}",
        props
      )
      .filter(dcDetailPenjualan("filter").toString)

    val dfResep = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcResep("database")}.${dcResep("table")}",
        props
      )
      .filter(dcResep("filter").toString)
      .select(
        col("id"),
        col("waktu")
      )

    val dfResepBebas = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcResepBebas("database")}.${dcResepBebas("table")}",
        props
      )
      .filter(dcResepBebas("filter").toString)
      .select(
        col("id"),
        col("waktu")
      )

    val dfLayananPendaftaran = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcLayananPendaftaran("database")}.${dcLayananPendaftaran("table")}",
        props
      )

    val dfPendaftaran = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcPendaftaran("database")}.${dcPendaftaran("table")}",
        props
      )
      .filter(dcPendaftaran("filter").toString)
      .select(
        col("id"),
        col("id_pasien")
      )

    val dfPengemasanBarang = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${fgDimPengemasanBarang("database")}.${fgDimPengemasanBarang("table")}",
        props
      )
      .select(
        col("id"),
        col("id_barang")
      )

    val windowStok = Window
      .partitionBy("id_penjualan", "id_barang")
      .orderBy(col("id").desc)

    val dfStok = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcStok("database")}.${dcStok("table")}",
        props
      )
      .filter(dcDetailPenjualan("filter").toString)
      .select(
        col("id"),
        col("id_penjualan"),
        col("nobatch"),
        col("id_unit"),
        col("id_barang"),
        col("waktu"),
        col("keluar")
      )
      .withColumn(
        "nobatch",
        when(
          trim(coalesce(col("nobatch"), lit(""))) === "",
          lit("nobatch kosong")
        ).otherwise(col("nobatch"))
      )
      .withColumn(
        "rn",
        row_number().over(windowStok)
      )

    val dfJoinSatu = dfPenjualan
      .join(
        dfDetailPenjualan,
        dfPenjualan("id") === dfDetailPenjualan("id_penjualan"),
        "inner"
      )
      .join(
        dfResep,
        dfResep("id") === dfPenjualan("id_resep"),
        "left"
      )
      .join(
        dfResepBebas,
        dfPenjualan("id_resep").isNull &&
          dfResepBebas("id") === dfPenjualan("id_resep_bebas"),
        "left"
      )
      .join(
        dfPengemasanBarang,
        dfDetailPenjualan("id_kemasan") === dfPengemasanBarang("id"),
        "inner"
      )
      .join(
        dfLayananPendaftaran,
        dfPenjualan("id_layanan_pendaftaran") ===
          dfLayananPendaftaran("id"),
        "left"
      )
      .join(
        dfPendaftaran,
        dfPendaftaran("id") ===
          dfLayananPendaftaran("id_pendaftaran"),
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
        dfResepBebas("id"),
        dfResepBebas("waktu"),
        dfPendaftaran("id"),
        dfPenjualan("id"),
        dfPenjualan("id_unit"),
        dfDetailPenjualan("id_unit")
      )

    val dfRank = dfJoinSatu.withColumn(
      "rn",
      row_number().over(
        Window
          .partitionBy("id_penjualan", "id_barang")
          .orderBy(col("id").desc)
      )
    )

    val dfJoin = dfRank
      .join(
        dfStok,
        dfRank("id_penjualan") === dfStok("id_penjualan") &&
          dfRank("id_barang") === dfStok("id_barang") &&
          dfRank("rn") === dfStok("rn"),
        "left"
      )
      .drop(
        dfStok("id"),
        dfStok("waktu"),
        dfStok("id_penjualan")
      )

    val dfFinal = dfJoin
      .filter(col("id").isNotNull)
      .withColumnRenamed("qty", "jumlah")
      .withColumnRenamed("jenis", "jenis_layanan")
      .withColumn(
        "partition_date",
        substring(col("waktu"), 1, 10)
      )
      .select(
        col("id"),
        col("waktu"),
        col("id_layanan_pendaftaran"),
        col("id_pasien"),
        col("id_dokter"),
        col("id_penjualan"),
        col("jenis_layanan"),
        col("id_unit_layanan"),
        col("cara_bayar"),
        col("id_penjamin"),
        col("id_kemasan"),
        col("id_barang_paket"),
        col("id_resep"),
        col("id_resep_bebas"),
        col("nobatch"),
        col("id_unit"),
        col("jumlah"),
        col("hna"),
        col("hpp"),
        col("harga_jual"),
        col("partition_date")
      )

    log.info(s"Output row count: ${dfFinal.count()}")

    dfFinal
  }
}