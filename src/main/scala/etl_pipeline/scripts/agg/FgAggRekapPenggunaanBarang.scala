package etl_pipeline.scripts.agg

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgAggRekapPenggunaanBarang extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    val fgFactPenjualanBarang =
      env("fg_fact_penjualan_barang").asInstanceOf[Map[String, Any]]

    val fgFactReturBarang =
      env("fg_fact_retur_barang").asInstanceOf[Map[String, Any]]

    val fgDimPengemasanBarang =
      env("fg_dim_pengemasan_barang").asInstanceOf[Map[String, Any]]

    val dcPenjamin =
      env("dc_penjamin").asInstanceOf[Map[String, Any]]

    val dcSpesialisasi =
      env("dc_spesialisasi").asInstanceOf[Map[String, Any]]

    val fgDimNakes =
      env("fg_dim_nakes").asInstanceOf[Map[String, Any]]

    val props = new Properties()

    jdbcConfig.props.foreach {
      case (k, v) =>
        props.setProperty(k, v)
    }

    val dfPenjualanBarang = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${fgFactPenjualanBarang("database")}.${fgFactPenjualanBarang("table")}",
        props
      )
      .filter(fgFactPenjualanBarang("filter").toString)
      .groupBy(
        "waktu",
        "id_layanan_pendaftaran",
        "id_dokter",
        "id_kemasan",
        "id_barang_paket",
        "nobatch",
        "id_penjamin",
        "id_unit_layanan",
        "hna",
        "hpp",
        "harga_jual",
        "jenis_layanan",
        "cara_bayar"
      )
      .agg(
        sum("jumlah").as("jumlah"),
        first("partition_date").as("partition_date")
      )

    val dfReturBarang = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${fgFactReturBarang("database")}.${fgFactReturBarang("table")}",
        props
      )
      .groupBy(
        "id_layanan_pendaftaran",
        "id_kemasan"
      )
      .agg(
        sum("jumlah").as("jumlah_retur")
      )

    val dfPengemasanBarang = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${fgDimPengemasanBarang("database")}.${fgDimPengemasanBarang("table")}",
        props
      )
      .drop(
        "hna",
        "hpp",
        "harga_jual"
      )

    val dfPenjamin = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcPenjamin("database")}.${dcPenjamin("table")}",
        props
      )
      .select(
        col("id"),
        col("nama").as("penjamin")
      )

    val dfSpesialisasi = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcSpesialisasi("database")}.${dcSpesialisasi("table")}",
        props
      )
      .select(
        col("id"),
        col("nama").as("klinik")
      )

    val dfDimNakes = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${fgDimNakes("database")}.${fgDimNakes("table")}",
        props
      )
      .withColumnRenamed("nama", "nama_dokter")

    log.info(s"Source row count fg_fact_penjualan_barang: ${dfPenjualanBarang.count()}")
    log.info(s"Source row count fg_fact_retur_barang: ${dfReturBarang.count()}")
    log.info(s"Source row count fg_dim_pengemasan_barang: ${dfPengemasanBarang.count()}")
    log.info(s"Source row count dc_penjamin: ${dfPenjamin.count()}")
    log.info(s"Source row count dc_spesialisasi: ${dfSpesialisasi.count()}")
    log.info(s"Source row count fg_dim_nakes: ${dfDimNakes.count()}")

    val dfJoin = dfPenjualanBarang
      .join(
        dfPengemasanBarang,
        when(
          dfPenjualanBarang("id_kemasan").isNotNull,
          dfPenjualanBarang("id_kemasan") === dfPengemasanBarang("id")
        ).otherwise(
          dfPenjualanBarang("id_barang_paket") ===
            dfPengemasanBarang("id_barang_paket")
        ),
        "inner"
      )
      .join(
        dfPenjamin,
        dfPenjualanBarang("id_penjamin") === dfPenjamin("id"),
        "left"
      )
      .join(
        dfSpesialisasi,
        dfPenjualanBarang("id_unit_layanan") === dfSpesialisasi("id"),
        "left"
      )
      .join(
        dfReturBarang,
        Seq("id_kemasan", "id_layanan_pendaftaran"),
        "left"
      )
      .drop(
        dfPengemasanBarang("id"),
        dfPenjamin("id"),
        dfSpesialisasi("id")
      )
      .join(
        dfDimNakes,
        dfPenjualanBarang("id_dokter") === dfDimNakes("id"),
        "left"
      )
      .drop(dfDimNakes("id"))

    val baseDate =
      to_date(col("partition_date"))

    val dfResult = dfJoin
      .withColumn(
        "id",
        concat(
          date_format(baseDate, "yyyyMMdd"),
          lpad(
            row_number()
              .over(
                Window
                  .partitionBy("partition_date")
                  .orderBy(lit(1))
              )
              .cast("string"),
            8,
            "0"
          )
        ).cast("long")
      )
      .withColumn(
        "jumlah_retur",
        coalesce(col("jumlah_retur"), lit(0))
      )
      .withColumn(
        "jumlah_pakai",
        col("jumlah") - col("jumlah_retur")
      )
      .withColumn(
        "subtotal_hna",
        col("hna") * col("jumlah_pakai")
      )
      .withColumn(
        "subtotal_hpp",
        col("hpp") * col("jumlah_pakai")
      )
      .withColumn(
        "subtotal_harga_jual",
        col("harga_jual") * col("jumlah_pakai")
      )
      .withColumn(
        "tahun",
        year(baseDate)
      )
      .withColumn(
        "triwulan",
        concat(lit("Q"), quarter(baseDate))
      )
      .withColumn(
        "nama_bulan",
        when(month(baseDate) === 1, "01-Januari")
          .when(month(baseDate) === 2, "02-Februari")
          .when(month(baseDate) === 3, "03-Maret")
          .when(month(baseDate) === 4, "04-April")
          .when(month(baseDate) === 5, "05-Mei")
          .when(month(baseDate) === 6, "06-Juni")
          .when(month(baseDate) === 7, "07-Juli")
          .when(month(baseDate) === 8, "08-Agustus")
          .when(month(baseDate) === 9, "09-September")
          .when(month(baseDate) === 10, "10-Oktober")
          .when(month(baseDate) === 11, "11-November")
          .otherwise("12-Desember")
      )
      .select(
        col("id"),
        col("waktu"),
        col("kategori"),
        col("nama_barang"),
        col("nobatch"),
        col("nama_dokter"),
        col("spesialisasi"),
        col("jenis_layanan"),
        col("klinik"),
        col("cara_bayar"),
        col("penjamin"),
        col("jumlah"),
        col("jumlah_retur"),
        col("jumlah_pakai"),
        col("hna"),
        col("hpp"),
        col("harga_jual"),
        col("subtotal_hna"),
        col("subtotal_hpp"),
        col("subtotal_harga_jual"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"Output row count: ${dfResult.count()}")

    dfResult
  }
}