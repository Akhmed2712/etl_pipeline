
package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactPergerakanStok extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    val dcStok = env("dc_stok").asInstanceOf[Map[String, Any]]
    val dcPenerimaan = env("dc_penerimaan").asInstanceOf[Map[String, Any]]
    val dcSupplier = env("dc_supplier").asInstanceOf[Map[String, Any]]
    val fgDimPengemasanBarang =
      env("fg_dim_pengemasan_barang").asInstanceOf[Map[String, Any]]

    val props = new Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfStok = spark.read
      .jdbc(jdbcConfig.url, s"${dcStok("database")}.${dcStok("table")}", props)
      .filter(dcStok("filter").toString)

    val dfPenerimaan = spark.read
      .jdbc(jdbcConfig.url, s"${dcPenerimaan("database")}.${dcPenerimaan("table")}", props)
      .filter(dcPenerimaan("filter").toString)

    val dfSupplier = spark.read
      .jdbc(jdbcConfig.url, s"${dcSupplier("database")}.${dcSupplier("table")}", props)

    val dfBarang = spark.read
      .jdbc(jdbcConfig.url, s"${fgDimPengemasanBarang("database")}.${fgDimPengemasanBarang("table")}", props)
      .groupBy("id_barang")
      .agg(
        first("nama_barang").alias("nama_barang"),
        first("hpp").alias("hpp")
      )

    log.info(s"Source row count dc_stok: ${dfStok.count()}")
    log.info(s"Source row count dc_penerimaan: ${dfPenerimaan.count()}")
    log.info(s"Source row count dc_supplier: ${dfSupplier.count()}")
    log.info(s"Source row count fg_dim_pengemasan_barang: ${dfBarang.count()}")

    val windowStok = Window
      .partitionBy("id_barang", "nobatch")
      .orderBy(col("waktu"), col("id"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val dfJoin = dfStok
      .join(dfBarang, Seq("id_barang"), "left")
      .join(dfPenerimaan, dfStok("id_penerimaan") === dfPenerimaan("id"), "left")
      .join(dfSupplier, dfPenerimaan("id_supplier") === dfSupplier("id"), "left")
      .drop(dfPenerimaan("id"), dfSupplier("id"))

    val baseDate = to_date(col("waktu"))

    val dfResult = dfJoin
      .withColumn("total_masuk_kumulatif",
        sum(coalesce(col("masuk"), lit(0))).over(windowStok))
      .withColumn("total_kurang_kumulatif",
        sum(coalesce(col("keluar"), lit(0))).over(windowStok))
      .withColumn("sisa_stok_saat_ini",
        coalesce(col("stok_sesudah"),
          col("total_masuk_kumulatif") - col("total_kurang_kumulatif")))
      .withColumn("total_nilai_stok_bergerak",
        col("sisa_stok_saat_ini") * coalesce(col("hpp"), lit(0)))
      .withColumn("tahun", year(baseDate))
      .withColumn("triwulan", concat(lit("Q"), quarter(baseDate)))
      .withColumn("nama_bulan",
        when(month(baseDate)===1,"01-Januari")
        .when(month(baseDate)===2,"02-Februari")
        .when(month(baseDate)===3,"03-Maret")
        .when(month(baseDate)===4,"04-April")
        .when(month(baseDate)===5,"05-Mei")
        .when(month(baseDate)===6,"06-Juni")
        .when(month(baseDate)===7,"07-Juli")
        .when(month(baseDate)===8,"08-Agustus")
        .when(month(baseDate)===9,"09-September")
        .when(month(baseDate)===10,"10-Oktober")
        .when(month(baseDate)===11,"11-November")
        .otherwise("12-Desember"))
      .withColumn("partition_date", baseDate)
      .select(
        col("id"),
        col("id_barang"),
        col("nama_barang"),
        col("nobatch"),
        col("transaksi"),
        col("nama").alias("nama_supplier"),
        col("waktu"),
        col("masuk"),
        col("keluar"),
        col("stok_sesudah"),
        col("ed"),
        col("total_masuk_kumulatif"),
        col("total_kurang_kumulatif"),
        col("sisa_stok_saat_ini"),
        col("total_nilai_stok_bergerak"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"Output row count: ${dfResult.count()}")

    dfResult
  }
}
