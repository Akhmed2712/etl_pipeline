package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactMutuHasilLabKritis extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
    spark: SparkSession,
    env: Map[String, Any],
    jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    val dcLaboratorium = env("dc_laboratorium").asInstanceOf[Map[String, Any]]
    val documentHistoryList = env("document_history_list").asInstanceOf[Map[String, Any]]

    val props = new Properties()
    jdbcConfig.props.foreach { case (k, v) =>
      props.setProperty(k, v)
    }

    // =========================
    // LOAD LAB
    // =========================
    val dfLab = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcLaboratorium("database")}.${dcLaboratorium("table")}",
        props
      )
      .select(
        col("id"),
        col("doc_his_id"),
        col("waktu_konfirm"),
        col("waktu_hasil")
      )

    // =========================
    // LOAD DOC HISTORY
    // =========================
    val dfDoc = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${documentHistoryList("database")}.${documentHistoryList("table")}",
        props
      )
      .select(
        col("id").as("staging_id"),
        col("created_time"),
        col("urutan_lab")
      )

    // =========================
    // JOIN BASED ON ID
    // =========================
    val dfJoin = dfLab
      .join(
        broadcast(dfDoc),
        dfLab("doc_his_id") === dfDoc("staging_id"),
        "left"
      )
      .withColumnRenamed("id", "lab_id")

    // =========================
    // HITUNG SELISIH WAKTU
    // =========================
    val dfWithTime = dfJoin
      .withColumn("waktu_unggah", col("created_time"))
      .withColumn(
        "selisih_menit",
        (
          unix_timestamp(col("waktu_unggah")) -
          unix_timestamp(col("waktu_hasil"))
        ) / 60
      )
      .withColumn("selisih_menit", col("selisih_menit").cast("long"))

    // =========================
    // STATUS LOGIC
    // =========================
    val dfStatus = dfWithTime.withColumn(
      "status",
        when(col("selisih_menit") <= 30, "Memenuhi")
          .otherwise("Tidak Memenuhi")
    )

    // =========================
    // DIMENSI WAKTU
    // =========================
    val dfFinal = dfStatus
      .withColumn("tahun", year(col("waktu_konfirm")))
      .withColumn("triwulan", concat(lit("Q"), quarter(col("waktu_konfirm"))))
      .withColumn(
        "nama_bulan",
        when(month(col("waktu_konfirm")) === 1, "01-Januari")
          .when(month(col("waktu_konfirm")) === 2, "02-Februari")
          .when(month(col("waktu_konfirm")) === 3, "03-Maret")
          .when(month(col("waktu_konfirm")) === 4, "04-April")
          .when(month(col("waktu_konfirm")) === 5, "05-Mei")
          .when(month(col("waktu_konfirm")) === 6, "06-Juni")
          .when(month(col("waktu_konfirm")) === 7, "07-Juli")
          .when(month(col("waktu_konfirm")) === 8, "08-Agustus")
          .when(month(col("waktu_konfirm")) === 9, "09-September")
          .when(month(col("waktu_konfirm")) === 10, "10-Oktober")
          .when(month(col("waktu_konfirm")) === 11, "11-November")
          .otherwise("12-Desember")
      )
      .withColumn("partition_date", to_date(col("waktu_konfirm")))
      .select(
        col("lab_id").as("id"),
        col("doc_his_id"),
        col("waktu_konfirm"),
        col("waktu_hasil"),
        col("waktu_unggah"),
        col("selisih_menit"),
        col("status"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"Output row count: ${dfFinal.count()}")

    dfFinal
  }
}