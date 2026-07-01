package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactMutuRawatJalan extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any], // Berisi data map dari YAML script ini
      jdbcConfig: CredentialsLoader.JdbcConfig // Ini write config bawaan runner utama
  ): DataFrame = {

    // =====================================================
    // 1. TRIGGER RUNNER LOOP & GENERATE PER-SOURCE LOGS
    // =====================================================
    // Baris ini memicu penjelajahan source dan mencetak seluruh detail JDBC ke terminal
    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    // Helper internal untuk mengekstrak java.util.Properties per source config
    def getJdbcProps(config: CredentialsLoader.JdbcConfig): Properties = {
      val props = new Properties()
      config.props.foreach { case (k, v) => props.setProperty(k, v) }
      props
    }

    // Ekstrak data konfigurasi mentah dari YAML env
    val dcPendaftaran = env("dc_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcLayananPendaftaran = env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcSpesialisasi = env("dc_spesialisasi").asInstanceOf[Map[String, Any]]
    val dcTaskidBpjs = env("dc_taskid_bpjs").asInstanceOf[Map[String, Any]]
    val dcNakes = env("dc_nakes").asInstanceOf[Map[String, Any]]
    val dcPegawai = env("dc_pegawai").asInstanceOf[Map[String, Any]]

    // Formula pembersihan No RM
    val noRmClean =
      lpad(
        substring(
          regexp_replace(col("id_pasien").cast("string"), "^0+", ""),
          -6,
          6
        ),
        6,
        "0"
      )

    // =====================================================
    // 2. READ (Menggunakan Config Spesifik Tiap Source)
    // =====================================================

    val confPendaftaran = readConfigs("dc_pendaftaran")
    val dbPendaftaran = dcPendaftaran.getOrElse("database", dcPendaftaran.getOrElse("schema", ""))
    val dfPendaftaran = spark.read
      .jdbc(
        confPendaftaran.url,
        s"$dbPendaftaran.${dcPendaftaran("table")}",
        getJdbcProps(confPendaftaran)
      )
      .filter(dcPendaftaran("filter").toString)
      .select(
        col("id").as("dp_id"),
        noRmClean.as("no_rm"),
        col("no_register"),
        col("jenis"),
        col("status"),
        col("rawat"),
        col("waktu_daftar")
      )

    val confLayanan = readConfigs("dc_layanan_pendaftaran")
    val dbLayanan = dcLayananPendaftaran.getOrElse("database", dcLayananPendaftaran.getOrElse("schema", ""))
    val dfLayanan = spark.read
      .jdbc(
        confLayanan.url,
        s"$dbLayanan.${dcLayananPendaftaran("table")}",
        getJdbcProps(confLayanan)
      )
      .filter(dcLayananPendaftaran("filter").toString)
      .select(
        col("id"),
        col("id_pendaftaran"),
        col("id_unit_layanan"),
        col("id_dokter"),
        col("waktu")
      )

    val confSpesialisasi = readConfigs("dc_spesialisasi")
    val dbSpesialisasi = dcSpesialisasi.getOrElse("database", dcSpesialisasi.getOrElse("schema", ""))
    val dfSpesialisasi = spark.read
      .jdbc(
        confSpesialisasi.url,
        s"$dbSpesialisasi.${dcSpesialisasi("table")}",
        getJdbcProps(confSpesialisasi)
      )
      .select(
        col("id").as("id_spesialisasi"),
        col("nama").as("spesialisasi")
      )

    val confTaskid = readConfigs("dc_taskid_bpjs")
    val dbTaskid = dcTaskidBpjs.getOrElse("database", dcTaskidBpjs.getOrElse("schema", ""))
    val dfTaskid = spark.read
      .jdbc(
        confTaskid.url,
        s"$dbTaskid.${dcTaskidBpjs("table")}",
        getJdbcProps(confTaskid)
      )
      .select(
        col("no_register"),
        col("task_id"),
        col("waktu_rs")
      )

    val confNakes = readConfigs("dc_nakes")
    val dbNakes = dcNakes.getOrElse("database", dcNakes.getOrElse("schema", ""))
    val dfNakes = spark.read
      .jdbc(
        confNakes.url,
        s"$dbNakes.${dcNakes("table")}",
        getJdbcProps(confNakes)
      )
      .select(
        col("id"),
        col("id_pegawai")
      )

    val confPegawai = readConfigs("dc_pegawai")
    val dbPegawai = dcPegawai.getOrElse("database", dcPegawai.getOrElse("schema", ""))
    val dfPegawai = spark.read
      .jdbc(
        confPegawai.url,
        s"$dbPegawai.${dcPegawai("table")}",
        getJdbcProps(confPegawai)
      )
      .select(
        col("id").as("pegawai_id"),
        col("nama").as("nama_dokter")
      )

    // =====================================================
    // TRANSFORMATION & LOGIC (WINDOW, FILTER, JOIN)
    // =====================================================

    val windowLayanan = Window
      .partitionBy("id_pendaftaran")
      .orderBy(col("waktu").asc)

    val dfLayananPertama = dfLayanan
      .withColumn("rn", row_number().over(windowLayanan))
      .filter(col("rn") === 1)
      .drop("rn")

    val dfTask3 = dfTaskid
      .filter(col("task_id") === 3)
      .select(
        col("no_register").as("no_register_t3"),
        col("waktu_rs").as("waktu_task_3")
      )

    val dfTask4 = dfTaskid
      .filter(col("task_id") === 4)
      .select(
        col("no_register").as("no_register_t4"),
        col("waktu_rs").as("waktu_task_4")
      )

    val dfJoined = dfPendaftaran
      .join(dfLayananPertama, dfPendaftaran("dp_id") === dfLayananPertama("id_pendaftaran"), "inner")
      .join(dfSpesialisasi, dfLayananPertama("id_unit_layanan") === dfSpesialisasi("id_spesialisasi"), "left")
      .join(dfNakes, dfLayananPertama("id_dokter") === dfNakes("id"), "left")
      .join(dfPegawai, dfNakes("id_pegawai") === dfPegawai("pegawai_id"), "left")
      .join(dfTask3, dfPendaftaran("no_register") === dfTask3("no_register_t3"), "inner")
      .join(dfTask4, dfPendaftaran("no_register") === dfTask4("no_register_t4"), "inner")

    val selisihMenit = (unix_timestamp(col("waktu_task_4")) - unix_timestamp(col("waktu_task_3"))) / 60
    val baseDate = to_date(col("waktu_task_3"))

    val dfFinal = dfJoined
      .withColumn("selisih_menit", selisihMenit.cast("long"))
      .withColumn(
        "status",
        when(
          lower(col("spesialisasi")) === "gigi" ||
            lower(col("jenis")) === "mcu" ||
            lower(col("spesialisasi")).contains("mcu"),
          "Eksklusi"
        )
          .when(col("selisih_menit") <= 60, "Memenuhi")
          .otherwise("Tidak Memenuhi")
      )
      .withColumn("partition_date", baseDate)
      .withColumn("tahun", year(baseDate))
      .withColumn("triwulan", concat(lit("Q"), quarter(baseDate)))
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
        col("dp_id").as("id").cast("bigint"),
        col("no_register"),
        lpad(col("no_rm").cast("string"), 6, "0").as("no_rm"),
        col("nama_dokter"),
        col("spesialisasi"),
        col("waktu_task_3"),
        col("waktu_task_4"),
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