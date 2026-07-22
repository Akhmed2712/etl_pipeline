package etl_pipeline.scripts.agg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgAggMutuFarmasi extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    // =====================================================
    // 1. TRIGGER RUNNER LOOP & GENERATE PER-SOURCE LOGS
    // =====================================================
    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    def getJdbcProps(config: CredentialsLoader.JdbcConfig): java.util.Properties = {
      val props = new java.util.Properties()
      config.props.foreach { case (k, v) => props.setProperty(k, v) }
      props
    }

    val dcPendaftaran = env("dc_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcLayananPendaftaran = env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcResep = env("dc_resep").asInstanceOf[Map[String, Any]]
    val dcTaskidBpjs = env("dc_taskid_bpjs").asInstanceOf[Map[String, Any]]

    // =====================================================
    // 2. READ & FILTER DATA SOURCES
    // =====================================================
    val confPendaftaran = readConfigs("dc_pendaftaran")
    val dfPendaftaran = spark.read
      .jdbc(
        confPendaftaran.url,
        s"${dcPendaftaran.getOrElse("database", dcPendaftaran.getOrElse("schema", ""))}.${dcPendaftaran("table")}",
        getJdbcProps(confPendaftaran)
      )
      .select("id", "no_register", "id_pasien")

    val confLayanan = readConfigs("dc_layanan_pendaftaran")
    val dfLayanan = spark.read
      .jdbc(
        confLayanan.url,
        s"${dcLayananPendaftaran.getOrElse("database", dcLayananPendaftaran.getOrElse("schema", ""))}.${dcLayananPendaftaran("table")}",
        getJdbcProps(confLayanan)
      )
      .select("id", "id_pendaftaran")

    val confResep = readConfigs("dc_resep")
    val dfResep = spark.read
      .jdbc(
        confResep.url,
        s"${dcResep.getOrElse("database", dcResep.getOrElse("schema", ""))}.${dcResep("table")}",
        getJdbcProps(confResep)
      )
      .select("id", "id_layanan_pendaftaran", "jenis_resep")
      .filter(dcResep("filter").toString)

    val confTaskId = readConfigs("dc_taskid_bpjs")
    val dfTaskId = spark.read
      .jdbc(
        confTaskId.url,
        s"${dcTaskidBpjs.getOrElse("database", dcTaskidBpjs.getOrElse("schema", ""))}.${dcTaskidBpjs("table")}",
        getJdbcProps(confTaskId)
      )
      .filter(col("task_id").isin(6, 7))
      .select("no_register", "task_id", "waktu_rs", "uploaded")

    // =====================================================
    // 3. JOIN & AGGREGATION LOGIC
    // =====================================================
    val dfJoined = dfPendaftaran.alias("dp")
      .join(
        dfLayanan.alias("dlp"),
        col("dp.id") === col("dlp.id_pendaftaran"),
        "left"
      )
      .join(
        dfResep.alias("dr"),
        col("dlp.id") === col("dr.id_layanan_pendaftaran"),
        "inner"
      )
      .join(
        dfTaskId.alias("dtb"),
        col("dp.no_register") === col("dtb.no_register"),
        "inner"
      )

    val dfAgg = dfJoined
      .groupBy(
        col("dr.id").alias("id_resep"),
        col("dp.no_register"),
        col("dp.id_pasien").alias("no_rm"),
        col("dr.jenis_resep")
      )
      .agg(
        max(when(col("dtb.task_id") === 6, col("dtb.waktu_rs"))).alias("waktu_task_6"),
        max(when(col("dtb.task_id") === 6, col("dtb.uploaded"))).alias("status_upload_task_6"),
        max(when(col("dtb.task_id") === 7, col("dtb.waktu_rs"))).alias("waktu_task_7"),
        max(when(col("dtb.task_id") === 7, col("dtb.uploaded"))).alias("status_upload_task_7")
      )

    // Variabel bantu untuk basis kalkulasi waktu (diambil dari waktu_task_6)
    val baseDate = to_date(col("waktu_task_6"))

    // Perhitungan SLA Farmasi & Ekstraksi Dimensi Waktu
    val dfResult = dfAgg
      .withColumn(
        "selisih_menit",
        (col("waktu_task_7").cast("long") - col("waktu_task_6").cast("long")) / 60
      )
      .withColumn(
        "status",
        when(
          col("jenis_resep") === "Racikan",
          when(col("selisih_menit") > 60, "Tidak Memenuhi").otherwise("Memenuhi")
        ).otherwise(
          when(col("selisih_menit") > 30, "Tidak Memenuhi").otherwise("Memenuhi")
        )
      )
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
      .withColumn("partition_date", baseDate)
      .select(
        col("id_resep"),
        col("no_register"),
        col("no_rm"),
        col("jenis_resep"),
        col("waktu_task_6"),
        col("status_upload_task_6"),
        col("waktu_task_7"),
        col("status_upload_task_7"),
        col("selisih_menit"),
        col("status"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"output count: ${dfResult.count()}")

    dfResult
  }
}