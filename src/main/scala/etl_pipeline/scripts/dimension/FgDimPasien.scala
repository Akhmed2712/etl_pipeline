package etl_pipeline.scripts.dimension

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgDimPasien extends Transformer {

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

    // Ekstrak data konfigurasi dari YAML env
    val dcPasien = env("dc_pasien").asInstanceOf[Map[String, Any]]
    val dcKelurahan = env("dc_kelurahan").asInstanceOf[Map[String, Any]]
    val dcKecamatan = env("dc_kecamatan").asInstanceOf[Map[String, Any]]
    val dcKabupaten = env("dc_kabupaten").asInstanceOf[Map[String, Any]]
    val dcProvinsi = env("dc_provinsi").asInstanceOf[Map[String, Any]]

    // =====================================================
    // 2. READ DATA SOURCES
    // =====================================================

    val confPasien = readConfigs("dc_pasien")
    val dfPasien = spark.read.jdbc(
      confPasien.url,
      s"${dcPasien.getOrElse("database", dcPasien.getOrElse("schema", ""))}.${dcPasien("table")}",
      getJdbcProps(confPasien)
    )

    val confKelurahan = readConfigs("dc_kelurahan")
    val dfKelurahan = spark.read.jdbc(
      confKelurahan.url,
      s"${dcKelurahan.getOrElse("database", dcKelurahan.getOrElse("schema", ""))}.${dcKelurahan("table")}",
      getJdbcProps(confKelurahan)
    )

    val confKecamatan = readConfigs("dc_kecamatan")
    val dfKecamatan = spark.read.jdbc(
      confKecamatan.url,
      s"${dcKecamatan.getOrElse("database", dcKecamatan.getOrElse("schema", ""))}.${dcKecamatan("table")}",
      getJdbcProps(confKecamatan)
    )

    val confKabupaten = readConfigs("dc_kabupaten")
    val dfKabupaten = spark.read.jdbc(
      confKabupaten.url,
      s"${dcKabupaten.getOrElse("database", dcKabupaten.getOrElse("schema", ""))}.${dcKabupaten("table")}",
      getJdbcProps(confKabupaten)
    )

    val confProvinsi = readConfigs("dc_provinsi")
    val dfProvinsi = spark.read.jdbc(
      confProvinsi.url,
      s"${dcProvinsi.getOrElse("database", dcProvinsi.getOrElse("schema", ""))}.${dcProvinsi("table")}",
      getJdbcProps(confProvinsi)
    )

    // =====================================================
    // 3. JOIN & SELECT PROSES
    // =====================================================

    val dfResult = dfPasien.alias("dp")
      .join(
        dfKelurahan.alias("dk"),
        col("dp.id_kelurahan") === col("dk.id"),
        "left"
      )
      .join(
        dfKecamatan.alias("dk2"),
        col("dk.id_kecamatan") === col("dk2.id"),
        "left"
      )
      .join(
        dfKabupaten.alias("dk3"),
        col("dk2.id_kabupaten") === col("dk3.id"),
        "left"
      )
      .join(
        dfProvinsi.alias("dp2"),
        col("dk3.id_provinsi") === col("dp2.id"),
        "left"
      )
      .select(
        col("dp.id").alias("id_pasien"),
        lpad(col("dp.id"), 6, "0").alias("no_rm"),
        col("dp.nama").alias("nama_pasien"),
        col("dp.kelamin").alias("jenis_kelamin"),
        col("dp.tanggal_lahir"),
        col("dp.gol_darah").alias("golongan_darah"),
        col("dk.nama").alias("kelurahan"),
        col("dk2.nama").alias("kecamatan"),
        col("dk3.nama").alias("kabupaten"),
        col("dp2.nama").alias("provinsi")
      )

    log.info(s"output count: ${dfResult.count()}")

    dfResult
  }
}