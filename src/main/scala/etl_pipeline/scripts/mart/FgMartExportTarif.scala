package etl_pipeline.scripts.mart

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgMartExportTarif extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcKomponenTarif = env("dc_komponen_tarif").asInstanceOf[Map[String, Any]]
    val dcLayanan = env("dc_layanan").asInstanceOf[Map[String, Any]]
    val dcBobot = env("dc_bobot").asInstanceOf[Map[String, Any]]
    val dcUnit = env("dc_unit").asInstanceOf[Map[String, Any]]
    val stgDetail = env("fg_staging_komponen_tarif_detail").asInstanceOf[Map[String, Any]]
    
    // Ambil langsung source catalog untuk kelas (bukan dari staging)
    val dcKelasKomponenTarif = env("dc_kelas_komponen_tarif").asInstanceOf[Map[String, Any]]
    val dcKelas = env("dc_kelas").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcKomponenTarif("database")}.${dcKomponenTarif("table")}", props)
      .select("id", "id_layanan", "id_unit", "id_bobot", "jenis", "keterangan", "active")

    val dfLayanan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcLayanan("database")}.${dcLayanan("table")}", props)
      .select("id", "nama", "id_parent", "id_jenis_pemeriksaan")

    val dfLayananParent = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcLayanan("database")}.${dcLayanan("table")}", props)
      .select("id", "nama")

    val dfBobot = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcBobot("database")}.${dcBobot("table")}", props)
      .select("id", "nama")

    val dfUnit = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcUnit("database")}.${dcUnit("table")}", props)
      .select("id", "nama")

    val dfDetail = spark.read
      .jdbc(url = jdbcConfig.url, s"${stgDetail("database")}.${stgDetail("table")}", props)

    // Load data relasi kelas langsung dari source
    val dfKelasKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcKelasKomponenTarif("database")}.${dcKelasKomponenTarif("table")}", props)
      .select("id_komponen_tarif", "id_kelas")

    val dfKelas = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcKelas("database")}.${dcKelas("table")}", props)
      .select("id", "nama")

    log.info(s"Source row count dc_komponen_tarif: ${dfKomponenTarif.count()}")
    log.info(s"Source row count dc_layanan: ${dfLayanan.count()}")
    log.info(s"Source row count dc_kelas_komponen_tarif: ${dfKelasKomponenTarif.count()}")
    log.info(s"Source row count fg_staging_komponen_tarif_detail: ${dfDetail.count()}")

    // -- Transformation logic -------------------------------------------------
    
    // Hubungkan tabel pemetaan kelas dengan master nama kelas terlebih dahulu
    val dfMasterKelas = dfKelasKomponenTarif
      .join(
        dfKelas,
        dfKelas("id") === dfKelasKomponenTarif("id_kelas"),
        "inner"
      )
      .select(
        dfKelasKomponenTarif("id_komponen_tarif"),
        dfKelas("nama").as("nama_kelas")
      )

    // Gabungkan semua ke dfKomponenTarif
    val dfResult = dfKomponenTarif
    .join(
        dfLayanan,
        dfLayanan("id") === dfKomponenTarif("id_layanan"),
        "inner"
    )
    .join(
        dfBobot,
        dfBobot("id") === dfKomponenTarif("id_bobot"),
        "left"
    )
    .join(
        dfLayananParent,
        dfLayananParent("id") === dfLayanan("id_parent"),
        "left"
    )
    .join(
        dfUnit,
        dfUnit("id") === dfKomponenTarif("id_unit"),
        "left"
    )
    .join(
        dfMasterKelas,
        dfMasterKelas("id_komponen_tarif") === dfKomponenTarif("id"),
        "left"
    )
    .join(
        dfDetail,
        dfDetail("id_komponen_tarif") === dfKomponenTarif("id"),
        "left"
    )
    .select(
        dfKomponenTarif("id"),
        dfKomponenTarif("jenis"),
        dfKomponenTarif("keterangan"),
        dfKomponenTarif("active"),

        // bobot
        coalesce(dfBobot("nama"), lit("")).as("bobot"),

        // layanan (Plain text bersih, tanpa tag HTML)
        when(
            dfLayananParent("nama").isNotNull,
            concat_ws(
                " ",
                dfLayanan("nama"),
                concat(lit("("), dfLayananParent("nama"), lit(")")),
                coalesce(dfKomponenTarif("keterangan"), lit(""))
            )
        ).otherwise(
            concat_ws(
                ", ",
                dfLayanan("nama"),
                dfKomponenTarif("keterangan")
            )
        ).as("layanan"),
        dfDetail("jenis_layanan"),

        // nama tarif
        dfLayanan("nama").as("nama_tarif"),

        // unit
        coalesce(dfUnit("nama"), lit("Semua Instalasi")).as("unit"),

        // KELAS (Langsung granular satu baris per kelas karena efek join di atas)
        coalesce(dfMasterKelas("nama_kelas"), lit("")).as("kelas"),
        
        dfDetail("penjamin"),
        dfDetail("nama_komponen"),
        dfDetail("total")
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}