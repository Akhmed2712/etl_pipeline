package etl_pipeline.scripts.staging

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgStagingKomponenTarifDetail extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    // -- Read source table ----------------------------------------------------
    val dcDetailKomponenTarif =
      env("dc_detail_komponen_tarif").asInstanceOf[Map[String, Any]]

    val dcSettingKomponenTarif =
      env("dc_setting_komponen_tarif").asInstanceOf[Map[String, Any]]

    val dcPenjamin =
      env("dc_penjamin").asInstanceOf[Map[String, Any]]

    val dcJenisPemeriksaan =
      env("dc_jenis_pemeriksaan").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) =>
      props.setProperty(k, v)
    }

    val dfDetailKomponenTarif = spark.read
      .jdbc(
        url = jdbcConfig.url,
        s"${dcDetailKomponenTarif("database")}.${dcDetailKomponenTarif("table")}",
        props
      )
      .select(
        "id_komponen_tarif",
        "id_setting_komponen_tarif",
        "id_penjamin",
        "id_jenis_pemeriksaan",
        "nominal"
      )

    val dfSettingKomponenTarif = spark.read
      .jdbc(
        url = jdbcConfig.url,
        s"${dcSettingKomponenTarif("database")}.${dcSettingKomponenTarif("table")}",
        props
      )
      .select(
        "id",
        "nama_komponen"
      )

    val dfPenjamin = spark.read
      .jdbc(
        url = jdbcConfig.url,
        s"${dcPenjamin("database")}.${dcPenjamin("table")}",
        props
      )
      .select(
        "id",
        "nama"
      )

    val dfJenisPemeriksaan = spark.read
      .jdbc(
        url = jdbcConfig.url,
        s"${dcJenisPemeriksaan("database")}.${dcJenisPemeriksaan("table")}",
        props
      )
      .select(
        "id",
        "nama"
      )

    log.info(
      s"Source row count dc_detail_komponen_tarif: ${dfDetailKomponenTarif.count()}"
    )

    log.info(
      s"Source row count dc_setting_komponen_tarif: ${dfSettingKomponenTarif.count()}"
    )

    log.info(
      s"Source row count dc_penjamin: ${dfPenjamin.count()}"
    )

    log.info(
      s"Source row count dc_jenis_pemeriksaan: ${dfJenisPemeriksaan.count()}"
    )

    // -- Transformation logic -------------------------------------------------
    val dfResult = dfDetailKomponenTarif
      .join(
        dfSettingKomponenTarif,
        dfSettingKomponenTarif("id") ===
          dfDetailKomponenTarif("id_setting_komponen_tarif"),
        "left"
      )
      .join(
        dfPenjamin,
        dfPenjamin("id") === dfDetailKomponenTarif("id_penjamin"),
        "left"
      )
      .join(
        dfJenisPemeriksaan,
        dfJenisPemeriksaan("id") === dfDetailKomponenTarif("id_jenis_pemeriksaan"),
        "left"
      )
      .groupBy(
        dfDetailKomponenTarif("id_komponen_tarif"),
        dfPenjamin("nama"),
        dfSettingKomponenTarif("nama_komponen"),
        dfJenisPemeriksaan("nama")
      )
      .agg(
        sum(dfDetailKomponenTarif("nominal")).as("total")
      )
      .select(
        dfDetailKomponenTarif("id_komponen_tarif"),
        dfJenisPemeriksaan("nama").as("jenis_layanan"),
        coalesce(
          dfPenjamin("nama"),
          lit("SEMUA PENJAMIN")
        ).as("penjamin"),
        dfSettingKomponenTarif("nama_komponen"),
        col("total")
      )

    log.info(s"Output row count: ${dfResult.count()}")

    dfResult
  }
}