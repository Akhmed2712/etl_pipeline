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

object FgStagingKomponenTarifSettingNominal extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcDetailKomponenTarif = env("dc_detail_komponen_tarif").asInstanceOf[Map[String, Any]]
    val dcSettingKomponenTarif = env("dc_setting_komponen_tarif").asInstanceOf[Map[String, Any]]
    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfDetailKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcDetailKomponenTarif("database")}.${dcDetailKomponenTarif("table")}", props)
      .select("id", "id_komponen_tarif", "id_setting_komponen_tarif", "id_jenis_pemeriksaan", "id_penjamin", "nominal", "aktif")

    val dfSettingKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcSettingKomponenTarif("database")}.${dcSettingKomponenTarif("table")}", props)
      .select("id", "nama_komponen", "kategori", "aktif", "id_jenis_pemeriksaan")
      .filter(trim(lower(col("aktif"))) === "ya")

    log.info(s"Source row count dc_detail_komponen_tarif: ${dfDetailKomponenTarif.count()}")
    log.info(s"Source row count dc_setting_komponen_tarif: ${dfSettingKomponenTarif.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfJoin = dfSettingKomponenTarif
      .join(
        dfDetailKomponenTarif,
        dfDetailKomponenTarif("id_setting_komponen_tarif") === dfSettingKomponenTarif("id"),
        "left"
      )
      .groupBy(
        dfDetailKomponenTarif("id_komponen_tarif"),
        dfSettingKomponenTarif("id_jenis_pemeriksaan"),
        dfSettingKomponenTarif("id"),
        dfSettingKomponenTarif("nama_komponen"),
        dfSettingKomponenTarif("kategori"),
        dfSettingKomponenTarif("aktif"),
        dfDetailKomponenTarif("id_penjamin")
      )
      .agg(
        coalesce(sum(dfDetailKomponenTarif("nominal")), lit(0)).as("nominal")
      )
      .select(
        col("id_komponen_tarif"),
        col("id_jenis_pemeriksaan"),
        col("id"),
        col("nama_komponen"),
        col("kategori"),
        col("aktif"),
        col("nominal"),
        coalesce(col("id_penjamin"), lit("")).as("id_penjamin")
      )

    val dfResult = dfJoin
      .withColumn(
        "detail_struct",
        struct(
          "id",
          "nama_komponen",
          "kategori",
          "aktif",
          "nominal",
          "id_penjamin"
        )
      )
      .groupBy(
        "id_komponen_tarif",
        "id_jenis_pemeriksaan"
      )
      .agg(
        to_json(
          sort_array(
            collect_list("detail_struct")
          )
        ).as("detail_komponen_json")
      )
    .withColumn(
        "id",
        concat_ws(
        "_",
        col("id_komponen_tarif"),
        col("id_jenis_pemeriksaan")
        )
    )
    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}