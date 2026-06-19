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

object FgStagingKomponenTarifKelas extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcKelasKomponenTarif = env("dc_kelas_komponen_tarif").asInstanceOf[Map[String, Any]]
    val dcKelas = env("dc_kelas").asInstanceOf[Map[String, Any]]
    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfKelasKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcKelasKomponenTarif("database")}.${dcKelasKomponenTarif("table")}", props)
      .select("id_komponen_tarif", "id_kelas")

    val dfKelas = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcKelas("database")}.${dcKelas("table")}", props)
      .select("id", "nama")

    log.info(s"Source row count dc_kelas_komponen_tarif: ${dfKelasKomponenTarif.count()}")
    log.info(s"Source row count dc_kelas: ${dfKelas.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfJoin = dfKelasKomponenTarif
      .join(
        dfKelas,
        dfKelas("id") === dfKelasKomponenTarif("id_kelas"),
        "inner"
      )
      .drop(dfKelasKomponenTarif("id_kelas"))
      .select(
        dfKelasKomponenTarif("id_komponen_tarif"),
        dfKelas("id").as("id_kelas"),
        dfKelas("nama").as("nama_kelas")
      )

    val dfResult = dfJoin
      .groupBy("id_komponen_tarif")
      .agg(
        concat_ws("<br>",
          sort_array(collect_list(col("nama_kelas")))
        ).as("group_kelas")
      )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}