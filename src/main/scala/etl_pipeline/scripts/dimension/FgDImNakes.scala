package etl_pipeline.scripts.dimension

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgDimNakes extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcNakes = env("dc_nakes").asInstanceOf[Map[String, Any]]
    val dcPegawai = env("dc_pegawai").asInstanceOf[Map[String, Any]]
    val dcProfesi = env("dc_profesi").asInstanceOf[Map[String, Any]]
    val dcSpesialiasi = env("dc_spesialisasi").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfNakes = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcNakes("database")}.${dcNakes("table")}", props)
      .select("id", "id_pegawai", "id_profesi", "id_spesialis")

    val dfPegawai = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPegawai("database")}.${dcPegawai("table")}", props)
      .select("id", "nama")

    val dfProfesi = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcProfesi("database")}.${dcProfesi("table")}", props)
      .select("id", "nama")
      .withColumnRenamed("nama", "profesi")

    val dfSpesialisasi = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcSpesialiasi("database")}.${dcSpesialiasi("table")}", props)
      .select("id", "nama")
      .withColumnRenamed("nama", "spesialisasi")

    log.info(s"Source row count dc_nakes: ${dfNakes.count()}")
    log.info(s"Source row count dc_pegawai: ${dfPegawai.count()}")
    log.info(s"Source row count dc_profesi: ${dfProfesi.count()}")
    log.info(s"Source row count dc_spesialisasi: ${dfSpesialisasi.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfJoin = dfNakes
      .join(
        dfPegawai,
        dfNakes("id_pegawai").equalTo(dfPegawai("id")),
        "left"
      )
      .drop(dfPegawai("id"))
      .join(
        dfProfesi,
        dfNakes("id_profesi").equalTo(dfProfesi("id")),
        "left"
      )
      .drop(dfProfesi("id"))
      .join(
        dfSpesialisasi,
        dfNakes("id_spesialis").equalTo(dfSpesialisasi("id")),
        "left"
      )
      .drop(dfSpesialisasi("id"))

    val dfResult = dfJoin.select(
        "id", "nama", "profesi", "spesialisasi"
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}