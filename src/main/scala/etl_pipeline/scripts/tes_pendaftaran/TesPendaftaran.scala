package etl_pipeline.scripts.tes_pendaftaran

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

/**
 * Transform
 *
 * Equivalent of scripts/my_job/transform.py.
 *
 * Extends Transformer so Main can load it dynamically via reflection.
 * Registered in catalog.yml as:
 * script_location: etl_pipeline.scripts.myjob.MyOrdersJob
 *
 * By the time processData() is called, ALL {~~~...~~~} templates in env
 * have already been resolved to real date strings by Main.
 */
object TesPendaftaran extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcPendaftaran = env("dc_pendaftaran").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfPendaftaran = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPendaftaran("database")}.${dcPendaftaran("table")}", props)
      .filter(dcPendaftaran("filter").toString)

    log.info(s"Source row count: ${dfPendaftaran.count()}")

    // -- Transformation logic -------------------------------------------------
    val loadTs = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val dfResult = dfPendaftaran
      .select("id", "id_pasien", "waktu_daftar", "waktu_keluar")
      .withColumn("load_ts", lit(loadTs))

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}