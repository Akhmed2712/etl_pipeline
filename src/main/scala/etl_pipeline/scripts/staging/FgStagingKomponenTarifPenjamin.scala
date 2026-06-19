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

object FgStagingKomponenTarifPenjamin extends Transformer {

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
    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfDetailKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcDetailKomponenTarif("database")}.${dcDetailKomponenTarif("table")}", props)
      .select("id", "id_komponen_tarif", "id_setting_komponen_tarif", "id_jenis_pemeriksaan", "id_penjamin", "nominal", "aktif")

    log.info(s"Source row count dc_detail_komponen_tarif: ${dfDetailKomponenTarif.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfResult = dfDetailKomponenTarif
    .withColumn(
        "penjamin_struct",
        struct(
        col("id_komponen_tarif"),
        col("id_jenis_pemeriksaan"),
        col("id_penjamin")
        )
    )
    .groupBy(
        col("id_komponen_tarif"),
        col("id_jenis_pemeriksaan")
    )
    .agg(
        to_json(
        transform(
            sort_array(
            collect_list(
                struct(
                col("id_penjamin"),
                col("penjamin_struct").as("data")
                )
            )
            ),
            x => x.getField("data")
        )
        ).as("penjamin_json")
    )
    .select(
        col("id_komponen_tarif"),
        col("id_jenis_pemeriksaan"),
        col("penjamin_json")
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