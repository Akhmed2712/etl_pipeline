package etl_pipeline.scripts.staging

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

object FgStagingKomponenTarifTotalPenjamin extends Transformer {

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
    val dcPenjamin = env("dc_penjamin").asInstanceOf[Map[String, Any]]
    val dcPenjaminKomponenTarif = env("dc_penjamin_komponen_tarif").asInstanceOf[Map[String, Any]]
    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfDetailKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcDetailKomponenTarif("database")}.${dcDetailKomponenTarif("table")}", props)
      .select("id", "id_komponen_tarif", "id_setting_komponen_tarif", "id_jenis_pemeriksaan", "id_penjamin", "nominal", "aktif")

    val dfSettingKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcSettingKomponenTarif("database")}.${dcSettingKomponenTarif("table")}", props)
      .select("id", "nama_komponen", "kategori", "aktif", "id_jenis_pemeriksaan")
      .filter(trim(lower(col("aktif"))) === "ya")

    val dfPenjamin = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPenjamin("database")}.${dcPenjamin("table")}", props)
      .select("id", "nama")

    val dfPenjaminKomponenTarif = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcPenjaminKomponenTarif("database")}.${dcPenjaminKomponenTarif("table")}", props)
      .select("id_komponen_tarif", "id_penjamin", "unit_cost")

    log.info(s"Source row count dc_detail_komponen_tarif: ${dfDetailKomponenTarif.count()}")
    log.info(s"Source row count dc_setting_komponen_tarif: ${dfSettingKomponenTarif.count()}")
    log.info(s"Source row count dc_penjamin: ${dfPenjamin.count()}")
    log.info(s"Source row count dc_penjamin: ${dfPenjaminKomponenTarif.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfAgg = dfDetailKomponenTarif
      .join(
        dfSettingKomponenTarif,
        dfSettingKomponenTarif("id") === dfDetailKomponenTarif("id_setting_komponen_tarif"),
        "inner"
      )
      .join(
        dfPenjamin,
        dfPenjamin("id") === dfDetailKomponenTarif("id_penjamin"),
        "left"
      )
      .join(
        dfPenjaminKomponenTarif,
        (dfPenjaminKomponenTarif("id_komponen_tarif") === dfDetailKomponenTarif("id_komponen_tarif")) &&
        (dfPenjaminKomponenTarif("id_penjamin") <=> dfDetailKomponenTarif("id_penjamin")),
        "left"
      )
      .groupBy(
        dfDetailKomponenTarif("id_komponen_tarif"),
        dfDetailKomponenTarif("id_penjamin"),
        dfPenjamin("nama"),
        dfPenjaminKomponenTarif("unit_cost"),
        dfDetailKomponenTarif("aktif"),
        dfSettingKomponenTarif("id_jenis_pemeriksaan")
      )
      .agg(
        coalesce(sum(dfDetailKomponenTarif("nominal")), lit(0)).as("total")
      )
      .select(
        col("id_komponen_tarif"),
        col("id_penjamin"),
        coalesce(col("nama"), lit("SEMUA PENJAMIN")).as("nama_penjamin"),
        coalesce(col("unit_cost"), lit(0)).as("unit_cost"),
        col("id_jenis_pemeriksaan"),
        when(
            col("id_penjamin").isNull,
            lit(null).cast("string")
        ).otherwise(col("aktif")).as("aktif"),
        col("total")
      )

    val dfDetailPerSetting = dfDetailKomponenTarif
      .join(
        dfSettingKomponenTarif,
        dfSettingKomponenTarif("id") === dfDetailKomponenTarif("id_setting_komponen_tarif"),
        "inner"
      )
      .groupBy(
        dfDetailKomponenTarif("id_komponen_tarif"),
        dfDetailKomponenTarif("id_penjamin"),
        dfSettingKomponenTarif("id"),
        dfSettingKomponenTarif("nama_komponen"),
        dfSettingKomponenTarif("kategori"),
        dfSettingKomponenTarif("aktif"),
        dfSettingKomponenTarif("id_jenis_pemeriksaan")
      )
      .agg(
        coalesce(sum(dfDetailKomponenTarif("nominal")), lit(0)).as("nominal")
      )
      .select(
        col("id_komponen_tarif"),
        col("id_penjamin"),
        col("id").as("id_setting"),
        col("nama_komponen"),
        col("kategori"),
        col("aktif"),
        col("id_jenis_pemeriksaan"),
        col("nominal")
      )

    val dfDetailJson = dfDetailPerSetting
      .withColumn(
        "detail_struct",
        struct(
        col("id_setting").as("id"),
        col("nama_komponen"),
        col("kategori"),
        col("aktif"),
        col("nominal")
        )
      )
      .groupBy(
        col("id_komponen_tarif"),
        col("id_penjamin")
      )
      .agg(
        to_json(
        sort_array(
            collect_list(col("detail_struct"))
        )
        ).as("detail_json")
      )

    val dfFinal = dfAgg
      .join(
        dfDetailJson,
        (dfDetailJson("id_komponen_tarif") === dfAgg("id_komponen_tarif")) &&
        (dfDetailJson("id_penjamin") <=> dfAgg("id_penjamin")),
        "left"
      )
      .select(
        dfAgg("id_komponen_tarif"),
        dfAgg("id_penjamin"),
        dfAgg("nama_penjamin"),
        dfAgg("total"),
        dfAgg("unit_cost"),
        dfAgg("aktif"),
        coalesce(
        dfDetailJson("detail_json"),
        lit("[]")
        ).as("detail_json")
      )

    val dfResult = dfFinal
    .withColumn(
        "detail_struct",
        struct(
        col("id_penjamin"),
        col("nama_penjamin").as("penjamin"),
        col("total"),
        col("unit_cost"),
        col("aktif"),
        from_json(
            col("detail_json"),
            ArrayType(
            StructType(Seq(
                StructField("id", StringType),
                StructField("nama_komponen", StringType),
                StructField("kategori", StringType),
                StructField("aktif", StringType),
                StructField("nominal", DoubleType)
            ))
            )
        ).as("detail")
        )
    )
    .groupBy(
        col("id_komponen_tarif")
    )
    .agg(
        to_json(
        transform(
            sort_array(
            collect_list(
                struct(
                col("id_penjamin"),
                col("detail_struct").as("data")
                )
            )
            ),
            x => x.getField("data")
        )
        ).as("detail_total_json")
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}