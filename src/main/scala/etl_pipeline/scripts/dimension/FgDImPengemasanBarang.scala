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

object FgDimPengemasanBarang extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Read source table — filter already resolved -------------------------
    val dcBarang = env("dc_barang").asInstanceOf[Map[String, Any]]
    val dcKemasanBarang = env("dc_kemasan_barang").asInstanceOf[Map[String, Any]]
    val dcSediaan = env("dc_sediaan").asInstanceOf[Map[String, Any]]
    val dcGolongan = env("dc_golongan").asInstanceOf[Map[String, Any]]
    val dcKategoriBarang = env("dc_kategori_barang").asInstanceOf[Map[String, Any]]
    val dcSatuan = env("dc_satuan").asInstanceOf[Map[String, Any]]
    val dcBarangPaket = env("dc_barang_paket").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfBarang = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcBarang("database")}.${dcBarang("table")}", props)
      .select("id", "id_sediaan", "id_golongan", "id_kategori", "nama_lengkap", "adm_r", "formularium", "fornas", "hna", "hpp", "harga_jual", "margin_non_resep", "margin_resep", "margin_resep_rajal", "plus_ppn")

    val dfKemasanBarang = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcKemasanBarang("database")}.${dcKemasanBarang("table")}", props)
      .select("id", "id_barang", "id_satuan")

    val dfSediaan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcSediaan("database")}.${dcSediaan("table")}", props)
      .select("id", "nama")

    val dfGolongan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcGolongan("database")}.${dcGolongan("table")}", props)
      .select("id", "nama")

    val dfKategoriBarang = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcKategoriBarang("database")}.${dcKategoriBarang("table")}", props)
      .select(col("id"), col("nama"), col("jenis").as("jenis_kategori"))

    val dfSatuan = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcSatuan("database")}.${dcSatuan("table")}", props)
      .select("id", "nama")

    val dfBarangPaket = spark.read
      .jdbc(url = jdbcConfig.url, s"${dcBarangPaket("database")}.${dcBarangPaket("table")}", props)
      .withColumnRenamed("id", "id_barang_paket")
      .withColumnRenamed("nama", "nama_barang")
      .withColumn("id",
        concat(
          regexp_replace(current_date().cast("string"), "-", ""),
          col("id_barang_paket").cast("string")
        ).cast("long")
      )
      .withColumn("kategori", lit("Paket"))
      .select("id", "id_barang_paket", "nama_barang", "kategori")

    log.info(s"Source row count dc_barang: ${dfBarang.count()}")
    log.info(s"Source row count dc_kemasan_barang: ${dfKemasanBarang.count()}")
    log.info(s"Source row count dc_sediaan: ${dfSediaan.count()}")
    log.info(s"Source row count dc_golongan: ${dfGolongan.count()}")
    log.info(s"Source row count dc_kategori_barang: ${dfKategoriBarang.count()}")
    log.info(s"Source row count dc_satuan: ${dfSatuan.count()}")
    log.info(s"Source row count dc_barang_paket: ${dfBarangPaket.count()}")

    // -- Transformation logic -------------------------------------------------
    val dfJoin = dfBarang
      .withColumnRenamed("nama_lengkap", "nama_barang")
      .join(
        dfKemasanBarang,
        dfBarang("id").equalTo(dfKemasanBarang("id_barang")),
        "inner"
      )
      .drop(dfBarang("id"))
      .join(
        dfSediaan.withColumnRenamed("nama", "sediaan"),
        dfBarang("id_sediaan").equalTo(dfSediaan("id")),
        "left"
      )
      .drop(dfSediaan("id"))
      .join(
        dfGolongan.withColumnRenamed("nama", "golongan"),
        dfBarang("id_golongan").equalTo(dfGolongan("id")),
        "left"
      )
      .drop(dfGolongan("id"))
      .join(
        dfKategoriBarang.withColumnRenamed("nama", "kategori"),
        dfBarang("id_kategori").equalTo(dfKategoriBarang("id")),
        "inner"
      )
      .drop(dfKategoriBarang("id"))
      .join(
        dfSatuan.withColumnRenamed("nama", "satuan"),
        dfKemasanBarang("id_satuan").equalTo(dfSatuan("id")),
        "inner"
      )
      .drop(dfSatuan("id"))
      
    val dfUnion = dfJoin.unionByName(dfBarangPaket, allowMissingColumns = true)

    val dfResult = dfUnion.select(
        "id", "id_barang_paket", "id_barang", "nama_barang", "sediaan", "adm_r", "golongan", "formularium", "fornas", "kategori", "jenis_kategori", "satuan", "hna", "hpp", "harga_jual", "margin_non_resep", "margin_resep", "margin_resep_rajal", "plus_ppn"
    )

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}