package etl_pipeline.scripts.dimension

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgDimPengemasanBarang extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any], // Berisi data map dari YAML script ini
      jdbcConfig: CredentialsLoader.JdbcConfig // Ini write config bawaan runner utama
  ): DataFrame = {

    // =====================================================
    // 1. TRIGGER RUNNER LOOP & GENERATE PER-SOURCE LOGS
    // =====================================================
    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    // Helper internal untuk mengekstrak java.util.Properties per source config
    def getJdbcProps(config: CredentialsLoader.JdbcConfig): Properties = {
      val props = new Properties()
      config.props.foreach { case (k, v) => props.setProperty(k, v) }
      props
    }

    // Ekstrak data konfigurasi mentah dari YAML env
    val dcBarang = env("dc_barang").asInstanceOf[Map[String, Any]]
    val dcKemasanBarang = env("dc_kemasan_barang").asInstanceOf[Map[String, Any]]
    val dcSediaan = env("dc_sediaan").asInstanceOf[Map[String, Any]]
    val dcGolongan = env("dc_golongan").asInstanceOf[Map[String, Any]]
    val dcKategoriBarang = env("dc_kategori_barang").asInstanceOf[Map[String, Any]]
    val dcSatuan = env("dc_satuan").asInstanceOf[Map[String, Any]]
    val dcBarangPaket = env("dc_barang_paket").asInstanceOf[Map[String, Any]]

    // =====================================================
    // 2. READ (Menggunakan Config Spesifik Tiap Source)
    // =====================================================

    val confBarang = readConfigs("dc_barang")
    val dbBarang = dcBarang.getOrElse("database", dcBarang.getOrElse("schema", ""))
    val dfBarang = spark.read
      .jdbc(
        confBarang.url,
        s"$dbBarang.${dcBarang("table")}",
        getJdbcProps(confBarang)
      )
      .select(
        col("id"),
        col("id_sediaan"),
        col("id_golongan"),
        col("id_kategori"),
        col("nama_lengkap"),
        col("adm_r"),
        col("formularium"),
        col("fornas"),
        col("hna"),
        col("hpp"),
        col("harga_jual"),
        col("margin_non_resep"),
        col("margin_resep"),
        col("margin_resep_rajal"),
        col("plus_ppn")
      )

    val confKemasanBarang = readConfigs("dc_kemasan_barang")
    val dbKemasanBarang = dcKemasanBarang.getOrElse("database", dcKemasanBarang.getOrElse("schema", ""))
    val dfKemasanBarang = spark.read
      .jdbc(
        confKemasanBarang.url,
        s"$dbKemasanBarang.${dcKemasanBarang("table")}",
        getJdbcProps(confKemasanBarang)
      )
      .select(
        col("id"),
        col("id_barang"),
        col("id_satuan")
      )

    val confSediaan = readConfigs("dc_sediaan")
    val dbSediaan = dcSediaan.getOrElse("database", dcSediaan.getOrElse("schema", ""))
    val dfSediaan = spark.read
      .jdbc(
        confSediaan.url,
        s"$dbSediaan.${dcSediaan("table")}",
        getJdbcProps(confSediaan)
      )
      .select(
        col("id"),
        col("nama")
      )

    val confGolongan = readConfigs("dc_golongan")
    val dbGolongan = dcGolongan.getOrElse("database", dcGolongan.getOrElse("schema", ""))
    val dfGolongan = spark.read
      .jdbc(
        confGolongan.url,
        s"$dbGolongan.${dcGolongan("table")}",
        getJdbcProps(confGolongan)
      )
      .select(
        col("id"),
        col("nama")
      )

    val confKategoriBarang = readConfigs("dc_kategori_barang")
    val dbKategoriBarang = dcKategoriBarang.getOrElse("database", dcKategoriBarang.getOrElse("schema", ""))
    val dfKategoriBarang = spark.read
      .jdbc(
        confKategoriBarang.url,
        s"$dbKategoriBarang.${dcKategoriBarang("table")}",
        getJdbcProps(confKategoriBarang)
      )
      .select(
        col("id"),
        col("nama"),
        col("jenis").as("jenis_kategori")
      )

    val confSatuan = readConfigs("dc_satuan")
    val dbSatuan = dcSatuan.getOrElse("database", dcSatuan.getOrElse("schema", ""))
    val dfSatuan = spark.read
      .jdbc(
        confSatuan.url,
        s"$dbSatuan.${dcSatuan("table")}",
        getJdbcProps(confSatuan)
      )
      .select(
        col("id"),
        col("nama")
      )

    val confBarangPaket = readConfigs("dc_barang_paket")
    val dbBarangPaket = dcBarangPaket.getOrElse("database", dcBarangPaket.getOrElse("schema", ""))
    val dfBarangPaket = spark.read
      .jdbc(
        confBarangPaket.url,
        s"$dbBarangPaket.${dcBarangPaket("table")}",
        getJdbcProps(confBarangPaket)
      )
      .withColumnRenamed("id", "id_barang_paket")
      .withColumnRenamed("nama", "nama_barang")
      .withColumn(
        "id",
        concat(
          regexp_replace(current_date().cast("string"), "-", ""),
          col("id_barang_paket").cast("string")
        ).cast("long")
      )
      .withColumn("kategori", lit("Paket"))
      .select(
        col("id"),
        col("id_barang_paket"),
        col("nama_barang"),
        col("kategori")
      )

    // =====================================================
    // 3. TRANSFORMATION & LOGIC (JOIN & UNION)
    // =====================================================

    val dfJoin = dfBarang
      .withColumnRenamed("nama_lengkap", "nama_barang")
      .join(dfKemasanBarang, dfBarang("id") === dfKemasanBarang("id_barang"), "inner")
      .drop(dfBarang("id"))
      .join(
        broadcast(dfSediaan.withColumnRenamed("nama", "sediaan")),
        dfBarang("id_sediaan") === dfSediaan("id"),
        "left"
      )
      .drop(dfSediaan("id"))
      .join(
        broadcast(dfGolongan.withColumnRenamed("nama", "golongan")),
        dfBarang("id_golongan") === dfGolongan("id"),
        "left"
      )
      .drop(dfGolongan("id"))
      .join(
        broadcast(dfKategoriBarang.withColumnRenamed("nama", "kategori")),
        dfBarang("id_kategori") === dfKategoriBarang("id"),
        "inner"
      )
      .drop(dfKategoriBarang("id"))
      .join(
        broadcast(dfSatuan.withColumnRenamed("nama", "satuan")),
        dfKemasanBarang("id_satuan") === dfSatuan("id"),
        "inner"
      )
      .drop(dfSatuan("id"))

    val dfUnion = dfJoin.unionByName(dfBarangPaket, allowMissingColumns = true)

    val dfResult = dfUnion.select(
      col("id"),
      col("id_barang_paket"),
      col("id_barang"),
      col("nama_barang"),
      col("sediaan"),
      col("adm_r"),
      col("golongan"),
      col("formularium"),
      col("fornas"),
      col("kategori"),
      col("jenis_kategori"),
      col("satuan"),
      col("hna"),
      col("hpp"),
      col("harga_jual"),
      col("margin_non_resep"),
      col("margin_resep"),
      col("margin_resep_rajal"),
      col("plus_ppn")
    )

    // OPTIMASI: Batasi partisi ke MySQL agar executor tidak rebutan lock tabel
    val dfFinal = dfResult.coalesce(2)

    log.info(s"Output row count sebelum dikirim ke MySQL: ${dfFinal.count()}")

    dfFinal
  }
}