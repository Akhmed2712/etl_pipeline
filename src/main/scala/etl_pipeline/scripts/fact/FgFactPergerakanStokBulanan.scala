package etl_pipeline.scripts.agg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.slf4j.LoggerFactory
import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactPergerakanStokBulanan extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    // =====================================================
    // 1. TRIGGER RUNNER LOOP & GENERATE PER-SOURCE LOGS
    // =====================================================
    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    def getJdbcProps(config: CredentialsLoader.JdbcConfig): java.util.Properties = {
      val props = new java.util.Properties()
      config.props.foreach { case (k, v) => props.setProperty(k, v) }
      props
    }

    // Ekstrak data konfigurasi dari YAML env
    val fgFactPergerakanStok = env("fg_fact_pergerakan_stok").asInstanceOf[Map[String, Any]]

    // =====================================================
    // 2. READ SOURCE DATA
    // =====================================================
    val confStok = readConfigs("fg_fact_pergerakan_stok")
    val dfStok = spark.read
      .jdbc(
        confStok.url,
        s"${fgFactPergerakanStok.getOrElse("database", fgFactPergerakanStok.getOrElse("schema", ""))}.${fgFactPergerakanStok("table")}",
        getJdbcProps(confStok)
      )
      // Gunakan filter dari config yaml jika didefinisikan
      .filter(fgFactPergerakanStok("filter").toString)

    // =====================================================
    // 3. WINDOW FUNCTION SPECIFICATION
    // =====================================================
    // Meniru logic MAX(id) per barang per periode tahun dan bulan
    val windowSpec = Window
      .partitionBy(col("id_barang"), col("nobatch"), col("tahun"), col("nama_bulan"))
      .orderBy(col("id").desc)

    // =====================================================
    // 4. TRANSFORMATION & FILTERING
    // =====================================================
    val dfRanked = dfStok
      .withColumn("rn", row_number().over(windowSpec))

    val dfResult = dfRanked
      .filter(col("rn") === 1) // Ambil record dengan ID terbesar (terakhir)
      .withColumn(
        "aktif",
        when(col("ed") < col("partition_date"), lit(false)).otherwise(lit(true))
      )
      .withColumn(
          "partition_date",
          last_day(col("partition_date"))
      )
      .select(
        col("id"),
        col("id_barang"),
        col("nama_barang"),
        col("nobatch"),
        col("kategori"),
        col("unit"),
        col("sisa_stok_saat_ini").alias("jumlah_stok_terakhir"),
        col("total_nilai_stok_bergerak").alias("nilai_stok_terakhir"),
        col("aktif"),
        col("ed"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date") // Membawa partition date asli untuk mempermudah management data mart
      )

    log.info(s"Output aggregasi stok terakhir count: ${dfResult.count()}")

    dfResult
  }
}