package etl_pipeline.scripts.agg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgAggMutuFornas extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any], // 'env' di sini berisi map data dari yaml fg_agg_mutu_fornas
      jdbcConfig: CredentialsLoader.JdbcConfig // Ini write config bawaan runner utama
  ): DataFrame = {

    // =====================================================
    // 1. TRIGGER RUNNER LOOP & GENERATE PER-SOURCE LOGS
    // =====================================================
    // Baris ini akan otomatis memicu print "found 8 sources" beserta detail lognya
    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    // Helper internal untuk mengonversi properti Map ke java.util.Properties per tabel
    def getJdbcProps(config: CredentialsLoader.JdbcConfig): java.util.Properties = {
      val props = new java.util.Properties()
      config.props.foreach { case (k, v) => props.setProperty(k, v) }
      props
    }

    // Ekstrak data konfigurasi mentah dari YAML env
    val dcPenjualan = env("dc_penjualan").asInstanceOf[Map[String, Any]]
    val dcDetailPenjualan = env("dc_detail_penjualan").asInstanceOf[Map[String, Any]]
    val fgDimPengemasanBarang = env("fg_dim_pengemasan_barang").asInstanceOf[Map[String, Any]]
    val dcUsers = env("dc_users").asInstanceOf[Map[String, Any]]
    val dcGroupUsers = env("dc_group_users").asInstanceOf[Map[String, Any]]
    val dcLayananPendaftaran = env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcPendaftaran = env("dc_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcSpesialisasi = env("dc_spesialisasi").asInstanceOf[Map[String, Any]]

    // =====================================================
    // 2. READ (Menggunakan Config Spesifik Tiap Source)
    // =====================================================

    val confPenjualan = readConfigs("dc_penjualan")
    val dfPenjualan = spark.read
      .jdbc(
        confPenjualan.url,
        s"${dcPenjualan.getOrElse("database", dcPenjualan.getOrElse("schema", ""))}.${dcPenjualan("table")}",
        getJdbcProps(confPenjualan)
      )
      .filter(dcPenjualan("filter").toString)

    val confDetailPenjualan = readConfigs("dc_detail_penjualan")
    val dfDetailPenjualan = spark.read
      .jdbc(
        confDetailPenjualan.url,
        s"${dcDetailPenjualan.getOrElse("database", dcDetailPenjualan.getOrElse("schema", ""))}.${dcDetailPenjualan("table")}",
        getJdbcProps(confDetailPenjualan)
      )
      .filter(dcDetailPenjualan("filter").toString)

    val confBarang = readConfigs("fg_dim_pengemasan_barang")
    val dfBarang = spark.read
      .jdbc(
        confBarang.url,
        s"${fgDimPengemasanBarang.getOrElse("database", fgDimPengemasanBarang.getOrElse("schema", ""))}.${fgDimPengemasanBarang("table")}",
        getJdbcProps(confBarang)
      )
      .filter(col("kategori") === "Obat")

    val confUsers = readConfigs("dc_users")
    val dfUsers = spark.read
      .jdbc(
        confUsers.url,
        s"${dcUsers.getOrElse("database", dcUsers.getOrElse("schema", ""))}.${dcUsers("table")}",
        getJdbcProps(confUsers)
      )

    val confGroupUsers = readConfigs("dc_group_users")
    val dfGroupUsers = spark.read
      .jdbc(
        confGroupUsers.url,
        s"${dcGroupUsers.getOrElse("database", dcGroupUsers.getOrElse("schema", ""))}.${dcGroupUsers("table")}",
        getJdbcProps(confGroupUsers)
      )

    val confLayanan = readConfigs("dc_layanan_pendaftaran")
    val dfLayanan = spark.read
      .jdbc(
        confLayanan.url,
        s"${dcLayananPendaftaran.getOrElse("database", dcLayananPendaftaran.getOrElse("schema", ""))}.${dcLayananPendaftaran("table")}",
        getJdbcProps(confLayanan)
      )

    val confPendaftaran = readConfigs("dc_pendaftaran")
    val dfPendaftaran = spark.read
      .jdbc(
        confPendaftaran.url,
        s"${dcPendaftaran.getOrElse("database", dcPendaftaran.getOrElse("schema", ""))}.${dcPendaftaran("table")}",
        getJdbcProps(confPendaftaran)
      )
      .filter(dcPendaftaran("filter").toString)

    val confSpesialisasi = readConfigs("dc_spesialisasi")
    val dfSpesialisasi = spark.read
      .jdbc(
        confSpesialisasi.url,
        s"${dcSpesialisasi.getOrElse("database", dcSpesialisasi.getOrElse("schema", ""))}.${dcSpesialisasi("table")}",
        getJdbcProps(confSpesialisasi)
      )

    // =====================================================
    // JOIN
    // =====================================================

    val dfJoined = dfPenjualan.alias("dp")
      .join(
        dfDetailPenjualan.alias("ddp"),
        col("ddp.id_penjualan") === col("dp.id"),
        "inner"
      )
      .join(
        dfBarang.alias("fdpb"),
        col("fdpb.id") === col("ddp.id_kemasan"),
        "inner"
      )
      .join(
        dfUsers.alias("du"),
        col("du.id") === col("ddp.id_user_approval"),
        "left"
      )
      .join(
        dfGroupUsers.alias("dgu"),
        col("dgu.id") === col("du.id_group_users"),
        "left"
      )
      .join(
        dfLayanan.alias("dlp"),
        col("dlp.id") === col("dp.id_layanan_pendaftaran"),
        "left"
      )
      .join(
        dfPendaftaran.alias("dp2"),
        col("dp2.id") === col("dlp.id_pendaftaran"),
        "left"
      )
      .join(
        dfSpesialisasi.alias("ds"),
        col("ds.id") === col("dlp.id_unit_layanan"),
        "left"
      )
      .withColumn(
        "jenis_layanan",
        when(
          col("dlp.jenis") === "Poliklinik",
          concat(lit("Poli "), col("ds.nama"))
        )
          .when(col("dlp.jenis").isNotNull, col("dlp.jenis"))
          .otherwise(col("dp.jenis"))
      )

    // =====================================================
    // AGGREGATION
    // =====================================================

    val dfAgg = dfJoined
      .groupBy(
        col("dp.id"),
        col("dp.tanggal_penjualan"),
        col("dp.waktu"),
        col("jenis_layanan")
      )
      .agg(
        count(lit(1)).alias("total_item"),
        sum(when(lower(trim(col("fdpb.fornas"))) === "ya", 1).otherwise(0)).alias("total_fornas"),
        sum(when(lower(trim(col("fdpb.fornas"))) =!= "ya" && col("fdpb.fornas").isNotNull, 1).otherwise(0)).alias("total_non_fornas"),
        sum(when(col("fdpb.fornas").isNull, 1).otherwise(0)).alias("total_null_fornas"),
        max(
          when(
            lower(coalesce(col("dgu.nama"), lit(""))).contains("komite") ||
            lower(coalesce(col("dgu.nama"), lit(""))).contains("direktur"),
            1
          ).otherwise(0)
        ).alias("is_eksklusi"),
        max(col("dgu.nama")).alias("nama_group_user"),
        concat_ws(", ", sort_array(collect_set(when(lower(trim(col("fdpb.fornas"))) === "tidak", col("fdpb.nama_barang"))))).alias("nama_obat_diluar_fornas"),
        concat_ws(", ", sort_array(collect_set(when(col("fdpb.fornas").isNull, col("fdpb.nama_barang"))))).alias("nama_obat_fornas_null")
      )
      .withColumn("persen_fornas", round(col("total_fornas") * lit(100.0) / col("total_item"), 2))
      .withColumn(
        "status",
        when(col("is_eksklusi") === 1, "Eksklusi")
          .when(col("total_null_fornas") > 0, "Tidak Ada Keterangan FORNAS")
          .when(col("persen_fornas") >= 80, "Memenuhi")
          .otherwise("Tidak Memenuhi")
      )
      .withColumn(
        "keterangan",
        when(col("is_eksklusi") === 1, concat(lit("Eksklusi Disetujui "), col("nama_group_user")))
          .when(col("total_null_fornas") > 0, lit("Belum ada keterangan FORNAS pada master obat"))
          .otherwise(lit(null))
      )

    // =====================================================
    // GENERATE DIMENSI WAKTU
    // =====================================================
    val dfWithTime = dfAgg
      .withColumn("tahun", year(col("waktu")))
      .withColumn("triwulan", concat(lit("Q"), quarter(col("waktu"))))
      .withColumn(
        "nama_bulan",
        when(month(col("waktu")) === 1, "01-Januari")
          .when(month(col("waktu")) === 2, "02-Februari")
          .when(month(col("waktu")) === 3, "03-Maret")
          .when(month(col("waktu")) === 4, "04-April")
          .when(month(col("waktu")) === 5, "05-Mei")
          .when(month(col("waktu")) === 6, "06-Juni")
          .when(month(col("waktu")) === 7, "07-Juli")
          .when(month(col("waktu")) === 8, "08-Agustus")
          .when(month(col("waktu")) === 9, "09-September")
          .when(month(col("waktu")) === 10, "10-Oktober")
          .when(month(col("waktu")) === 11, "11-November")
          .otherwise("12-Desember")
      )

    // =====================================================
    // FINAL
    // =====================================================

    val dfResult = dfWithTime
      .withColumn("partition_date", coalesce(to_date(col("tanggal_penjualan")), to_date(col("waktu"))))
      .select(
        col("id"),
        col("tanggal_penjualan"),
        col("waktu"),
        col("jenis_layanan"),
        col("total_item"),
        col("total_fornas"),
        col("total_non_fornas"),
        col("persen_fornas"),
        col("status"),
        col("keterangan"),
        when(trim(col("nama_obat_diluar_fornas")) === "", lit(null)).otherwise(col("nama_obat_diluar_fornas")).alias("nama_obat_diluar_fornas"),
        when(trim(col("nama_obat_fornas_null")) === "", lit(null)).otherwise(col("nama_obat_fornas_null")).alias("nama_obat_fornas_null"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"output count: ${dfResult.count()}")

    dfResult
  }
}