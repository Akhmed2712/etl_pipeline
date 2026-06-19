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
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    val dcPenjualan = env("dc_penjualan").asInstanceOf[Map[String, Any]]
    val dcDetailPenjualan = env("dc_detail_penjualan").asInstanceOf[Map[String, Any]]
    val fgDimPengemasanBarang = env("fg_dim_pengemasan_barang").asInstanceOf[Map[String, Any]]
    val dcUsers = env("dc_users").asInstanceOf[Map[String, Any]]
    val dcGroupUsers = env("dc_group_users").asInstanceOf[Map[String, Any]]
    val dcLayananPendaftaran = env("dc_layanan_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcPendaftaran = env("dc_pendaftaran").asInstanceOf[Map[String, Any]]
    val dcSpesialisasi = env("dc_spesialisasi").asInstanceOf[Map[String, Any]]
    val fgDimWaktu = env("fg_dim_waktu").asInstanceOf[Map[String, Any]]

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) =>
      props.setProperty(k, v)
    }

    // =====================================================
    // READ
    // =====================================================

    val dfPenjualan = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcPenjualan("database")}.${dcPenjualan("table")}",
        props
      )
      .filter(dcPenjualan("filter").toString)

    val dfDetailPenjualan = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcDetailPenjualan("database")}.${dcDetailPenjualan("table")}",
        props
      )
      .filter(dcDetailPenjualan("filter").toString)

    val dfBarang = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${fgDimPengemasanBarang("database")}.${fgDimPengemasanBarang("table")}",
        props
      )
      .filter(col("kategori") === "Obat")

    val dfUsers = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcUsers("database")}.${dcUsers("table")}",
        props
      )

    val dfGroupUsers = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcGroupUsers("database")}.${dcGroupUsers("table")}",
        props
      )

    val dfLayanan = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcLayananPendaftaran("database")}.${dcLayananPendaftaran("table")}",
        props
      )

    val dfPendaftaran = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcPendaftaran("database")}.${dcPendaftaran("table")}",
        props
      )
      .filter(dcPendaftaran("filter").toString)

    val dfSpesialisasi = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${dcSpesialisasi("database")}.${dcSpesialisasi("table")}",
        props
      )

    val dfWaktu = spark.read
      .jdbc(
        jdbcConfig.url,
        s"${fgDimWaktu("database")}.${fgDimWaktu("table")}",
        props
      )
      .dropDuplicates("tanggal")

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

        count(lit(1))
          .alias("total_item"),

        sum(
          when(
            lower(trim(col("fdpb.fornas"))) === "ya",
            1
          ).otherwise(0)
        ).alias("total_fornas"),

        sum(
          when(
            lower(trim(col("fdpb.fornas"))) =!= "ya" &&
            col("fdpb.fornas").isNotNull,
            1
          ).otherwise(0)
        ).alias("total_non_fornas"),

        sum(
          when(
            col("fdpb.fornas").isNull,
            1
          ).otherwise(0)
        ).alias("total_null_fornas"),

        max(
          when(
            lower(coalesce(col("dgu.nama"), lit(""))).contains("komite") ||
            lower(coalesce(col("dgu.nama"), lit(""))).contains("direktur"),
            1
          ).otherwise(0)
        ).alias("is_eksklusi"),

        max(col("dgu.nama"))
          .alias("nama_group_user"),

        // GROUP_CONCAT formularium = 'tidak'
        concat_ws(
          ", ",
          sort_array(
            collect_set(
              when(
                lower(trim(col("fdpb.fornas"))) === "tidak",
                col("fdpb.nama_barang")
              )
            )
          )
        ).alias("nama_obat_diluar_fornas"),

        // GROUP_CONCAT formularium IS NULL
        concat_ws(
          ", ",
          sort_array(
            collect_set(
              when(
                col("fdpb.fornas").isNull,
                col("fdpb.nama_barang")
              )
            )
          )
        ).alias("nama_obat_fornas_null")
      )
      .withColumn(
        "persen_fornas",
        round(
          col("total_fornas") * lit(100.0) /
            col("total_item"),
          2
        )
      )
      .withColumn(
        "status",
        when(
          col("is_eksklusi") === 1,
          "Eksklusi"
        )
          .when(
            col("total_null_fornas") > 0,
            "Tidak Ada Keterangan FORNAS"
          )
          .when(
            col("persen_fornas") >= 80,
            "Memenuhi"
          )
          .otherwise("Tidak Memenuhi")
      )
      .withColumn(
        "keterangan",
        when(
          col("is_eksklusi") === 1,
          concat(
            lit("Eksklusi Disetujui "),
            col("nama_group_user")
          )
        )
          .when(
            col("total_null_fornas") > 0,
            lit("Belum ada keterangan FORNAS pada master obat")
          )
          .otherwise(lit(null))
      )

    // =====================================================
    // JOIN DIM WAKTU
    // =====================================================

    val dfWithTime = dfAgg
      .join(
        dfWaktu,
        to_date(col("waktu")) === col("tanggal"),
        "left"
      )
      .drop(dfWaktu("id"))
      .withColumn(
        "triwulan",
        concat(lit("Q"), col("triwulan"))
      )

    // =====================================================
    // FINAL
    // =====================================================

    val dfResult = dfWithTime
      .withColumn(
        "partition_date",
        to_date(col("tanggal_penjualan"))
      )
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

        when(
          trim(col("nama_obat_diluar_fornas")) === "",
          lit(null)
        ).otherwise(col("nama_obat_diluar_fornas"))
          .alias("nama_obat_diluar_fornas"),

        when(
          trim(col("nama_obat_fornas_null")) === "",
          lit(null)
        ).otherwise(col("nama_obat_fornas_null"))
          .alias("nama_obat_fornas_null"),

        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"output count: ${dfResult.count()}")

    dfResult
  }
}