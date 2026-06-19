package etl_pipeline.scripts.dimension

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgDimWaktu extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Menghasilkan data dimensi waktu (Date Dimension) secara otomatis.
   * Tidak memerlukan pembacaan tabel sumber MySQL.
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {
    import spark.implicits._

    // -- Konfigurasi Rentang Tahun dari catalog.yml ---------------------------
    // Default: 2020 sampai 2030 jika tidak ditentukan di env
    val startYear = env.getOrElse("start_year", 2020).toString.toInt
    val endYear   = env.getOrElse("end_year", 2030).toString.toInt

    log.info(s"Generating dim_waktu from year $startYear to $endYear")

    val startDate = s"$startYear-01-01"
    val endDate   = s"$endYear-12-31"

    // -- 1. Generate Sequence Angka untuk Rentang Hari ------------------------
    val daysCount = java.time.temporal.ChronoUnit.DAYS.between(
      java.time.LocalDate.parse(startDate),
      java.time.LocalDate.parse(endDate).plusDays(1)
    )

    val dfBase = spark.range(0, daysCount)
      .withColumn("tanggal", expr(s"date_add('$startDate', cast(id as int))"))

    // -- 2. Transformation Logic: Ekstrak Atribut Waktu ------------------------
    val dfResult = dfBase.select(
      // id_waktu format: 20260504
      date_format($"tanggal", "yyyyMMdd").cast("int").as("id"),
      $"tanggal",
      dayofmonth($"tanggal").as("hari"),
      when(date_format($"tanggal", "EEEE") === "Monday", "Senin")
        .when(date_format($"tanggal", "EEEE") === "Tuesday", "Selasa")
        .when(date_format($"tanggal", "EEEE") === "Wednesday", "Rabu")
        .when(date_format($"tanggal", "EEEE") === "Thursday", "Kamis")
        .when(date_format($"tanggal", "EEEE") === "Friday", "Jumat")
        .when(date_format($"tanggal", "EEEE") === "Saturday", "Sabtu")
        .otherwise("Minggu")
        .as("nama_hari"),
      weekofyear($"tanggal").as("minggu_ke"),
      month($"tanggal").as("bulan"),
      when(month($"tanggal") === 1, "01-Januari")
        .when(month($"tanggal") === 2, "02-Februari")
        .when(month($"tanggal") === 3, "03-Maret")
        .when(month($"tanggal") === 4, "04-April")
        .when(month($"tanggal") === 5, "05-Mei")
        .when(month($"tanggal") === 6, "06-Juni")
        .when(month($"tanggal") === 7, "07-Juli")
        .when(month($"tanggal") === 8, "08-Agustus")
        .when(month($"tanggal") === 9, "09-September")
        .when(month($"tanggal") === 10, "10-Oktober")
        .when(month($"tanggal") === 11, "11-November")
        .otherwise("12-Desember")
        .as("nama_bulan"),
      quarter($"tanggal").as("triwulan"),
      year($"tanggal").as("tahun"),
      // Flag untuk identifikasi hari libur akhir pekan
      when(dayofweek($"tanggal").isin(1, 7), true)
        .otherwise(false).as("is_weekend")
    )

    log.info(s"Generated ${dfResult.count()} rows for dim_waktu")

    dfResult
  }
}