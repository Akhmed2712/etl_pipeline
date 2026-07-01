package etl_pipeline.scripts.fact

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactMutuIdentifikasiPasien extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any], // Berisi data map dari YAML script ini
      jdbcConfig: CredentialsLoader.JdbcConfig // Ini write config bawaan runner utama
  ): DataFrame = {

    // =====================================================
    // 1. TRIGGER RUNNER LOOP & GENERATE SOURCE LOGS
    // =====================================================
    // Baris ini memicu log "found X sources" dan mencetak detail JDBC source ke terminal
    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    val documentHistoryList = env("document_history_list").asInstanceOf[Map[String, Any]]

    // Ambil properti JDBC spesifik untuk source document_history_list
    val confDocHistory = readConfigs("document_history_list")
    val sourceProps = new Properties()
    confDocHistory.props.foreach { case (k, v) =>
      sourceProps.setProperty(k, v)
    }

    // Resolusi nama database/schema secara dinamis
    val dbName = documentHistoryList.getOrElse("database", documentHistoryList.getOrElse("schema", ""))
    val tableName = s"$dbName.${documentHistoryList("table")}"

    // =====================================================
    // READ (Menggunakan Config Spesifik Source, Bukan Global)
    // =====================================================
    val dfSource =
      spark.read
        .jdbc(
          confDocHistory.url, // Menggunakan URL hasil loader per source
          tableName,
          sourceProps // Menggunakan tuning props milik source
        )
        .select(
          col("document_history_id"),
          col("document_code"),
          col("emr_data"),
          col("created_time")
        )
        .filter(documentHistoryList("filter").toString)

    // 2. Definisi Schema untuk Array data_list di dalam JSON
    val dataListSchema =
      ArrayType(
        StructType(
          Seq(
            StructField("profesi", StringType, true),
            StructField("paraf", StringType, true),
            StructField("identifikasi_nama", StringType, true),
            StructField("identifikasi_tanggal_lahir", StringType, true)
          )
        )
      )

    // 3. Ekstraksi Field dari JSON & Explode Array
    val dfExploded =
      dfSource
        .withColumn(
          "no_rm",
          get_json_object(col("emr_data"), "$.pasien.no_rm")
        )
        .withColumn(
          "pasien",
          get_json_object(col("emr_data"), "$.pasien.nama")
        )
        .withColumn(
          "data_list",
          from_json(
            get_json_object(col("emr_data"), "$.data_list"),
            dataListSchema
          )
        )
        .withColumn(
          "item",
          explode(col("data_list"))
        )

    // 4. Transformasi Waktu
    val baseDate = to_date(col("created_time"))

    // 5. Transformasi Logika dan Seleksi Kolom Akhir
    val dfFinal =
      dfExploded
        .withColumn(
          "identifikasi_nama",
          when(lower(trim(col("item.identifikasi_nama"))) === "true", lit("Ya"))
            .otherwise(lit("Tidak"))
        )
        .withColumn(
          "identifikasi_tanggal_lahir",
          when(lower(trim(col("item.identifikasi_tanggal_lahir"))) === "true", lit("Ya"))
            .otherwise(lit("Tidak"))
        )
        .withColumn(
          "status",
          when(
            lower(trim(col("item.identifikasi_nama"))) === "true" &&
              lower(trim(col("item.identifikasi_tanggal_lahir"))) === "true",
            "Memenuhi"
          ).otherwise("Tidak Memenuhi")
        )
        .withColumn("tahun", year(baseDate))
        .withColumn("triwulan", concat(lit("Q"), quarter(baseDate)))
        .withColumn(
          "nama_bulan",
          when(month(baseDate) === 1, "01-Januari")
            .when(month(baseDate) === 2, "02-Februari")
            .when(month(baseDate) === 3, "03-Maret")
            .when(month(baseDate) === 4, "04-April")
            .when(month(baseDate) === 5, "05-Mei")
            .when(month(baseDate) === 6, "06-Juni")
            .when(month(baseDate) === 7, "07-Juli")
            .when(month(baseDate) === 8, "08-Agustus")
            .when(month(baseDate) === 9, "09-September")
            .when(month(baseDate) === 10, "10-Oktober")
            .when(month(baseDate) === 11, "11-November")
            .otherwise("12-Desember")
        )
        .withColumn("partition_date", baseDate)
        .select(
          col("document_history_id").cast("bigint").alias("id"),
          col("no_rm"),
          col("pasien"),
          col("item.profesi").alias("verifikasi"),
          col("item.paraf").alias("nama_verifikator"),
          col("identifikasi_nama"),
          col("identifikasi_tanggal_lahir"),
          col("status"),
          col("tahun"),
          col("triwulan"),
          col("nama_bulan"),
          col("partition_date")
        )
        .filter(
          length(trim(col("nama_verifikator"))) > 0
        )

    log.info(s"Output row count: ${dfFinal.count()}")

    dfFinal
  }
}