package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactMutuScp extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    // =====================================================
    // 1. TRIGGER RUNNER LOOP & GENERATE SOURCE LOGS
    // =====================================================
    // Mengisolasi kredensial read source dan memicu log debug di terminal
    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    val documentHistoryList =
      env("document_history_list").asInstanceOf[Map[String, Any]]

    // Ambil konfigurasi spesifik milik source document_history_list
    val confDocHistory = readConfigs("document_history_list")
    val sourceProps = new Properties()
    confDocHistory.props.foreach {
      case (k, v) => sourceProps.setProperty(k, v)
    }

    // Resolusi nama schema/database secara dinamis
    val dbName = documentHistoryList.getOrElse("database", documentHistoryList.getOrElse("schema", ""))
    val tableName = s"$dbName.${documentHistoryList("table")}"

    // =====================================================
    // 2. READ (Menggunakan Config Spesifik Source)
    // =====================================================
    val dfDocumentHistory = spark.read
      .jdbc(
        confDocHistory.url, // Menggunakan URL terisolasi dari YAML
        tableName,
        sourceProps // Menggunakan properti optimasi/tuning dari YAML
      )
      .filter(documentHistoryList("filter").toString)
      .select(
        col("document_history_id"),
        col("document_code"), // Ditambahkan agar filter downstream tidak melempar AnalysisException
        col("title"),
        col("emr_data")
      )

    // =====================================================
    // 3. FILTER LAPORAN OPERASI
    // =====================================================
    val dfLaporanOperasi = dfDocumentHistory
      .filter(
        col("document_code") === "RS52_RM4.10" &&
          coalesce(
            get_json_object(
              col("emr_data"),
              "$.laporan.kualifikasi_2.emergency"
            ).cast("boolean"),
            lit(false)
          )
      )
      .select(
        col("document_history_id"),
        get_json_object(
          col("emr_data"),
          "$.waktu.tgl_bedah"
        ).as("tanggal_operasi"),
        get_json_object(
          col("emr_data"),
          "$.waktu.mulai_operasi"
        ).as("jam_operasi"),
        get_json_object(
          col("emr_data"),
          "$.pasien.nama_pasien"
        ).as("nama_pasien"),
        get_json_object(
          col("emr_data"),
          "$.staf.dokter_operator"
        ).as("dokter_bedah")
      )

    // =====================================================
    // 4. FILTER PRE OPERASI
    // =====================================================
    val dfPreOperasi = dfDocumentHistory
      .filter(
        col("document_code") === "RM55_K"
      )
      .select(
        col("document_history_id"),
        get_json_object(
          col("emr_data"),
          "$.asesmen.tanggal"
        ).as("tanggal_asesmen"),
        get_json_object(
          col("emr_data"),
          "$.asesmen.jam"
        ).as("jam_asesmen"),
        get_json_object(
          col("emr_data"),
          "$.pasien.no_rm"
        ).as("no_rm"),
        get_json_object(
          col("emr_data"),
          "$.asesmen.diagnosa_pra_operasi[0].icd_x"
        ).as("icd_x"),
        get_json_object(
          col("emr_data"),
          "$.asesmen.diagnosa_pra_operasi[0].nama_sebab_sakit"
        ).as("diagnosa")
      )

    // =====================================================
    // 5. JOIN & TRANSFORMASI LOGIKA
    // =====================================================
    val dfFinal = dfLaporanOperasi
      .join(
        dfPreOperasi,
        Seq("document_history_id"),
        "inner"
      )
      .withColumn(
        "ts_asesmen",
        to_timestamp(
          concat_ws(
            " ",
            col("tanggal_operasi"),
            col("jam_asesmen")
          ),
          "yyyy-MM-dd HH:mm"
        )
      )
      .withColumn(
        "ts_operasi",
        to_timestamp(
          concat_ws(
            " ",
            col("tanggal_operasi"),
            col("jam_operasi")
          ),
          "yyyy-MM-dd HH:mm"
        )
      )
      .withColumn(
        "selisih_menit",
        (
          unix_timestamp(col("ts_operasi")) -
            unix_timestamp(col("ts_asesmen"))
        ) / 60
      )
      .withColumn(
        "status",
        when(col("selisih_menit") <= 30, "Memenuhi")
          .otherwise("Tidak Memenuhi")
      )
      .withColumn(
        "partition_date",
        to_date(col("tanggal_operasi"))
      )
      .withColumn(
        "tahun",
        year(col("partition_date"))
      )
      .withColumn(
        "bulan",
        month(col("partition_date"))
      )
      .withColumn(
        "triwulan",
        concat(lit("Q"), quarter(col("partition_date")))
      )
      .withColumn(
        "nama_bulan",
        when(col("bulan") === 1, "01-Januari")
          .when(col("bulan") === 2, "02-Februari")
          .when(col("bulan") === 3, "03-Maret")
          .when(col("bulan") === 4, "04-April")
          .when(col("bulan") === 5, "05-Mei")
          .when(col("bulan") === 6, "06-Juni")
          .when(col("bulan") === 7, "07-Juli")
          .when(col("bulan") === 8, "08-Agustus")
          .when(col("bulan") === 9, "09-September")
          .when(col("bulan") === 10, "10-Oktober")
          .when(col("bulan") === 11, "11-November")
          .otherwise("12-Desember")
      )
      .withColumn(
        "id",
        col("document_history_id").cast("bigint")
      )
      .select(
        col("id"),
        col("no_rm"),
        col("nama_pasien"),
        col("dokter_bedah"),
        col("icd_x"),
        col("diagnosa"),
        col("tanggal_operasi"),
        col("jam_asesmen"),
        col("jam_operasi"),
        col("selisih_menit").cast("long"),
        col("status"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"Output row count: ${dfFinal.count()}")

    dfFinal
  }
}