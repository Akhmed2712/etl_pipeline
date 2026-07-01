package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactMutuPasienJatuh extends Transformer {

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
        .filter(documentHistoryList("filter").toString)
        .select(
          col("document_history_id"),
          col("document_code"),
          col("created_time"),
          col("emr_data")
        )

    // =========================================================
    // 2. RM21B
    // Struktur: emr_data -> $.value (array of object)
    // Field skor bisa berupa integer atau string di JSON asli
    // =========================================================
    val rm21bItemSchema =
      StructType(Seq(
        StructField("tanggal", StringType, true),
        StructField("paraf_petugas", StringType, true),
        StructField("riwayat_jatuh",
          StructType(Seq(
            // skor bisa "0" atau 0 — pakai StringType lalu cast
            StructField("skor", StringType, true)
          )), true
        ),
        StructField("status_mental",
          StructType(Seq(
            StructField("agitasi",      StructType(Seq(StructField("nilai", StringType, true))), true),
            StructField("delirium",     StructType(Seq(StructField("nilai", StringType, true))), true),
            StructField("disorientasi", StructType(Seq(StructField("nilai", StringType, true))), true)
          )), true
        ),
        StructField("penglihatan",
          StructType(Seq(
            StructField("buram",    StructType(Seq(StructField("nilai", StringType, true))), true),
            StructField("glakoma",  StructType(Seq(StructField("nilai", StringType, true))), true),
            StructField("kacamata", StructType(Seq(StructField("nilai", StringType, true))), true)
          )), true
        ),
        StructField("berkemih",
          StructType(Seq(StructField("skor", StringType, true))), true
        ),
        StructField("transfer_mobilitas",
          StructType(Seq(StructField("skor_total", StringType, true))), true
        )
      ))

    val dfRm21b =
      dfSource
        .filter(col("document_code") === "RM21b_K")
        .withColumn("no_rm",  get_json_object(col("emr_data"), "$.pasien.no_rm"))
        .withColumn("pasien", get_json_object(col("emr_data"), "$.pasien.nama"))
        .withColumn("bangsal", get_json_object(col("emr_data"), "$.pasien.bangsal"))
        .withColumn(
          "items",
          explode(from_json(get_json_object(col("emr_data"), "$.value"), ArrayType(rm21bItemSchema)))
        )
        .withColumn("tanggal", col("items.tanggal"))
        .filter(col("tanggal").isNotNull && trim(col("tanggal")) =!= "")
        .withColumn("petugas", col("items.paraf_petugas"))
        .withColumn("skor_jatuh",
          coalesce(col("items.riwayat_jatuh.skor").cast("int"), lit(0))
        )
        .withColumn("skor_mental",
          greatest(
            coalesce(col("items.status_mental.agitasi.nilai").cast("int"),      lit(0)),
            coalesce(col("items.status_mental.delirium.nilai").cast("int"),     lit(0)),
            coalesce(col("items.status_mental.disorientasi.nilai").cast("int"), lit(0))
          )
        )
        .withColumn("skor_penglihatan",
          greatest(
            coalesce(col("items.penglihatan.buram.nilai").cast("int"),   lit(0)),
            coalesce(col("items.penglihatan.glakoma.nilai").cast("int"), lit(0)),
            coalesce(col("items.penglihatan.kacamata.nilai").cast("int"),lit(0))
          )
        )
        .withColumn("skor_berkemih",
          coalesce(col("items.berkemih.skor").cast("int"), lit(0))
        )
        .withColumn("skor_mobilitas",
          coalesce(col("items.transfer_mobilitas.skor_total").cast("int"), lit(0))
        )
        .withColumn("total_skor",
          col("skor_jatuh") + col("skor_mental") + col("skor_penglihatan") +
          col("skor_berkemih") + col("skor_mobilitas")
        )
        .withColumn("kategori_risiko",
          when(col("total_skor") >= 17, "Risiko Tinggi")
            .when(col("total_skor") >= 6,  "Risiko Sedang")
            .otherwise("Risiko Rendah")
        )
        .select(
          "document_history_id", "no_rm", "pasien", "tanggal", "bangsal",
          "petugas", "total_skor", "kategori_risiko"
        )

    // =========================================================
    // 3. RM21A
    // Struktur: emr_data root berisi key "parameter_1".."parameter_N"
    //           dan juga "parameter_pa" (dokter/PA) — semua diambil
    // Filter: key LIKE 'parameter_%' AND tgl tidak kosong
    // total_skor: integer di JSON (bukan string) — cast via string aman
    // =========================================================
    val dfRm21a =
      dfSource
        .filter(col("document_code") === "RM21a_K")
        .withColumn("no_rm",  get_json_object(col("emr_data"), "$.pasien.no_rm"))
        .withColumn("pasien", get_json_object(col("emr_data"), "$.pasien.nama"))
        .withColumn("bangsal", get_json_object(col("emr_data"), "$.pasien.bangsal"))
        // Parse seluruh root emr_data sebagai Map[String, String]
        // Nilai yang integer di JSON akan tetap terbaca sebagai string "0", "9", dst
        .withColumn(
          "emr_map",
          from_json(col("emr_data"), MapType(StringType, StringType))
        )
        // Explode map -> 2 kolom (map_key, map_value)
        .select(
          col("document_history_id"),
          col("no_rm"),
          col("pasien"),
          col("bangsal"),
          explode(col("emr_map")).as(Seq("map_key", "map_value"))
        )
        // Ambil semua key parameter_* (termasuk parameter_pa)
        .filter(col("map_key").like("parameter_%"))
        // Parse tiap value sebagai object JSON
        .withColumn(
          "v",
          from_json(col("map_value"), MapType(StringType, StringType))
        )
        .withColumn("tanggal", col("v.tgl"))
        .filter(col("tanggal").isNotNull && trim(col("tanggal")) =!= "")
        .withColumn("petugas", col("v.nama"))
        // total_skor: bisa "0" atau integer — coalesce null-safe
        .withColumn("total_skor",
          coalesce(
            when(col("v.total_skor").isNotNull && trim(col("v.total_skor")) =!= "",
              col("v.total_skor").cast("int")),
            lit(0)
          )
        )
        .withColumn("kategori_risiko",
          when(col("total_skor") <= 24, "Risiko Rendah")
            .when(col("total_skor") <= 50, "Risiko Sedang")
            .otherwise("Risiko Tinggi")
        )
        .select(
          "document_history_id", "no_rm", "pasien", "tanggal", "bangsal",
          "petugas", "total_skor", "kategori_risiko"
        )

    // =========================================================
    // 4. RM21C
    // Struktur: sama dengan RM21a (parameter_1..N + parameter_pa)
    // Field: total (bukan total_skor), resiko (bukan kategori_risiko)
    // PERHATIAN: nilai resiko di data asli = "Resiko Sedang" (bukan "Risiko")
    //            normalize ke "Risiko Tinggi/Sedang/Rendah" agar konsisten
    // =========================================================
    val dfRm21c =
      dfSource
        .filter(col("document_code") === "RM21c_K")
        .withColumn("no_rm",  get_json_object(col("emr_data"), "$.pasien.no_rm"))
        .withColumn("pasien", get_json_object(col("emr_data"), "$.pasien.nama"))
        .withColumn("bangsal", get_json_object(col("emr_data"), "$.pasien.bangsal"))
        .withColumn(
          "emr_map",
          from_json(col("emr_data"), MapType(StringType, StringType))
        )
        .select(
          col("document_history_id"),
          col("no_rm"),
          col("pasien"),
          col("bangsal"),
          explode(col("emr_map")).as(Seq("map_key", "map_value"))
        )
        .filter(col("map_key").like("parameter_%"))
        .withColumn(
          "v",
          from_json(col("map_value"), MapType(StringType, StringType))
        )
        .withColumn("tanggal", col("v.tgl"))
        .filter(col("tanggal").isNotNull && trim(col("tanggal")) =!= "")
        .withColumn("petugas", col("v.nama"))
        .withColumn("total_skor",
          coalesce(
            when(col("v.total").isNotNull && trim(col("v.total")) =!= "",
              col("v.total").cast("int")),
            lit(0)
          )
        )
        // Normalize typo "Resiko" -> "Risiko" agar konsisten dengan RM21b/RM21a
        .withColumn("kategori_risiko_raw",
          coalesce(
            when(col("v.resiko").isNotNull && trim(col("v.resiko")) =!= "", trim(col("v.resiko"))),
            lit("Tidak Ada")
          )
        )
        .withColumn("kategori_risiko",
          when(lower(col("kategori_risiko_raw")).contains("tinggi"), lit("Risiko Tinggi"))
            .when(lower(col("kategori_risiko_raw")).contains("sedang"), lit("Risiko Sedang"))
            .when(lower(col("kategori_risiko_raw")).contains("rendah"), lit("Risiko Rendah"))
            .otherwise(col("kategori_risiko_raw"))
        )
        .select(
          "document_history_id", "no_rm", "pasien", "tanggal", "bangsal",
          "petugas", "total_skor", "kategori_risiko"
        )

    // =========================================================
    // 5. UNION ALL
    // =========================================================
    val dfUnion = dfRm21b.unionByName(dfRm21a).unionByName(dfRm21c)

    // Pasien yang pernah Risiko Tinggi
    val dfHighRisk =
      dfUnion
        .filter(col("kategori_risiko") === "Risiko Tinggi")
        .select("no_rm")
        .distinct()

    // Semua data pasien high risk
    val dfFiltered =
      dfUnion.join(dfHighRisk, Seq("no_rm"), "inner")

    // =========================================================
    // 6. RM101 (intervensi)
    // =========================================================
    val dfRm101 =
      dfSource
        .filter(col("document_code") === "RM101")
        .select(col("document_history_id").as("rm101_doc_id"))
        .distinct()

    // =========================================================
    // 7. WINDOW
    // ranked  : ROW_NUMBER per no_rm, order tanggal desc, doc_id desc
    // ranked2 : MAX(rn) OVER (PARTITION BY no_rm) = total asesmen
    // =========================================================
    val wRank =
      Window.partitionBy("no_rm")
        .orderBy(to_date(col("tanggal")).desc, col("document_history_id").desc)

    val wMax =
      Window.partitionBy("no_rm")

    val dfRanked =
      dfFiltered
        .withColumn("rn", row_number().over(wRank))

    // total_asesmen dihitung setelah rn tersedia (MAX rn per partisi = jumlah baris per pasien)
    val dfRanked2 =
      dfRanked
        .withColumn("total_asesmen", max(col("rn")).over(wMax))

    // =========================================================
    // 8. FINAL
    // =========================================================
    val dfFinal =
      dfRanked2
        .filter(col("rn") === 1)
        .join(dfRm101, col("document_history_id") === col("rm101_doc_id"), "left")
        //.withColumn("status", lit("Asesmen Terakhir"))
        .withColumn("asesmen_awal",
          when(col("total_asesmen") >= 1, "Dilakukan").otherwise("Tidak")
        )
        .withColumn("asesmen_ulang",
          when(col("total_asesmen") > 1, "Dilakukan").otherwise("Tidak")
        )
        // rm101_doc_id null = tidak ada intervensi di RM101
        .withColumn("intervensi",
          when(col("rm101_doc_id").isNotNull, "Dilakukan").otherwise("Tidak")
        )
        .withColumn("status",
          when(col("rm101_doc_id").isNotNull, "Memenuhi").otherwise("Tidak Memenuhi")
        )
        .withColumn("partition_date", to_date(col("tanggal")))
        .withColumn("tahun",    year(col("partition_date")))
        .withColumn("triwulan", concat(lit("Q"), quarter(col("partition_date"))))
        .withColumn("nama_bulan",
          when(month(col("partition_date")) === 1,  "01-Januari")
          .when(month(col("partition_date")) === 2,  "02-Februari")
          .when(month(col("partition_date")) === 3,  "03-Maret")
          .when(month(col("partition_date")) === 4,  "04-April")
          .when(month(col("partition_date")) === 5,  "05-Mei")
          .when(month(col("partition_date")) === 6,  "06-Juni")
          .when(month(col("partition_date")) === 7,  "07-Juli")
          .when(month(col("partition_date")) === 8,  "08-Agustus")
          .when(month(col("partition_date")) === 9,  "09-September")
          .when(month(col("partition_date")) === 10, "10-Oktober")
          .when(month(col("partition_date")) === 11, "11-November")
          .otherwise("12-Desember")
        )
        .withColumn("id", col("document_history_id").cast("bigint"))
        .select(
          "id",
          "no_rm",
          "pasien",
          "tanggal",
          "bangsal",
          "petugas",
          "total_skor",
          "kategori_risiko",
          "total_asesmen",
          //"status",
          "asesmen_awal",
          "asesmen_ulang",
          "intervensi",
          "status",
          "tahun",
          "triwulan",
          "nama_bulan",
          "partition_date"
        )

    log.info(s"Output row count: ${dfFinal.count()}")

    dfFinal
  }
}