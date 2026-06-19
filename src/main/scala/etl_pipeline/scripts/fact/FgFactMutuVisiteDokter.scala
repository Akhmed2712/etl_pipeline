package etl_pipeline.scripts.fact

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgFactMutuVisiteDokter extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    val documentHistoryList =
      env("document_history_list")
        .asInstanceOf[Map[String, Any]]

    val props = new Properties()

    jdbcConfig.props.foreach {
      case (k, v) =>
        props.setProperty(k, v)
    }

    val tableName =
      s"${documentHistoryList("database")}.${documentHistoryList("table")}"

    val dfSource =
      spark.read
        .jdbc(
          jdbcConfig.url,
          tableName,
          props
        )
        .select(
          col("document_history_list_id"),
          col("document_code"),
          col("label"),
          col("created_time"),
          col("emr_data")
        )
        .filter(documentHistoryList("filter").toString)

    val dataListSchema =
      ArrayType(
        StructType(
          Seq(
            StructField(
              "jam_cppt",
              StringType,
              true
            ),
            StructField(
              "tanggal_cppt",
              StringType,
              true
            ),
            StructField(
              "profesi",
              StringType,
              true
            ),
            StructField(
              "pilihan",
              StringType,
              true
            ),
            StructField(
              "klinik",
              StringType,
              true
            ),
            StructField(
              "verifikasi_dpjp",
              StructType(
                Seq(
                  StructField(
                    "nama_dpjp",
                    StringType,
                    true
                  )
                )
              ),
              true
            )
          )
        )
      )

    val dfExploded =
      dfSource
        .withColumn(
          "no_rm",
          get_json_object(
            col("emr_data"),
            "$.pasien.no_rm"
          )
        )
        .withColumn(
          "pasien",
          get_json_object(
            col("emr_data"),
            "$.pasien.nama"
          )
        )
        .withColumn(
          "data_list",
          from_json(
            get_json_object(
              col("emr_data"),
              "$.data_list"
            ),
            dataListSchema
          )
        )
        .withColumn(
          "item",
          explode(col("data_list"))
        )

    val dfFiltered =
      dfExploded
        .filter(
          lower(col("item.profesi")) === "dokter"
        )

    val baseDate =
      to_date(
        col("item.tanggal_cppt"),
        "yyyy-MM-dd"
      )

    val dfFinal =
      dfFiltered
        .withColumn(
          "status",
          when(
            baseDate === to_date(col("created_time")),
            "Eksklusi"
          )
            .when(
              col("item.jam_cppt")
                .between("06:00", "14:00"),
              "Memenuhi"
            )
            .otherwise(
              "Tidak Memenuhi"
            )
        )
        .withColumn(
          "tahun",
          year(baseDate)
        )
        .withColumn(
          "triwulan",
          concat(
            lit("Q"),
            quarter(baseDate)
          )
        )
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
        .withColumn(
          "partition_date",
          baseDate
        )
        .select(
          col("document_history_list_id")
            .cast("bigint")
            .alias("id"),

          col("no_rm"),

          col("pasien"),

          col("item.klinik")
            .alias("klinik"),

          col("item.verifikasi_dpjp.nama_dpjp")
            .alias("dokter"),

          to_date(col("created_time"))
            .alias("tanggal_masuk"),

          col("item.tanggal_cppt")
            .alias("tanggal_visite"),

          col("item.jam_cppt")
            .alias("jam"),

          col("item.pilihan")
            .alias("pemeriksaan"),

          col("status"),

          col("tahun"),

          col("triwulan"),

          col("nama_bulan"),

          col("partition_date")
        )

    log.info(
      s"Output row count: ${dfFinal.count()}"
    )

    dfFinal
  }
}