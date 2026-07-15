package etl_pipeline.scripts.agg

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

object FgAggAbcAnalysis extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  def processData(
      spark: SparkSession,
      env: Map[String, Any],
      jdbcConfig: CredentialsLoader.JdbcConfig
  ): DataFrame = {

    val readConfigs = CredentialsLoader.loadAllReadSources(env)

    val fact =
      env("fg_fact_pergerakan_stok_bulanan")
        .asInstanceOf[Map[String, Any]]

    val conf = readConfigs("fg_fact_pergerakan_stok_bulanan")

    val props = new Properties()
    conf.props.foreach { case (k, v) =>
      props.setProperty(k, v)
    }

    val df = spark.read
      .jdbc(
        conf.url,
        s"${fact("database")}.${fact("table")}",
        props
      )
      .filter(fact("filter").toString)

    val totalPerMonth = df
        .groupBy("partition_date")
        .agg(
            sum("nilai_stok_terakhir")
            .alias("total_nilai_stok")
        )

    val dfTotal = df.join(
        totalPerMonth,
        Seq("partition_date")
    )

    val baseDate = to_date(col("partition_date"))

    val windowSpec = Window
    .partitionBy("partition_date")
    .orderBy(col("nilai_stok_terakhir").desc)

    val dfResult = dfTotal
      .withColumn(
        "ranking",
        row_number().over(windowSpec)
      )
      .withColumn(
        "persentase_nilai",
        when(col("total_nilai_stok") === 0, lit(0))
          .otherwise(
            col("nilai_stok_terakhir") /
              col("total_nilai_stok")
          )
      )
      .withColumn(
        "persentase_kumulatif",
        sum("persentase_nilai")
          .over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
      )
      .withColumn(
        "kategori_abc",
        when(col("persentase_kumulatif") <= 0.80, "A")
          .when(col("persentase_kumulatif") <= 0.95, "B")
          .otherwise("C")
      )
      .withColumn("tahun", year(baseDate))
      .withColumn("triwulan", concat(lit("Q"), quarter(baseDate)))
      .withColumn("nama_bulan",
        when(month(baseDate)===1,"01-Januari")
        .when(month(baseDate)===2,"02-Februari")
        .when(month(baseDate)===3,"03-Maret")
        .when(month(baseDate)===4,"04-April")
        .when(month(baseDate)===5,"05-Mei")
        .when(month(baseDate)===6,"06-Juni")
        .when(month(baseDate)===7,"07-Juli")
        .when(month(baseDate)===8,"08-Agustus")
        .when(month(baseDate)===9,"09-September")
        .when(month(baseDate)===10,"10-Oktober")
        .when(month(baseDate)===11,"11-November")
        .otherwise("12-Desember"))
      .withColumn("partition_date", baseDate)
      .select(
        col("id_barang"),
        col("nama_barang"),
        col("kategori"),
        col("jumlah_stok_terakhir"),
        col("nilai_stok_terakhir"),
        col("total_nilai_stok"),
        round(col("persentase_nilai") * 100, 2)
          .alias("persentase_nilai"),
        round(col("persentase_kumulatif") * 100, 2)
          .alias("persentase_kumulatif"),
        col("kategori_abc"),
        col("ranking"),
        col("tahun"),
        col("triwulan"),
        col("nama_bulan"),
        col("partition_date")
      )

    log.info(s"ABC Analysis Count : ${dfResult.count()}")

    dfResult
  }

}