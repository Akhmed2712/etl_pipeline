package etl_pipeline.scripts.my_orders_job

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import etl_pipeline.Transformer
import etl_pipeline.utils.CredentialsLoader

import scala.jdk.CollectionConverters._

/**
 * Transform
 *
 * Equivalent of scripts/my_job/transform.py.
 *
 * Extends Transformer so Main can load it dynamically via reflection.
 * Registered in catalog.yml as:
 * script_location: etl_pipeline.scripts.myjob.MyOrdersJob
 *
 * By the time processData() is called, ALL {~~~...~~~} templates in env
 * have already been resolved to real date strings by Main.
 */
object MyOrdersJob extends Transformer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * @param spark active SparkSession
   * @param env fully-resolved read_conf from catalog.yml
   * (all {~~~...~~~} templates already replaced with dates)
   * @return transformed DataFrame ready to be written to MySQL
   */
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame = {

    // -- Scalar dates (resolved from {~~~...~~~} templates) ------------------
    val currentDate = env("current_date").toString    // e.g. "2024-01-15"
    val loadDate = env("load_date").toString       // e.g. "2024-01-15"
    val currentMonth = env("current_month").toString   // e.g. "2024-01-01"

    log.info(s"current_date=$currentDate | load_date=$loadDate | current_month=$currentMonth")

    // -- Read source table — filter already resolved -------------------------
    val rejectConf = env("transaction_1").asInstanceOf[java.util.Map[String, Any]].asScala

    val props = new java.util.Properties()
    jdbcConfig.props.foreach { case (k, v) => props.setProperty(k, v) }

    val dfOrders = spark.read
      .jdbc(url = jdbcConfig.url, s"${rejectConf("database")}.${rejectConf("table")}", props)
      .filter(rejectConf("filter").toString)

    log.info(s"Source row count: ${dfOrders.count()}")

    // -- Transformation logic -------------------------------------------------
    val loadTs = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val dfResult = dfOrders
      .groupBy("customer_id", "order_status", "created_date")
      .agg(
        sum("amount").alias("total_amount"),
        max("id").alias("order_id")
      )
      .withColumn("load_ts", lit(loadTs))

    log.info(s"Output row count: ${dfResult.count()}")
    dfResult
  }
}