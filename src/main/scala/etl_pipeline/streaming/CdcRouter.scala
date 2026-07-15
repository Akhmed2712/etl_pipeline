package etl_pipeline.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CdcRouter {

  /**
    * Memisahkan CDC berdasarkan nama tabel
    */
  def route(df: DataFrame): Map[String, DataFrame] = {

    val tables =
      df.select("table_name")
        .distinct()
        .collect()
        .map(_.getString(0))

    tables.map { table =>

      val tableDf =
        df.filter(col("table_name") === table)

      table -> tableDf

    }.toMap
  }

  /**
    * Mengambil hanya INSERT
    */
  def inserts(df: DataFrame): DataFrame =
    df.filter(col("op") === "c")

  /**
    * Mengambil hanya UPDATE
    */
  def updates(df: DataFrame): DataFrame =
    df.filter(col("op") === "u")

  /**
    * Mengambil hanya DELETE
    */
  def deletes(df: DataFrame): DataFrame =
    df.filter(col("op") === "d")

  /**
    * Snapshot awal
    */
  def snapshots(df: DataFrame): DataFrame =
    df.filter(col("op") === "r")

  /**
    * Snapshot + Insert
    */
  def upserts(df: DataFrame): DataFrame =
    df.filter(col("op").isin("r", "c", "u"))

}