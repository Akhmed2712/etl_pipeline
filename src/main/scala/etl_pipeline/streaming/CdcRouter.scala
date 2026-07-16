package etl_pipeline.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CdcRouter {

  /**
    * Split DataFrame envelope generik (hasil DebeziumParser.parse) jadi
    * satu DataFrame per tabel, sekaligus nge-type before/after sesuai
    * schema masing-masing tabel dari registry.
    *
    * PENTING: daftar tabel diambil dari `schemas` (config), BUKAN
    * di-discover dari data via collect() — itu bug versi lama yang
    * nggak akan jalan di atas streaming DataFrame.
    */
  def route(
    df: DataFrame,
    schemas: Map[String, CdcTableSchema]
  ): Map[String, DataFrame] = {

    schemas.map { case (tableName, tableSchema) =>
      val structType = CdcSchemaLoader.toStructType(tableSchema)

      val typedDf = df
        .filter(col("table") === tableName)
        .withColumn("before", from_json(col("before_json"), structType))
        .withColumn("after", from_json(col("after_json"), structType))
        .withColumn("before", CdcSchemaLoader.decodeLogicalTypes(col("before"), tableSchema))
        .withColumn("after", CdcSchemaLoader.decodeLogicalTypes(col("after"), tableSchema))
        .drop("before_json", "after_json")

      tableName -> typedDf
    }
  }

  def inserts(df: DataFrame): DataFrame = df.filter(col("op") === "c")
  def updates(df: DataFrame): DataFrame = df.filter(col("op") === "u")
  def deletes(df: DataFrame): DataFrame = df.filter(col("op") === "d")
  def snapshots(df: DataFrame): DataFrame = df.filter(col("op") === "r")
  def upserts(df: DataFrame): DataFrame = df.filter(col("op").isin("r", "c", "u"))
}