package etl_pipeline.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DebeziumParser {

  /**
    * Parse Debezium CDC message ketika value.converter.schemas.enable = false.
    *
    * Format pesan Kafka (flat, tanpa wrapper "schema"/"payload"):
    * {
    *   "before": { ... } | null,
    *   "after":  { ... } | null,
    *   "source": { "db": ..., "table": ..., "file": ..., "pos": ..., "snapshot": ... },
    *   "op": "c" | "u" | "d" | "r",
    *   "ts_ms": 1234567890
    * }
    *
    * PENTING: "before" dan "after" SENGAJA tidak di-typing di sini (tetap
    * sebagai teks JSON mentah). Alasannya: struktur kolom tiap tabel beda-beda,
    * dan parser ini harus tetap generik untuk tabel apapun tanpa modifikasi.
    * Typing per tabel (mapping kolom & tipe data) dilakukan belakangan, di
    * tahap Router — setelah kita tahu pesan ini milik tabel yang mana.
    */
  def parse(kafkaDf: DataFrame): DataFrame = {

    kafkaDf
      .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),

        // Tetap string JSON mentah — belum di-parse jadi struct
        get_json_object(col("kafka_value"), "$.before").alias("before_json"),
        get_json_object(col("kafka_value"), "$.after").alias("after_json"),

        get_json_object(col("kafka_value"), "$.op").alias("op"),

        get_json_object(col("kafka_value"), "$.ts_ms")
          .cast("long")
          .alias("event_ts"),

        get_json_object(col("kafka_value"), "$.source.db").alias("database"),
        get_json_object(col("kafka_value"), "$.source.table").alias("table"),
        get_json_object(col("kafka_value"), "$.source.file").alias("binlog_file"),

        get_json_object(col("kafka_value"), "$.source.pos")
          .cast("long")
          .alias("binlog_position"),

        get_json_object(col("kafka_value"), "$.source.snapshot")
          .alias("snapshot")
      )
  }
}