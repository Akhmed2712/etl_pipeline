package etl_pipeline.streaming

import java.io.{File, FileInputStream}
import java.util.{Map => JMap}

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._

case class CdcColumnDef(
  name: String,
  sparkType: String,
  logicalType: Option[String]
)

case class CdcTableSchema(
  primaryKey: String,
  writeCredentialsKey: String,
  writeDatabase: String,
  writeTable: String,
  columns: Seq[CdcColumnDef]
)

object CdcSchemaLoader {

  /** Load semua definisi tabel dari cdc/schemas.yml */
  def loadAll(path: String = "cdc/schemas.yml"): Map[String, CdcTableSchema] = {
    val file = new File(System.getProperty("user.dir"), path)
    val fis = new FileInputStream(file)
    try {
      val yaml = new Yaml()
      val raw = yaml.load[JMap[String, Any]](fis).asScala.toMap

      raw.map { case (tableName, rawConfRaw) =>
        val rawConf = rawConfRaw.asInstanceOf[JMap[String, Any]].asScala.toMap
        val primaryKey = rawConf("primary_key").toString

        val columns = rawConf("columns")
          .asInstanceOf[java.util.List[JMap[String, Any]]]
          .asScala
          .map { c =>
            val m = c.asScala.toMap
            CdcColumnDef(
              name = m("name").toString,
              sparkType = m("type").toString,
              logicalType = m.get("logical_type").map(_.toString)
            )
          }
          .toSeq

        tableName -> CdcTableSchema(primaryKey, columns)
      }
    } finally {
      fis.close()
    }
  }

  /** Convert definisi kolom jadi StructType, buat parsing raw JSON (before/after) */
  def toStructType(schema: CdcTableSchema): StructType = {
    StructType(
      schema.columns.map { c =>
        val dataType = c.sparkType match {
          case "bigint"  => LongType
          case "int"     => IntegerType
          case "string"  => StringType
          case "double"  => DoubleType
          case "boolean" => BooleanType
          case other     => throw new IllegalArgumentException(s"Unsupported cdc column type: $other")
        }
        StructField(c.name, dataType, nullable = true)
      }
    )
  }

  /**
    * Decode logical types di dalam struct column (before/after) yang
    * datanya masih mentah dari Debezium (mis. epoch day -> date asli).
    *
    * Perlu dipanggil SETELAH from_json, karena butuh struct sudah typed.
    */
  def decodeLogicalTypes(structCol: Column, schema: CdcTableSchema): Column = {
    schema.columns.foldLeft(structCol) { (accCol, colDef) =>
      colDef.logicalType match {

        case Some("date_epoch_day") =>
          accCol.withField(
            colDef.name,
            date_add(lit("1970-01-01").cast("date"), accCol.getField(colDef.name))
          )

        case Some("timestamp_epoch_millis") =>
          accCol.withField(
            colDef.name,
            (accCol.getField(colDef.name) / 1000).cast("timestamp")
          )

        case Some("timestamp_epoch_micros") =>
          accCol.withField(
            colDef.name,
            (accCol.getField(colDef.name) / 1000000).cast("timestamp")
          )

        case _ => accCol
      }
    }
  }
}