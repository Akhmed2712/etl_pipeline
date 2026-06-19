package etl_pipeline.utils

import org.apache.spark.sql.DataFrame

object SchemaValidator {

  def validateWriteSchema(
    df: DataFrame,
    catalogConf: Map[String, Any]
  ): Unit = {

    // ── Ambil write_conf ────────────────────────────────────────────────────
    val writeConf =
      catalogConf("write_conf")
        .asInstanceOf[Map[String, Any]]

    // ── Ambil schema dari YAML ──────────────────────────────────────────────
    val schema =
      writeConf("schema")
        .asInstanceOf[List[Any]]
        .map(_.asInstanceOf[Map[String, Any]])

    // ── Expected Columns dari catalog ───────────────────────────────────────
    val expectedColumns =
      schema.map(_("column_name").toString)

    // ── Actual Columns dari DataFrame ───────────────────────────────────────
    val actualColumns =
      df.columns.toList

    // ── Missing Columns ─────────────────────────────────────────────────────
    val missing =
      expectedColumns.diff(actualColumns)

    // ── Unexpected Columns ──────────────────────────────────────────────────
    val unexpected =
      actualColumns.diff(expectedColumns)

    // ── Throw Error jika mismatch ───────────────────────────────────────────
    if (
      missing.nonEmpty ||
      unexpected.nonEmpty
    ) {

      val error =
        s"""
           |
           |SCHEMA VALIDATION FAILED
           |
           |Expected Columns:
           |${expectedColumns.mkString(", ")}
           |
           |Actual Columns:
           |${actualColumns.mkString(", ")}
           |
           |Missing Columns:
           |${missing.mkString(", ")}
           |
           |Unexpected Columns:
           |${unexpected.mkString(", ")}
           |
           |""".stripMargin

      throw new IllegalArgumentException(error)
    }

    println("Schema validation passed.")
  }
}