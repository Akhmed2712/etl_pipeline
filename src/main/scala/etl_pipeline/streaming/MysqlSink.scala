package etl_pipeline.streaming

import java.sql.DriverManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import etl_pipeline.utils.CredentialsLoader

object MysqlSink {

  def sink(
      df: DataFrame,
      jdbcConfig: CredentialsLoader.JdbcConfig,
      table: String,
      primaryKey: String
  ): Unit = {

    val upsert = df.filter(col("op").isin("c", "u", "r"))
    val delete = df.filter(col("op") === "d")

    // isEmpty aman dipanggil di sini karena df ini datang dari
    // dalam foreachBatch (bounded), bukan streaming DataFrame langsung
    if (!upsert.isEmpty) writeUpsert(upsert, jdbcConfig, table, primaryKey)
    if (!delete.isEmpty) writeDelete(delete, jdbcConfig, table, primaryKey)
  }

  private def connect(jdbcConfig: CredentialsLoader.JdbcConfig) = {
    val driver = jdbcConfig.props.getOrElse("driver", "com.mysql.cj.jdbc.Driver")
    Class.forName(driver)
    DriverManager.getConnection(
      jdbcConfig.url,
      jdbcConfig.props("user"),
      jdbcConfig.props("password")
    )
  }

  private def writeUpsert(
      df: DataFrame,
      jdbcConfig: CredentialsLoader.JdbcConfig,
      table: String,
      primaryKey: String
  ): Unit = {

    val columns = df.select("after.*").columns
    val insertColumns = columns.mkString(",")
    val placeholders = columns.map(_ => "?").mkString(",")
    val updates = columns.filter(_ != primaryKey).map(c => s"$c = VALUES($c)").mkString(",")

    val sql =
      s"""
         |INSERT INTO $table ($insertColumns)
         |VALUES ($placeholders)
         |ON DUPLICATE KEY UPDATE $updates
         |""".stripMargin

    df.select("after.*").foreachPartition { rows =>
      val conn = connect(jdbcConfig)
      conn.setAutoCommit(false)
      val stmt = conn.prepareStatement(sql)
      rows.foreach { row =>
        columns.indices.foreach(i => stmt.setObject(i + 1, row.get(i)))
        stmt.addBatch()
      }
      stmt.executeBatch()
      conn.commit()
      stmt.close()
      conn.close()
    }
  }

  private def writeDelete(
      df: DataFrame,
      jdbcConfig: CredentialsLoader.JdbcConfig,
      table: String,
      primaryKey: String
  ): Unit = {

    val sql = s"DELETE FROM $table WHERE $primaryKey = ?"

    df.select("before.*").foreachPartition { rows =>
      val conn = connect(jdbcConfig)
      conn.setAutoCommit(false)
      val stmt = conn.prepareStatement(sql)
      rows.foreach { row =>
        stmt.setObject(1, row.getAs[Any](primaryKey))
        stmt.addBatch()
      }
      stmt.executeBatch()
      conn.commit()
      stmt.close()
      conn.close()
    }
  }
}