package etl_pipeline.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MysqlSink {

  def sink(
      df: DataFrame,
      jdbcUrl: String,
      username: String,
      password: String,
      table: String,
      primaryKey: String
  ): Unit = {

    val upsert =
      df.filter(col("op").isin("c", "u", "r"))

    val delete =
      df.filter(col("op") === "d")

    writeUpsert(
      upsert,
      jdbcUrl,
      username,
      password,
      table,
      primaryKey
    )

    writeDelete(
      delete,
      jdbcUrl,
      username,
      password,
      table,
      primaryKey
    )

  }

  private def writeUpsert(
      df: DataFrame,
      jdbcUrl: String,
      username: String,
      password: String,
      table: String,
      primaryKey: String
  ): Unit = {

    val columns =
      df.select("after.*").columns

    val insertColumns =
      columns.mkString(",")

    val placeholders =
      columns.map(_ => "?").mkString(",")

    val updates =
      columns
        .filter(_ != primaryKey)
        .map(c => s"$c = VALUES($c)")
        .mkString(",")

    val sql =
      s"""
         |INSERT INTO $table ($insertColumns)
         |VALUES ($placeholders)
         |ON DUPLICATE KEY UPDATE
         |$updates
         |""".stripMargin

    df.select("after.*")
      .foreachPartition { rows =>

        val conn =
          DriverManager.getConnection(
            jdbcUrl,
            username,
            password
          )

        conn.setAutoCommit(false)

        val stmt =
          conn.prepareStatement(sql)

        rows.foreach { row =>

          columns.indices.foreach { i =>
            stmt.setObject(i + 1, row.get(i))
          }

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
      jdbcUrl: String,
      username: String,
      password: String,
      table: String,
      primaryKey: String
  ): Unit = {

    val sql =
      s"DELETE FROM $table WHERE $primaryKey = ?"

    df.select("before.*")
      .foreachPartition { rows =>

        val conn =
          DriverManager.getConnection(
            jdbcUrl,
            username,
            password
          )

        conn.setAutoCommit(false)

        val stmt =
          conn.prepareStatement(sql)

        rows.foreach { row =>

          stmt.setObject(
            1,
            row.getAs[Any](primaryKey)
          )

          stmt.addBatch()

        }

        stmt.executeBatch()

        conn.commit()

        stmt.close()
        conn.close()

      }

  }

}