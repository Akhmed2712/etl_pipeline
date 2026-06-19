package etl_pipeline.utils

import java.io.{File, FileInputStream}
import java.util.{Map => JMap}

import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._

object CredentialsLoader {

  // =========================================================================
  // Credentials File Path
  // =========================================================================

  val CredentialsPath: String =
    new File(
      System.getProperty("user.dir"),
      "credentials/credentials.yml"
    ).getAbsolutePath

  case class JdbcConfig(
    url: String,
    props: Map[String, String]
  )

  // =========================================================================
  // Resource Path Resolver
  // =========================================================================

  /**
   * Resolve resource.yml path based on env
   */
  private def getResourcePath(
    catalogConf: Map[String, Any]
  ): String = {

    val env =
      catalogConf.getOrElse("env", "dev").toString

    new File(
      System.getProperty("user.dir"),
      s"catalogs/$env/resource.yml"
    ).getAbsolutePath
  }

  // =========================================================================
  // YAML Loader
  // =========================================================================

  /**
   * Read YAML file into Scala Map
   */
  private def loadYaml(
    path: String
  ): Map[String, Any] = {

    val file = new File(path)

    if (!file.exists()) {
      println(s"WARNING: File not found at $path")
      return Map.empty
    }

    val fis = new FileInputStream(file)

    try {

      val yaml = new Yaml()

      val raw =
        yaml.load[JMap[String, Any]](fis)

      if (raw == null)
        Map.empty
      else
        raw.asScala.toMap

    } finally {
      fis.close()
    }
  }

  // =========================================================================
  // Main JDBC Config Loader
  // =========================================================================

  /**
   * Build JDBC Config for Read / Write
   */
  def loadJdbcConfig(
    catalogConf: Map[String, Any],
    isWrite: Boolean = false
  ): JdbcConfig = {

    // -----------------------------------------------------------------------
    // 1. Resolve Credential Key
    // -----------------------------------------------------------------------

    val credKey =
      if (isWrite) {
        catalogConf
          .getOrElse(
            "write_credentials_key",
            catalogConf.getOrElse("credentials_key", "")
          )
          .toString
      } else {
        catalogConf
          .getOrElse("credentials_key", "")
          .toString
      }

    // -----------------------------------------------------------------------
    // 2. Load credentials.yml
    // -----------------------------------------------------------------------

    val allCreds =
      loadYaml(CredentialsPath)

    require(
      allCreds.contains(credKey),
      s"Key '$credKey' tidak ditemukan di $CredentialsPath"
    )

    val creds =
      allCreds(credKey)
        .asInstanceOf[JMap[String, Any]]
        .asScala
        .toMap

    // -----------------------------------------------------------------------
    // 3. Load resource.yml
    // -----------------------------------------------------------------------

    val resourcePath =
      getResourcePath(catalogConf)

    val allResources =
      loadYaml(resourcePath)

    val tableName =
      catalogConf
        .getOrElse("table", "")
        .toString

    val resourceConfig =
      allResources.get(tableName) match {

        case Some(m: JMap[_, _]) =>
          m.asInstanceOf[JMap[String, Any]]
            .asScala
            .toMap

        case _ =>
          Map.empty[String, Any]
      }

    // -----------------------------------------------------------------------
    // 4. Base JDBC Info
    // -----------------------------------------------------------------------

    val host =
      creds("host").toString

    val port =
      creds.getOrElse("port", 3306).toString

    val database =
      if (isWrite) {
        catalogConf
          .getOrElse(
            "write_database",
            catalogConf.getOrElse("database", "")
          )
          .toString
      } else {
        catalogConf
          .getOrElse("database", "")
          .toString
      }

    val driver =
      creds
        .getOrElse(
          "driver",
          "com.mysql.cj.jdbc.Driver"
        )
        .toString

    // -----------------------------------------------------------------------
    // 5. JDBC URL Parameters
    // -----------------------------------------------------------------------

    val defaultJdbcParams = Map(
      "useSSL" -> "false",
      "serverTimezone" -> "Asia/Jakarta"
    )

    /**
     * Any extra JDBC params allowed from credentials.yml
     *
     * Example:
     * zeroDateTimeBehavior: convertToNull
     * useUnicode: true
     * characterEncoding: UTF-8
     */
    val excludedKeys = Set(
      "host",
      "port",
      "user",
      "password",
      "driver",
      "partitionColumn",
      "lowerBound",
      "upperBound",
      "numPartitions",
      "batchsize"
    )

    val extraJdbcParams =
      creds.collect {
        case (k, v)
          if !excludedKeys.contains(k) =>
          k -> v.toString
      }

    val jdbcParams =
      (defaultJdbcParams ++ extraJdbcParams)
        .map { case (k, v) =>
          s"$k=$v"
        }
        .mkString("&")

    val dbType =
      creds.getOrElse("type", "mysql").toString

    val jdbcUrl =
      dbType match {
        case "postgres" =>
          s"jdbc:postgresql://$host:$port/$database?$jdbcParams"

        case "mysql" =>
          s"jdbc:mysql://$host:$port/$database?$jdbcParams"

        case other =>
          throw new IllegalArgumentException(
            s"Unsupported database type: $other"
          )
      }

    // -----------------------------------------------------------------------
    // 6. Base JDBC Properties
    // -----------------------------------------------------------------------

    val baseProps = Map(
      "user" -> creds("user").toString,
      "password" -> creds("password").toString,
      "driver" -> driver
    )

    // -----------------------------------------------------------------------
    // 7. Spark JDBC Tuning Props
    // -----------------------------------------------------------------------

    val tuningKeys = Seq(
      "partitionColumn",
      "lowerBound",
      "upperBound",
      "numPartitions",
      "batchsize"
    )

    /**
     * Priority:
     *
     * resource.yml
     * -> catalog.yml
     * -> credentials.yml
     */
    val tuningProps =
      tuningKeys.flatMap { key =>

        val value =
          resourceConfig.get(key)
            .orElse(catalogConf.get(key))
            .orElse(creds.get(key))

        value.map(v => key -> v.toString)

      }.toMap

    // -----------------------------------------------------------------------
    // 8. Debug Logging
    // -----------------------------------------------------------------------

    println(
      s"""
         |DEBUG JDBC CONFIG
         |Mode       : ${if (isWrite) "WRITE" else "READ"}
         |Table      : $tableName
         |Resource   : $resourcePath
         |URL        : $jdbcUrl
         |Tuning     : $tuningProps
         |""".stripMargin
    )

    // -----------------------------------------------------------------------
    // 9. Final Config
    // -----------------------------------------------------------------------

    JdbcConfig(
      jdbcUrl,
      baseProps ++ tuningProps
    )
  }

  // =========================================================================
  // Public Helpers
  // =========================================================================

  def loadCredentials(
    catalogConf: Map[String, Any]
  ): JdbcConfig =
    loadJdbcConfig(catalogConf, isWrite = false)

  def loadWriteCredentials(
    catalogConf: Map[String, Any]
  ): JdbcConfig =
    loadJdbcConfig(catalogConf, isWrite = true)
}