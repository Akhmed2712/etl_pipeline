package etl_pipeline.utils

import java.io.{File, FileInputStream}
import java.util.{Map => JMap}
import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._

object CredentialsLoader {

  // =========================================================================
  // Path Konfigurasi Utama
  // =========================================================================

  val CredentialsPath: String =
    new File(System.getProperty("user.dir"), "credentials/credentials.yml").getAbsolutePath

  case class JdbcConfig(
    url: String,
    props: Map[String, String]
  )

  // =========================================================================
  // Helper: Path & YAML Resolver
  // =========================================================================

  private def getResourcePath(catalogConf: Map[String, Any]): String = {
    val env = catalogConf.getOrElse("env", "dev").toString
    new File(System.getProperty("user.dir"), s"catalogs/$env/resource.yml").getAbsolutePath
  }

  private def loadYaml(path: String): Map[String, Any] = {
    val file = new File(path)
    if (!file.exists()) return Map.empty
    val fis = new FileInputStream(file)
    try {
      val yaml = new Yaml()
      val raw = yaml.load[JMap[String, Any]](fis)
      if (raw == null) Map.empty else raw.asScala.toMap
    } finally {
      fis.close()
    }
  }

  // =========================================================================
  // Core Engine: JDBC Config Loader
  // =========================================================================

  def loadJdbcConfig(
    catalogConf: Map[String, Any],
    isWrite: Boolean = false,
    sourceName: String = "-",
    filter: String = "-"
  ): JdbcConfig = {

    // 1. Resolve Credential Key
    val credKey = if (isWrite) {
      catalogConf.getOrElse("write_credentials_key", catalogConf.getOrElse("credentials_key", "")).toString
    } else {
      catalogConf.getOrElse("credentials_key", "").toString
    }

    // 2. Load credentials.yml
    val allCreds = loadYaml(CredentialsPath)
    require(allCreds.contains(credKey), s"Key '$credKey' tidak ditemukan di $CredentialsPath")

    val creds = allCreds(credKey).asInstanceOf[JMap[String, Any]].asScala.toMap

    // 3. Load resource.yml
    val resourcePath = getResourcePath(catalogConf)
    val allResources = loadYaml(resourcePath)
    val tableName = catalogConf.getOrElse("table", "").toString

    val resourceConfig = allResources.get(tableName) match {
      case Some(m: JMap[_, _]) => m.asInstanceOf[JMap[String, Any]].asScala.toMap
      case _                   => Map.empty[String, Any]
    }

    // 4. Base DB Info
    val host = creds("host").toString
    val port = creds.getOrElse("port", 3306).toString
    
    val database = if (isWrite) {
      catalogConf.getOrElse("write_database", catalogConf.getOrElse("database", "")).toString
    } else {
      // Fallback: cek key "database" atau "schema" dari YAML
      catalogConf.get("database").orElse(catalogConf.get("schema")).getOrElse("").toString
    }

    val driver = creds.getOrElse("driver", "com.mysql.cj.jdbc.Driver").toString

    // 5. URL Parameters
    val defaultJdbcParams = Map("useSSL" -> "false", "serverTimezone" -> "Asia/Jakarta")
    val excludedKeys = Set("host", "port", "user", "password", "driver", "partitionColumn", "lowerBound", "upperBound", "numPartitions", "batchsize")
    val extraJdbcParams = creds.collect { case (k, v) if !excludedKeys.contains(k) => k -> v.toString }
    val jdbcParams = (defaultJdbcParams ++ extraJdbcParams).map { case (k, v) => s"$k=$v" }.mkString("&")

    val dbType = creds.getOrElse("type", "mysql").toString
    val jdbcUrl = dbType match {
      case "postgres" => s"jdbc:postgresql://$host:$port/$database?$jdbcParams"
      case "mysql"    => s"jdbc:mysql://$host:$port/$database?$jdbcParams"
      case other      => throw new IllegalArgumentException(s"Unsupported database type: $other")
    }

    // 6. Properties & Tuning
    val baseProps = Map("user" -> creds("user").toString, "password" -> creds("password").toString, "driver" -> driver)
    val tuningKeys = Seq("partitionColumn", "lowerBound", "upperBound", "numPartitions", "batchsize")
    val tuningProps = tuningKeys.flatMap { key =>
      val value = resourceConfig.get(key).orElse(catalogConf.get(key)).orElse(creds.get(key))
      value.map(v => key -> v.toString)
    }.toMap

    // 7. Format Log Output Terminal sesuai request-mu
    // =========================================================================
    // 7. FORMAT DEBUG OUTPUT SESUAI REQ (Update Bagian Ini)
    // =========================================================================
    
    // Log hanya dicetak jika WRITE atau READ yang memiliki nama Source resmi
    if (isWrite || sourceName != "-") {
      println(
        s"""
           |DEBUG JDBC CONFIG
           |Mode       : ${if (isWrite) "WRITE" else "READ"}
           |Source     : $sourceName
           |Table      : $tableName
           |Database   : $database
           |Resource   : $resourcePath
           |URL        : $jdbcUrl
           |Filter     : $filter
           |Tuning     : $tuningProps
           |""".stripMargin
      )
    }

    JdbcConfig(jdbcUrl, baseProps ++ tuningProps)
  }

// =========================================================================
  // Public Interface API (Ganti bagian ini di CredentialsLoader.scala)
  // =========================================================================

  /**
   * Otomatis melakukan loop dari catalog.
   * Mendukung skema dengan wrapper `read_conf` maupun yang sudah di-flat langsung.
   */
  def loadAllReadSources(
    catalogConf: Map[String, Any]
  ): Map[String, JdbcConfig] = {

    // Deteksi: Jika ada `read_conf` gunakan isinya, jika tidak ada, 
    // anggap catalogConf itu sendiri adalah map sources (filter hanya yang value-nya berwujud Map)
    val readConfMap = catalogConf.get("read_conf") match {
      case Some(m: JMap[_, _]) => m.asInstanceOf[JMap[String, Any]].asScala.toMap
      case Some(m: Map[_, _])  => m.asInstanceOf[Map[String, Any]]
      case _ =>
        catalogConf.filter { case (_, value) =>
          value.isInstanceOf[Map[_, _]] || value.isInstanceOf[JMap[_, _]]
        }
    }

    // Cetak Header Summary Pencarian Source
    println(s"\nDEBUG loadAllReadSources: found ${readConfMap.size} sources")

    // Jalankan Loop Runner untuk generate Map[SourceName -> JdbcConfig]
    readConfMap.map { case (sourceKey, sourceDataRaw) =>
      val sourceConf = sourceDataRaw match {
        case m: JMap[_, _] => m.asInstanceOf[JMap[String, Any]].asScala.toMap
        case m: Map[_, _]  => m.asInstanceOf[Map[String, Any]]
        case _             => Map.empty[String, Any]
      }

      val filterValue = sourceConf.getOrElse("filter", "-").toString

      // Wariskan value 'env' dari root catalog jika child level tidak mendefinisikannya
      val finalizedSourceConf = Map("env" -> catalogConf.getOrElse("env", "dev")) ++ sourceConf

      val jdbcConfig = loadCredentials(
        catalogConf = finalizedSourceConf,
        sourceName = sourceKey,
        filter = filterValue
      )

      sourceKey -> jdbcConfig
    }
  }
  /** Single Interface untuk Mode READ */
  def loadCredentials(
    catalogConf: Map[String, Any],
    sourceName: String = "-",
    filter: String = "-"
  ): JdbcConfig =
    loadJdbcConfig(catalogConf, isWrite = false, sourceName = sourceName, filter = filter)

  /** Single Interface untuk Mode WRITE */
  def loadWriteCredentials(
    catalogConf: Map[String, Any]
  ): JdbcConfig =
    loadJdbcConfig(catalogConf, isWrite = true, sourceName = "-", filter = "-")
}