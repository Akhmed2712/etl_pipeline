package etl_pipeline.utils

import java.io.{File, FileInputStream}
import java.util.{Map => JMap}

import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._

/**
 * CredentialsLoader
 *
 * Loads JDBC credentials from credentials/credentials.yml using the
 * `credentials_key` field defined in the catalog.
 *
 * Returns (jdbcUrl, jdbcProps) ready for spark.read.jdbc() / df.write.jdbc().
 */
object CredentialsLoader {

  val CredentialsPath: String =
    new File(System.getProperty("user.dir"), "credentials/credentials.yml").getAbsolutePath

  case class JdbcConfig(url: String, props: Map[String, String])

  /**
   * Load credentials for the given catalog config.
   *
   * @param catalogConf fully-resolved catalog map (must have "credentials_key" and "database")
   * @return dbcConfig(url, props)
   */
  def loadCredentials(catalogConf: Map[String, Any]): JdbcConfig = {
    val credFile = new File(CredentialsPath)
    require(
      credFile.exists(),
      s"Credentials file not found: $CredentialsPath\n" +
        "Copy credentials/credentials_example.yml → credentials/credentials.yml and fill in values."
    )

    val yaml = new Yaml()
    val allCreds: Map[String, Any] = {
      val raw = yaml.load[JMap[String, Any]](new FileInputStream(credFile))
      raw.asScala.toMap
    }

    val credKey = catalogConf.getOrElse("credentials_key", "").toString
    require(credKey.nonEmpty, "'credentials_key' is missing from the catalog config.")
    require(
      allCreds.contains(credKey),
      s"credentials_key='$credKey' not found in credentials.yml. " +
        s"Available keys: ${allCreds.keys.mkString(", ")}"
    )

    val creds: Map[String, Any] =
      allCreds(credKey).asInstanceOf[JMap[String, Any]].asScala.toMap

    val host = creds("host").toString
    val port = creds.getOrElse("port", 3306).toString
    val database = catalogConf.getOrElse("database", "").toString
    val driver = creds.getOrElse("driver", "com.mysql.cj.jdbc.Driver").toString

    val jdbcUrl =
      s"jdbc:mysql://$host:$port/$database?useSSL=false&serverTimezone=Asia/Jakarta"

    val jdbcProps = Map(
      "user" -> creds("user").toString,
      "password" -> creds("password").toString,
      "driver" -> driver
    )

    JdbcConfig(jdbcUrl, jdbcProps)
  }
}