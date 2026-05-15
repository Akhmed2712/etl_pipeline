package etl_pipeline.utils

import java.io.{File, FileInputStream}
import java.util.{Map => JMap}
import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._

object CredentialsLoader {

  // Path absolut ke file credentials (tetap di root/credentials)
  val CredentialsPath: String =
    new File(System.getProperty("user.dir"), "credentials/credentials.yml").getAbsolutePath

  case class JdbcConfig(url: String, props: Map[String, String])

  /**
   * Mendapatkan path resource.yml berdasarkan field 'env' di catalog
   */
  private def getResourcePath(catalogConf: Map[String, Any]): String = {
    val env = catalogConf.getOrElse("env", "dev").toString
    new File(System.getProperty("user.dir"), s"catalogs/$env/resource.yml").getAbsolutePath
  }

  /**
   * Helper untuk membaca file YAML menjadi Map Scala
   */
  private def loadYaml(path: String): Map[String, Any] = {
    val file = new File(path)
    if (!file.exists()) {
      println(s"WARNING: File not found at $path")
      return Map.empty
    }
    val fis = new FileInputStream(file)
    try {
      val yaml = new Yaml()
      val raw = yaml.load[JMap[String, Any]](fis)
      if (raw == null) Map.empty else raw.asScala.toMap
    } finally {
      fis.close()
    }
  }

  /**
   * Logika utama untuk loading JDBC Config (Read & Write)
   */
  def loadJdbcConfig(catalogConf: Map[String, Any], isWrite: Boolean = false): JdbcConfig = {
    // 1. Identifikasi Key Kredensial
    val credKey = if (isWrite) {
      catalogConf.getOrElse("write_credentials_key", catalogConf.getOrElse("credentials_key", "")).toString
    } else {
      catalogConf.getOrElse("credentials_key", "").toString
    }

    // 2. Load dari credentials.yml (Host, Auth)
    val allCreds = loadYaml(CredentialsPath)
    require(allCreds.contains(credKey), s"Key '$credKey' tidak ditemukan di $CredentialsPath")
    val creds = allCreds(credKey).asInstanceOf[JMap[String, Any]].asScala.toMap

    // 3. Load dari resource.yml (Partitioning & Tuning)
    val resourcePath = getResourcePath(catalogConf)
    val allResources = loadYaml(resourcePath)
    
    // Ambil blok berdasarkan nama table (misal: fg_fact_rekap_penggunaan_barang_non_paket)
    val tableName = catalogConf.getOrElse("table", "").toString
    val resourceConfig = allResources.get(tableName) match {
      case Some(m: JMap[_, _]) => m.asInstanceOf[JMap[String, Any]].asScala.toMap
      case _ => Map.empty[String, Any]
    }

    // 4. Bangun JDBC URL
    val host = creds("host").toString
    val port = creds.getOrElse("port", 3306).toString
    val database = if (isWrite) {
      catalogConf.getOrElse("write_database", catalogConf.getOrElse("database", "")).toString
    } else {
      catalogConf.getOrElse("database", "").toString
    }
    val driver = creds.getOrElse("driver", "com.mysql.cj.jdbc.Driver").toString

    val jdbcUrl = s"jdbc:mysql://$host:$port/$database?useSSL=false&serverTimezone=Asia/Jakarta"

    // 5. Gabungkan Properti (Base + Tuning)
    val baseProps = Map(
      "user"     -> creds("user").toString,
      "password" -> creds("password").toString,
      "driver"   -> driver
    )

    // Parameter yang dicari di resource.yml untuk diinject ke JDBC Props
    val tuningKeys = Seq("partitionColumn", "lowerBound", "upperBound", "numPartitions", "batchsize")
    
    val tuningProps = tuningKeys.flatMap { key =>
      // Urutan Prioritas: resource.yml > catalogConf > credentials.yml
      val value = resourceConfig.get(key)
        .orElse(catalogConf.get(key))
        .orElse(creds.get(key))
      
      value.map(v => key -> v.toString)
    }.toMap

    println(s"DEBUG [${if(isWrite) "WRITE" else "READ"}]: Table=$tableName | EnvPath=$resourcePath")
    println(s"DEBUG Props: $tuningProps")

    JdbcConfig(jdbcUrl, baseProps ++ tuningProps)
  }

  // Integrasi dengan fungsi lama
  def loadCredentials(catalogConf: Map[String, Any]): JdbcConfig = loadJdbcConfig(catalogConf, isWrite = false)
  def loadWriteCredentials(catalogConf: Map[String, Any]): JdbcConfig = loadJdbcConfig(catalogConf, isWrite = true)
}