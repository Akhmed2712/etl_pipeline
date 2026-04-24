package etl_pipeline.utils

import java.io.{File, FileInputStream}
import java.util.{Map => JMap}

import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._

/** Utility for loading typed keys out of YAML catalog / resource files. */
object YamlLoader {

  /**
   * Load a named top-level key from all YAML files under catalogs/<env>/
   * whose filename contains `configType`.
   *
   * @param env environment folder name (e.g. "dev")
   * @param configType filename fragment to match (e.g. "catalog", "resource")
   * @param key top-level key to retrieve
   * @return the value as Map[String, Any]
   */
  def loadYamlKey(env: String, configType: String, key: String): Map[String, Any] = {
    val currentDir = System.getProperty("user.dir")
    val folder = new File(currentDir, s"catalogs/$env")
    val yaml = new Yaml()

    folder.listFiles()
      .filter(f => f.getName.contains(configType) && f.getName.endsWith(".yml"))
      .flatMap { file =>
        val raw = yaml.load[JMap[String, Any]](new FileInputStream(file))
        if (raw != null && raw.containsKey(key))
          Some(raw.get(key).asInstanceOf[JMap[String, Any]].asScala.toMap)
        else
          None
      }
      .headOption
      .getOrElse(throw new NoSuchElementException(
        s"Key '$key' not found in catalogs/$env/*$configType*.yml"
      ))
  }

  def getCatalog(env: String, name: String): Map[String, Any] =
    loadYamlKey(env, "catalog", name)

  def getResource(env: String, name: String): Map[String, Any] =
    loadYamlKey(env, "resource", name)
}
