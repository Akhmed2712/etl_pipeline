package etl_pipeline

import etl_pipeline.utils.CredentialsLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Transformer
 *
 * Every transform object must extend this trait.
 * Main loads the class named in `script_location` via reflection
 * and calls processData() on it.
 *
 * To create a new job:
 *   1. Create a Scala object that extends Transformer
 *   2. Point `script_location` in catalog.yml to its fully-qualified name
 *   3. No changes to Main needed
 *
 * Example:
 *   object MyNewJob extends Transformer {
 *     def processData(spark: SparkSession, env: Map[String, Any]): DataFrame = { ... }
 *   }
 *
 *   # catalog.yml
 *   script_location: mypipeline.scripts.mynewjob.MyNewJob
 */
trait Transformer {
  def processData(spark: SparkSession, env: Map[String, Any], jdbcConfig: CredentialsLoader.JdbcConfig): DataFrame
}
