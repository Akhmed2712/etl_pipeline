package etl_pipeline

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import etl_pipeline.utils.{CredentialsLoader, DateResolver, YamlLoader}

import scala.jdk.CollectionConverters._

/**
 * Main
 *
 * MySQL → transform → MySQL pipeline runner.
 * Scala equivalent of main.py.
 *
 * Usage:
 *   spark-submit \
 *     --class mypipeline.Main \
 *     my_pipeline-assembly-1.0.0.jar \
 *     --env dev \
 *     --catalog my_orders_job \
 *     --resource my_orders_job \
 *     [--start_date 2024-01-15]
 */
object Main {

  private val log = LoggerFactory.getLogger(getClass)

  // ── Config helpers ──────────────────────────────────────────────────────────

  def createSparkSession(resourceConf: Map[String, Any]): SparkSession = {
    val conf = new SparkConf()
    resourceConf.foreach { case (k, v) => conf.set(k, v.toString) }
    SparkSession.builder().config(conf).getOrCreate()
  }

  // ── MySQL writer ─────────────────────────────────────────────────────────────

  def writeToMySQL(
    spark: SparkSession,
    df: DataFrame,
    catalogConf: Map[String, Any],
    jdbcConfig: CredentialsLoader.JdbcConfig
  ): Unit = {
    val writeConf = catalogConf("write_conf").asInstanceOf[java.util.Map[String, Any]].asScala
    val options = Option(writeConf.get("options"))
      .map(_.asInstanceOf[java.util.Map[String, Any]].asScala)
      .getOrElse(scala.collection.mutable.Map.empty)

    val schema = catalogConf.getOrElse("schema", "").toString
    val table = catalogConf("table").toString
    val fullTable = if (schema.nonEmpty) s"$schema.$table" else table
    val mode = writeConf.getOrElse("mode", "overwrite").toString
    val batchSize = options.getOrElse("batchsize", 1000).toString
    val numParts = options.getOrElse("numPartitions", 5).toString

    val props = jdbcConfig.props ++ Map("batchsize" -> batchSize, "numPartitions" -> numParts)

    log.info(s"Writing to MySQL table: $fullTable | mode: $mode")
    df.write.jdbc(url = jdbcConfig.url, table = fullTable, connectionProperties = {
      val p = new java.util.Properties()
      props.foreach { case (k, v) => p.setProperty(k, v) }
      p
    })
    log.info(s"Write complete → $fullTable")
  }

  // ── Script loader (reflection) ───────────────────────────────────────────────

  /**
   * Load a Transformer by its fully-qualified Scala object name.
   *
   * How it works:
   *   - Scala compiles `object Foo` into a JVM class called `Foo$` with a
   *     MODULE$ field that holds the singleton instance.
   *   - We append `$` to the class name, grab MODULE$, and cast to Transformer.
   *
   * @param className  fully-qualified object name, e.g. "mypipeline.scripts.myjob.Transform"
   * @return the Transformer singleton
   */
  def loadTransformer(className: String): Transformer = {
    try {
      val clazz = Class.forName(className + "$")          // Scala object → Foo$
      val moduleField = clazz.getField("MODULE$")            // the singleton field
      moduleField.get(null).asInstanceOf[Transformer]
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalArgumentException(
          s"Transform class not found: '$className'\n" +
          s"Check script_location in catalog.yml — must be a fully-qualified Scala object name, e.g.:\n" +
          s"  script_location: mypipeline.scripts.myjob.Transform", e
        )
      case e: ClassCastException =>
        throw new IllegalArgumentException(
          s"Class '$className' does not extend mypipeline.Transformer.\n" +
          s"Add `extends Transformer` to the object definition.", e
        )
    }
  }

  // ── Pipeline ─────────────────────────────────────────────────────────────────

  def runPipeline(
    spark: SparkSession,
    catalogConf: Map[String, Any],
    jdbcConfig: CredentialsLoader.JdbcConfig
  ): Unit = {
    val readConf = catalogConf.getOrElse("read_conf", Map.empty[String, Any])
      .asInstanceOf[java.util.Map[String, Any]].asScala.toMap

    val scriptLocation = catalogConf.getOrElse("script_location", "")
      .toString.trim
    require(scriptLocation.nonEmpty, "'script_location' is missing from the catalog config.")

    log.info(s"Loading transformer: $scriptLocation")
    val transformer = loadTransformer(scriptLocation)

    log.info("Running transform...")
    val df = transformer.processData(spark, readConf, jdbcConfig)
    writeToMySQL(spark, df, catalogConf, jdbcConfig)
  }

  // ── CLI ───────────────────────────────────────────────────────────────────────

  case class Args(
    env: String,
    catalog: String,
    resource: String,
    startDate: Option[String]
  )

  def parseArgs(argv: Array[String]): Args = {
    var env: String = ""
    var catalog: String = ""
    var resource: String  = ""
    var startDate: Option[String] = None

    val it = argv.iterator
    while (it.hasNext) {
      it.next() match {
        case "--env" => env = it.next()
        case "--catalog" => catalog = it.next()
        case "--resource" => resource = it.next()
        case "--start_date" => startDate = Some(it.next())
        case other => log.warn(s"Unknown argument: $other")
      }
    }

    require(env.nonEmpty, "--env is required (dev | uat | prod)")
    require(catalog.nonEmpty, "--catalog is required")
    require(resource.nonEmpty,"--resource is required")
    require(
      Set("dev", "uat", "prod").contains(env),
      s"--env must be one of dev, uat, prod (got: $env)"
    )

    Args(env, catalog, resource, startDate)
  }

  // ── Entrypoint ────────────────────────────────────────────────────────────────

  def main(argv: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val args = parseArgs(argv)

    log.info(
      s"Starting pipeline | env=${args.env} catalog=${args.catalog} " +
        s"resource=${args.resource} start_date=${args.startDate.getOrElse("none")}"
    )

    // Load configs
    var catalogConf = YamlLoader.getCatalog(args.env, args.catalog)
    val resourceConf = YamlLoader.getResource(args.env, args.resource)

    // Resolve all {~~~%Y-%m-%d:day=N~~~} templates in the catalog
    args.startDate.foreach { sd =>
      catalogConf = DateResolver.resolveCatalogDates(catalogConf, sd)
      log.info(s"Date templates resolved using start_date=$sd")
    }

    // MySQL JDBC connection — loaded from credentials/credentials.yml
    val jdbcConfig = CredentialsLoader.loadCredentials(catalogConf)
    log.info(
      s"Credentials loaded for key='${catalogConf("credentials_key")}' → " +
        jdbcConfig.url.split("\\?").head
    )

    // Start Spark
    val spark = createSparkSession(resourceConf)
    log.info(s"Spark started | app=${spark.sparkContext.applicationId}")

    try {
      runPipeline(spark, catalogConf, jdbcConfig)
    } catch {
      case e: Exception =>
        log.error("Pipeline failed.", e)
        System.exit(1)
    } finally {
      val elapsed = (System.currentTimeMillis() - start) / 1000
      log.info(s"Done in ${elapsed / 60}m ${elapsed % 60}s")
      spark.sparkContext.stop()
    }
  }
}
