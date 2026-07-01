package etl_pipeline

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import etl_pipeline.utils.{CredentialsLoader, DateResolver, YamlLoader, SchemaValidator}
import java.util.Properties
import java.sql.{Connection, DriverManager, SQLException}

import scala.jdk.CollectionConverters._

/**
 * Main Object
 * Pipeline ETL Spark untuk MySQL → Transformasi → MySQL.
 */
object Main {

  private val log = LoggerFactory.getLogger(getClass)

  // ── 1. Spark Session Creator ───────────────────────────────────────────────
  def createSparkSession(resourceConf: Map[String, Any]): SparkSession = {
    val conf = new SparkConf()
    resourceConf.foreach { case (k, v) => conf.set(k, v.toString) }
    SparkSession.builder().config(conf).getOrCreate()
  }

  // ── 2. MySQL Writer with Auto-Index Logic ──────────────────────────────────
  def writeToMySQL(
    spark: SparkSession,
    df: DataFrame,
    catalogConf: Map[String, Any],
    jdbcConfig: CredentialsLoader.JdbcConfig,
    startDate: Option[String] = None
  ): Unit = {
    val writeConf = catalogConf("write_conf").asInstanceOf[Map[String, Any]]
    val options = writeConf.get("options").map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map.empty[String, Any])
    val schema =
  catalogConf
    .get("write_database")
    .orElse(catalogConf.get("schema"))
    .getOrElse("")
    .toString
    val table = catalogConf("table").toString
    val fullTable = if (schema.nonEmpty) s"`$schema`.`$table`" else s"`$table`"
    val mode = writeConf.getOrElse("mode", "overwrite").toString
    val batchSize = options.getOrElse("batchsize", 1000).toString
    val numParts = options.getOrElse("numPartitions", 5).toString

    val catalogCols: List[Map[String, Any]] = writeConf.get("schema") match {
      case Some(s) => s.asInstanceOf[List[Any]].map(_.asInstanceOf[Map[String, Any]])
      case None => throw new IllegalArgumentException(s"write_conf.schema is required for table $fullTable")
    }

    val catalogColNames = catalogCols.map(_("column_name").toString)
    val catalogHasPartitionDate = catalogColNames.contains("partition_date")

    // ── Injeksi partition_date jika diperlukan ───────────────────────────────
    val dfWithPartition = if (!catalogHasPartitionDate) {
      df
    } else if (df.columns.contains("partition_date")) {
      df
    } else {
      startDate match {
        case Some(sd) =>
          val anchor = java.time.LocalDate.parse(sd, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          val pd = anchor.minusDays(1).format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          log.info(s"Injecting partition_date=$pd")
          df.withColumn("partition_date", lit(pd).cast("date"))
        case None => df
      }
    }

    val dfOrdered = dfWithPartition.select(catalogColNames.map(dfWithPartition(_)): _*)

    // ── Membangun DDL (Table & Primary Key) ──────────────────────────────────
    val colDefs = catalogCols.map { col =>
    val name = col("column_name").toString
    val colType = col("type").toString
    val constraint = col.getOrElse("constraint", "").toString
    val notNull = if (constraint.contains("not_null")) " NOT NULL" else ""

    val default = col.get("default") match {
      case Some(null) | Some("null") =>
        // timestamp butuh NULL sebelum DEFAULT NULL agar tidak ditolak strict mode MySQL
        val nullability = if (colType.toLowerCase.startsWith("timestamp") && notNull.isEmpty) " NULL" else ""
        s"$nullability DEFAULT NULL"
      case Some(v) => s" DEFAULT '${v.toString}'"
      case None    => ""
    }

    s"  `$name` $colType$notNull$default"
  }.mkString(",\n")

    val pkCols = catalogCols
      .filter(_.getOrElse("constraint", "").toString.contains("primary_key"))
      .map(c => s"`${c("column_name")}`")
    val pkClause = if (pkCols.nonEmpty) s",\n  PRIMARY KEY (${pkCols.mkString(", ")})" else ""

    val ddl = s"CREATE TABLE IF NOT EXISTS $fullTable (\n$colDefs$pkClause\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

    // ── Eksekusi via JDBC Dasar (DDL & Index) ────────────────────────────────
    val connProps = new Properties()
    jdbcConfig.props.foreach { case (k, v) => connProps.setProperty(k, v) }
    val driverClass = jdbcConfig.props.getOrElse("driver", "com.mysql.cj.jdbc.Driver")
    log.info(s"Registering JDBC Driver: $driverClass")
    Class.forName(driverClass)
    val conn = DriverManager.getConnection(jdbcConfig.url, connProps)

    try {
      log.info(s"Ensuring table exists:\n$ddl")
      conn.createStatement().executeUpdate(ddl)

      // ── LOGIKA OTOMATISASI INDEX (SINGLE COLUMN) ───────────────────────────
      val indexCols = catalogCols.filter(c => c.getOrElse("index", false).toString.toBoolean)
      indexCols.foreach { colMap =>
        val colName = colMap("column_name").toString
        val indexName = s"idx_${table}_$colName"
        val createIndexSql = s"CREATE INDEX `$indexName` ON $fullTable (`$colName`)"
        
        try {
          log.info(s"Creating index: $indexName")
          conn.createStatement().executeUpdate(createIndexSql)
        } catch {
          case e: SQLException if e.getErrorCode == 1061 => 
            log.debug(s"Index $indexName already exists, skipping.")
          case e: Exception => 
            log.error(s"Failed to create index $indexName: ${e.getMessage}")
        }
      }

      // ── LOGIKA COMPOSITE INDEX ─────────────────────────────────────────────
      val compositeIndices = writeConf.get("composite_indices") match {
        case Some(list) => list.asInstanceOf[List[Map[String, Any]]]
        case None => List.empty[Map[String, Any]]
      }

      compositeIndices.foreach { idxMap =>
        val idxName = idxMap("name").toString
        
        // ── PARSE PREFIX INDEX: "nama_barang(100)" → `nama_barang`(100)
        val cols = idxMap("columns").asInstanceOf[List[String]].map { c =>
          val prefixPattern = """^(\w+)\((\d+)\)$""".r
          c match {
            case prefixPattern(colName, prefixLen) => s"`$colName`($prefixLen)"
            case plain => s"`$plain`"
          }
        }.mkString(", ")
        
        val createCompositeIdxSql = s"CREATE INDEX `$idxName` ON $fullTable ($cols)"
        
        try {
          log.info(s"Creating composite index: $idxName")
          conn.createStatement().executeUpdate(createCompositeIdxSql)
        } catch {
          case e: SQLException if e.getErrorCode == 1061 =>
            log.debug(s"Composite Index $idxName already exists, skipping.")
          case e: Exception =>
            log.error(s"Failed to create composite index $idxName: ${e.getMessage}")
        }
      }

      // ── Manajemen Partisi (Delete/Overwrite Logic) ─────────────────────────
      if (catalogHasPartitionDate && mode == "overwrite") {
        val effectivePartitionDates = dfWithPartition.select("partition_date")
          .distinct().collect().flatMap(r => Option(r.get(0)).map(_.toString)).toList

        effectivePartitionDates.foreach { pd =>
          val deleteSql = s"DELETE FROM $fullTable WHERE `partition_date` = '$pd'"
          log.info(s"Cleaning partition: $pd")
          conn.createStatement().executeUpdate(deleteSql)
        }
      } else if (mode == "overwrite" && !catalogHasPartitionDate) {

      log.info(
        s"""
          |========== TRUNCATE TABLE ==========
          |URL      : ${jdbcConfig.url}
          |Schema   : $schema
          |Table    : $table
          |FullName : $fullTable
          |SQL      : TRUNCATE TABLE $fullTable
          |====================================
          |""".stripMargin
      )

      conn.createStatement().executeUpdate(s"TRUNCATE TABLE $fullTable")
    }
    } finally {
      conn.close()
    }

    // ── Write Data via Spark JDBC ────────────────────────────────────────────
    val writeProps = new Properties()
    (jdbcConfig.props ++ Map("batchsize" -> batchSize, "numPartitions" -> numParts))
      .foreach { case (k, v) => writeProps.setProperty(k, v) }

    log.info(s"Writing data to $fullTable...")
    dfOrdered.write.mode("append").jdbc(jdbcConfig.url, fullTable, writeProps)
  }

  // ── 3. Reflection Loader ───────────────────────────────────────────────────
  def loadTransformer(className: String): Transformer = {
    val clazz = Class.forName(className + "$")
    val moduleField = clazz.getField("MODULE$")
    moduleField.get(null).asInstanceOf[Transformer]
  }

  // ── 4. Pipeline Runner ─────────────────────────────────────────────────────
  def runPipeline(
    spark: SparkSession,
    catalogConf: Map[String, Any],
    jdbcConfig: CredentialsLoader.JdbcConfig,
    startDate: Option[String]
  ): Unit = {
    val jdbcWriteConfig = CredentialsLoader.loadWriteCredentials(catalogConf)
    val readConf = catalogConf.getOrElse("read_conf", Map.empty).asInstanceOf[Map[String, Any]]
    val scriptLocation = catalogConf("script_location").toString.trim
    
    val transformer =
  loadTransformer(scriptLocation)

  val df =
    transformer.processData(
      spark,
      readConf,
      jdbcConfig
    )

  // ── VALIDATE SCHEMA ───────────────────────────────────────────────
  SchemaValidator.validateWriteSchema(
    df,
    catalogConf
  )

  // ── WRITE TO MYSQL ────────────────────────────────────────────────
  writeToMySQL(
    spark,
    df,
    catalogConf,
    jdbcWriteConfig,
    startDate
  )
  }

  // ── 5. Argument Parser ─────────────────────────────────────────────────────
  case class Args(env: String, catalog: String, resource: String, startDate: Option[String])

  def parseArgs(argv: Array[String]): Args = {
    var env = ""; var catalog = ""; var resource = ""; var startDate: Option[String] = None
    val it = argv.iterator
    while (it.hasNext) {
      it.next() match {
        case "--env"        => env = it.next()
        case "--catalog"    => catalog = it.next()
        case "--resource"   => resource = it.next()
        case "--start_date" => startDate = Some(it.next())
        case _ => 
      }
    }
    require(env.nonEmpty && catalog.nonEmpty && resource.nonEmpty, "Missing required arguments")
    Args(env, catalog, resource, startDate)
  }

  // ── 6. Entrypoint (FIXED SIGNATURE) ────────────────────────────────────────
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    
    if (args.isEmpty) {
      log.error("Usage: spark-submit --class ... --env <dev|uat|prod> --catalog <name> --resource <name>")
      System.exit(1)
    }

    val parsedArgs = parseArgs(args)
    log.info(s"Starting Pipeline: env=${parsedArgs.env}, catalog=${parsedArgs.catalog}")

    // Load Configurations
    var catalogConf = YamlLoader.getCatalog(parsedArgs.env, parsedArgs.catalog)
    val resourceConf = YamlLoader.getResource(parsedArgs.env, parsedArgs.resource)

    // Resolve Date Templates
    parsedArgs.startDate.foreach { sd =>
      catalogConf = DateResolver.resolveCatalogDates(catalogConf, sd)
    }

    val jdbcConfig = CredentialsLoader.loadCredentials(catalogConf)
    val spark = createSparkSession(resourceConf)

    try {
      runPipeline(spark, catalogConf, jdbcConfig, parsedArgs.startDate)
      log.info("Pipeline completed successfully.")
    } catch {
      case e: Exception =>
        log.error("Pipeline failed!", e)
        System.exit(1)
    } finally {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000
      log.info(s"Execution time: ${elapsed / 60}m ${elapsed % 60}s")
      spark.sparkContext.stop()
    }
  }
}