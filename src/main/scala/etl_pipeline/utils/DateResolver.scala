package etl_pipeline.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.matching.Regex

/**
 * DateResolver
 *
 * Resolves Triton-style date template syntax found in catalog YAML values:
 *
 *   {~~~%Y-%m-%d:day=1~~~}  →  start_date − 1 day, formatted as yyyy-MM-dd
 *   {~~~%Y-%m-%d:day=0~~~}  →  start_date − 0 days
 *   {~~~%Y-%m-01:day=1~~~}  →  start_date − 1 day, literal "01" day
 *
 * strftime tokens supported: %Y, %m, %d
 */
object DateResolver {

  // Matches {~~~<format>:day=<N>~~~}
  private val TemplateRe: Regex = """\{~~~(.+?):day=(\d+)~~~\}""".r

  /** Convert a Python strftime format string to Java DateTimeFormatter pattern. */
  private def strftimeToJava(fmt: String): String =
    fmt
      .replace("%Y", "yyyy")
      .replace("%m", "MM")
      .replace("%d", "dd")

  /** Replace all {~~~...~~~} templates in a single string value. */
  private def resolveValue(value: String, anchor: LocalDate): String =
    TemplateRe.replaceAllIn(value, m => {
      val fmt = m.group(1)            // e.g. "%Y-%m-%d" or "%Y-%m-01"
      val offset = m.group(2).toInt      // days to subtract
      val resolved = anchor.minusDays(offset.toLong)
      val pattern = DateTimeFormatter.ofPattern(strftimeToJava(fmt))
      resolved.format(pattern)
    })

  /** Recursively walk a catalog Map/List and resolve every string value. */
  def walk(node: Any, anchor: LocalDate): Any = node match {
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].map { case (k, v) => k -> walk(v, anchor) }
    case l: List[_] =>
      l.map(item => walk(item, anchor))
    case s: String =>
      resolveValue(s, anchor)
    case other =>
      other // Int, Boolean, null — unchanged
  }

  /**
   * Walk the entire catalog config and resolve all {~~~...~~~} date templates.
   *
   * @param catalogConf  catalog map loaded from YAML
   * @param startDate  anchor date string, e.g. "2024-01-15"
   * @param dateFmt  format to parse startDate (default: yyyy-MM-dd)
   * @return new map with all templates replaced by concrete date strings
   */
  def resolveCatalogDates(
    catalogConf: Map[String, Any],
    startDate: String,
    dateFmt: String = "yyyy-MM-dd"
  ): Map[String, Any] = {
    val anchor = LocalDate.parse(startDate, DateTimeFormatter.ofPattern(dateFmt))
    walk(catalogConf, anchor).asInstanceOf[Map[String, Any]]
  }
}
