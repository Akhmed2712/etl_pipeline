package etl_pipeline.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.matching.Regex
import scala.jdk.CollectionConverters._

/**
 * DateResolver
 *
 * Supported syntax:
 *
 *   {~~~%Y-%m-%d:day=1~~~}
 *   {~~~%Y-%m-%d:month=1~~~}
 *   {~~~%Y-%m-%d:year=1~~~}
 *
 * Example:
 *   anchor = 2024-05-21
 *
 *   {~~~%Y-%m-%d:day=1~~~}   -> 2024-05-20
 *   {~~~%Y-%m-%d:month=1~~~} -> 2024-04-21
 *   {~~~%Y-%m-%d:year=1~~~}  -> 2023-05-21
 *
 * Supported strftime tokens:
 *   %Y %m %d
 */
object DateResolver {

  // Matches:
  // {~~~<format>:day=1~~~}
  // {~~~<format>:month=1~~~}
  // {~~~<format>:year=1~~~}
  private val TemplateRe: Regex =
    """\{~~~(.+?):(day|month|year)=(-?\d+)~~~\}""".r

  /** Convert Python strftime to Java formatter pattern */
  private def strftimeToJava(fmt: String): String =
    fmt
      .replace("%Y", "yyyy")
      .replace("%m", "MM")
      .replace("%d", "dd")

  /** Apply date offset based on unit */
  private def applyOffset(
    date: LocalDate,
    unit: String,
    offset: Int
  ): LocalDate = {
    unit match {
      case "day" =>
        date.minusDays(offset.toLong)

      case "month" =>
        date.minusMonths(offset.toLong)

      case "year" =>
        date.minusYears(offset.toLong)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported unit: $unit")
    }
  }

  /** Resolve all templates in a string */
  private def resolveValue(value: String, anchor: LocalDate): String =
    TemplateRe.replaceAllIn(value, m => {
      val fmt = m.group(1)
      val unit = m.group(2)
      val offset = m.group(3).toInt

      val resolved = applyOffset(anchor, unit, offset)

      val pattern = DateTimeFormatter.ofPattern(strftimeToJava(fmt))

      resolved.format(pattern)
    })

  /** Recursively resolve templates inside catalog object */
  def walk(node: Any, anchor: LocalDate): Any = node match {

    case m: java.util.Map[_, _] =>
      m.asInstanceOf[java.util.Map[String, Any]]
        .asScala
        .map { case (k, v) => k -> walk(v, anchor) }
        .toMap

    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]]
        .map { case (k, v) => k -> walk(v, anchor) }

    case l: java.util.List[_] =>
      l.asScala.map(item => walk(item, anchor)).toList

    case l: List[_] =>
      l.map(item => walk(item, anchor))

    case s: String =>
      resolveValue(s, anchor)

    case other =>
      other
  }

  /**
   * Resolve all date templates in catalog config
   */
  def resolveCatalogDates(
    catalogConf: Map[String, Any],
    startDate: String,
    dateFmt: String = "yyyy-MM-dd"
  ): Map[String, Any] = {

    val anchor =
      LocalDate.parse(startDate, DateTimeFormatter.ofPattern(dateFmt))

    walk(catalogConf, anchor)
      .asInstanceOf[Map[String, Any]]
  }
}