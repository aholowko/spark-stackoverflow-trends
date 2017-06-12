package pl.holowko.bigdata.so.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import com.github.nscala_time.time.Imports.{DateTime, Period}
import org.joda.time.YearMonth
import org.joda.time.format.DateTimeFormat

object time {

  val PATTERN = "yyyy-MM-01"
  private val formatter = DateTimeFormatter.ofPattern(PATTERN).withZone(ZoneId.systemDefault())
  private val jodaFormatter = DateTimeFormat.forPattern(PATTERN)
  
  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  
  def dateRange(from: String, to: String, step: Period): Iterator[YearMonth] =
    dateRange(YearMonth.parse(from, jodaFormatter), YearMonth.parse(to, jodaFormatter), step)
  
  def dateRange(from: YearMonth, to: YearMonth, step: Period): Iterator[YearMonth] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def monthStr(date: YearMonth): String = date.toString(jodaFormatter)
  
  def monthStr(date: Instant): String = formatter.format(date)
  
  def monthStr(date: DateTime): String = jodaFormatter.print(date)
  
}
