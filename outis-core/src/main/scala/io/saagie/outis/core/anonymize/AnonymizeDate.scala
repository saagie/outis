package io.saagie.outis.core.anonymize

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, LocalDateTime}

import scala.util.{Random, Try}


object AnonymizeDate {

  val from: LocalDate =  LocalDate.of(1920, 1, 1)
  val to: LocalDate = LocalDate.now()
  val hours = 23
  val minutes = 59
  val secondes = 59

  private def generateLocalDateTime = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime)
    from.plusDays(random.nextInt(diff.toInt)) atTime(random.nextInt(hours), random.nextInt(minutes), random.nextInt(secondes))
  }

  def isEvenOption(n: Integer): Option[Boolean] = {
    val num = Option(n).getOrElse(return None)
    Some(num % 2 == 0)
  }

  def randomDate(date: Date): Option[Date] = {
    Option(date).getOrElse(return None)
    val time: LocalDateTime = generateLocalDateTime
    Some(Date.valueOf(time.toLocalDate))

  }

  def randomTimestamp(date: Timestamp): Option[Timestamp] = {
    Option(date).getOrElse(return None)
    val time: LocalDateTime = generateLocalDateTime
    Some(Timestamp.valueOf(time))
  }

  def randomString(date: String, pattern: String): Option[String] = {
    Option(date).getOrElse(return None)
    val time: LocalDateTime = generateLocalDateTime
    val formatter = DateTimeFormatter.ofPattern(pattern)
    Try(time.format(formatter)).toOption
  }

}
