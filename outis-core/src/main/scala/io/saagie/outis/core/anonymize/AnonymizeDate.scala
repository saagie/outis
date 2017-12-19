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

  def randomDate(): Date = {
    val time: LocalDateTime = generateLocalDateTime
    Date.valueOf(time.toLocalDate)
  }

  def randomTimestamp(): Timestamp = {
    val time: LocalDateTime = generateLocalDateTime
    Timestamp.valueOf(time)
  }

  def randomString(pattern: String): Option[String] = {
    val time: LocalDateTime = generateLocalDateTime
    val formatter = DateTimeFormatter.ofPattern(pattern)
    Try(time.format(formatter)).toOption
  }

}
