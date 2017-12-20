package io.saagie.outis.core.anonymize

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, LocalDateTime}

import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Random, Success, Try}


object AnonymizeDate {

  val logger: Logger = Logger.getRootLogger

  val from: LocalDate = LocalDate.of(1920, 1, 1)
  val to: LocalDate = LocalDate.now()
  val hours = 23
  val minutes = 59
  val secondes = 59

  private def generateLocalDateTime = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime)
    from.plusDays(random.nextInt(diff.toInt)) atTime(random.nextInt(hours), random.nextInt(minutes), random.nextInt(secondes))
  }


  def randomDate(date: Date, errorAccumulator: LongAccumulator): Date = {
    Try {
      val time: LocalDateTime = generateLocalDateTime
      Date.valueOf(time.toLocalDate)
    } match {
      case Success(d) => d
      case Failure(e) =>
        logger.error(s"Impossible to process column value $date", e)
        errorAccumulator.add(1)
        date
    }
  }

  def randomTimestamp(date: Timestamp, errorAccumulator: LongAccumulator): Timestamp = {
    Try {
      val time: LocalDateTime = generateLocalDateTime
      Timestamp.valueOf(time)
    } match {
      case Success(d) => d
      case Failure(e) =>
        logger.error(s"Impossible to process column value $date", e)
        errorAccumulator.add(1)
        date
    }
  }

  def randomString(date: String, pattern: String, errorAccumulator: LongAccumulator): String = {
    Try {
      val time: LocalDateTime = generateLocalDateTime
      val formatter = DateTimeFormatter.ofPattern(pattern)
      time.format(formatter)
    } match {
      case Success(d) => d
      case Failure(e) =>
        logger.error(s"Impossible to process column value $date", e)
        errorAccumulator.add(1)
        date
    }
  }

}
