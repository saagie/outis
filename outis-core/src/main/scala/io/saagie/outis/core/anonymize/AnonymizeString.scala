package io.saagie.outis.core.anonymize

import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator

import scala.collection.immutable
import scala.util.{Failure, Random, Success, Try}

object AnonymizeString {

  val logger: Logger = Logger.getRootLogger

  val letters: immutable.IndexedSeq[Char] = ('a' to 'z') ++ ('A' to 'Z')

  def suppression(): Option[String] = {
    None
  }

  def setTo(value: String, pattern: Char = ' '): String = value.map(_ => pattern)

  def setToBlank(value: String): String = setTo(value)

  def setToX(value: String): String = setTo(value, 'X')

  def truncate(value: String, size: Int): Either[AnonymizationException, String] = {
    if (value.length > size) {
      Right(value.substring(0, size))
    } else {
      Left(AnonymizationException(s"Can't truncate '$value' because size is less or equal to truncation size of $size"))
    }
  }

  def substitute(value: String, errorAccumulator: LongAccumulator): String = {
    Try {
      value.map {
        case c if c.isDigit => Random.nextInt(9)
        case c if c.isLetter => letters(Random.nextInt(letters.size))
        case c => c.toString
      }.mkString
    } match {
      case Success(s) => s
      case Failure(e) =>
        logger.error(s"Impossible to process column value $value", e)
        errorAccumulator.add(1)
        value
    }
  }

}
