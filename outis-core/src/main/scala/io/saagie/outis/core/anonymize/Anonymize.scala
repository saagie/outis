package io.saagie.outis.core.anonymize

import scala.util.Random

object Anonymize {

  val letters = ('a' to 'z') ++ ('A' to 'Z')
  val random = Random

  def suppression(): Option[String] = {
      None
  }

  def setTo(value: String, pattern: Char = ' '): String = value.map(_ => pattern)

  def setToBlank(value: String): String = setTo(value)

  def setToX(value: String): String =  setTo(value, 'X')

  def truncate(value: String, size: Int): Either[AnonymizationException, String] = {
    if (value.length > size) {
      Right(value.substring(0, size))
    } else {
      Left(AnonymizationException(s"Can't truncate '$value' because size is less or equal to truncation size of $size"))
    }
  }

  def substitute(value: String): String = {
    value.map {
      case c if c.isDigit => random.nextInt(9)
      case c if c.isLetter => letters(random.nextInt(letters.size))
      case c => c.toString
    }.mkString
  }

}
