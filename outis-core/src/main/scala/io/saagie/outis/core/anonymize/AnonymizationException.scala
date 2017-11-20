package io.saagie.outis.core.anonymize

case class AnonymizationException(message: String, throwable: Throwable) extends Exception(message, throwable)

object AnonymizationException {
  def apply(message: String): AnonymizationException = new AnonymizationException(message, null)
}
