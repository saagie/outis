package io.saagie.outis.core.job

import java.sql.{Date, Timestamp}

import scala.reflect.ClassTag

/**
  * Singleton value which represents the column value for parameters.
  */
case object ColumnValue

/**
  * Outis property for anonymizer selection.
  *
  * @param clazz      The class to load for anonymization.
  * @param method     The method to call for anonymization.
  * @param parameters The parameter list for the method to call.
  */
case class OutisProperty[+T](clazz: String, method: String, parameters: List[Any])(implicit m: ClassTag[T]) {
  def tpe: Class[_] = m.runtimeClass
}

/**
  * Outis' configuration.
  *
  * @param properties List of anonymizers. The first parameter is the name of the anonymizer's type.
  */
case class OutisConf(properties: Map[String, OutisProperty[Any]] = OutisConf.defaultConf) {
  /**
    * Utilitarian function to retrieve class for given anonymizer.
    *
    * @param anonymizer The anonymizer identifier.
    * @return The class.
    */
  def getClassFor(anonymizer: String): String = {
    properties(anonymizer).clazz
  }

  /**
    * Utilitarian function to retrieve class for given anonymizer.
    *
    * @param anonymizer The anonymizer identifier.
    * @return The class.
    */
  def getMethodFor(anonymizer: String): String = {
    properties(anonymizer).method
  }

  /**
    * Utilitarian function to retrieve parameters' classes for given anonymizer.
    *
    * @param anonymizer The anonymizer identifier.
    * @return The list of parameters classes.
    */
  def getParameterClassesFor(anonymizer: String): List[Class[_]] = {
    properties(anonymizer).parameters.map(_.getClass)
  }

  /**
    * Utilitarian function to retrieve parameters for given anonymizer.
    *
    * @param anonymizer The anonymizer identifier.
    * @return The parameters' list.
    */
  def getParameters(anonymizer: String): List[Any] = {
    properties(anonymizer).parameters
  }
}

/**
  * Constants for anonymizers configuration.
  */
object OutisConf {
  val ANONYMIZER_STRING = "anonymizer.string"
  val ANONYMIZER_STRING_CLASS = "anonymizer.string.class"
  val ANONYMIZER_STRING_METHOD = "anonymizer.string.method"
  val ANONYMIZER_STRING_PARAMETERS = "anonymizer.string.parameters"

  val ANONYMIZER_BYTE = "anonymizer.byte"
  val ANONYMIZER_BYTE_CLASS = "anonymizer.byte.class"
  val ANONYMIZER_BYTE_METHOD = "anonymizer.byte.method"
  val ANONYMIZER_BYTE_PARAMETERS = "anonymizer.byte.parameters"

  val ANONYMIZER_SHORT = "anonymizer.short"
  val ANONYMIZER_SHORT_CLASS = "anonymizer.short.class"
  val ANONYMIZER_SHORT_METHOD = "anonymizer.short.method"
  val ANONYMIZER_SHORT_PARAMETERS = "anonymizer.short.parameters"

  val ANONYMIZER_INT = "anonymizer.int"
  val ANONYMIZER_INT_CLASS = "anonymizer.int.class"
  val ANONYMIZER_INT_METHOD = "anonymizer.int.method"
  val ANONYMIZER_INT_PARAMETERS = "anonymizer.int.parameters"

  val ANONYMIZER_LONG = "anonymizer.long"
  val ANONYMIZER_LONG_CLASS = "anonymizer.long.class"
  val ANONYMIZER_LONG_METHOD = "anonymizer.long.method"
  val ANONYMIZER_LONG_PARAMETERS = "anonymizer.long.parameters"

  val ANONYMIZER_FLOAT = "anonymizer.float"
  val ANONYMIZER_FLOAT_CLASS = "anonymizer.float.class"
  val ANONYMIZER_FLOAT_METHOD = "anonymizer.float.method"
  val ANONYMIZER_FLOAT_PARAMETERS = "anonymizer.float.parameters"

  val ANONYMIZER_DOUBLE = "anonymizer.double"
  val ANONYMIZER_DOUBLE_CLASS = "anonymizer.double.class"
  val ANONYMIZER_DOUBLE_METHOD = "anonymizer.double.method"
  val ANONYMIZER_DOUBLE_PARAMETERS = "anonymizer.double.parameters"

  val ANONYMIZER_BIGDECIMAL = "anonymizer.bigdecimal"
  val ANONYMIZER_BIGDECIMAL_CLASS = "anonymizer.bigdecimal.class"
  val ANONYMIZER_BIGDECIMAL_METHOD = "anonymizer.bigdecimal.method"
  val ANONYMIZER_BIGDECIMAL_PARAMETERS = "anonymizer.bigdecimal.parameters"

  val ANONYMIZER_DATE = "anonymizer.date"
  val ANONYMIZER_DATE_CLASS = "anonymizer.date.class"
  val ANONYMIZER_DATE_METHOD = "anonymizer.date.method"
  val ANONYMIZER_DATE_PARAMETERS = "anonymizer.date.parameters"

  val ANONYMIZER_TIMESTAMP = "anonymizer.timestamp"
  val ANONYMIZER_TIMESTAMP_CLASS = "anonymizer.timestamp.class"
  val ANONYMIZER_TIMESTAMP_METHOD = "anonymizer.timestamp.method"
  val ANONYMIZER_TIMESTAMP_PARAMETERS = "anonymizer.timestamp.parameters"

  val ANONYMIZER_DATE_STRING = "anonymizer.date.string"
  val ANONYMIZER_DATE_STRING_CLASS = "anonymizer.date.string.class"
  val ANONYMIZER_DATE_STRING_METHOD = "anonymizer.date.string.method"
  val ANONYMIZER_DATE_STRING_PARAMETERS = "anonymizer.date.string.parameters"

  def defaultConf: Map[String, OutisProperty[Any]] = {
    Map(ANONYMIZER_STRING -> OutisConf.defaultStringAnonymizer(),
      ANONYMIZER_BYTE -> OutisConf.defaultByteAnonymizer(),
      ANONYMIZER_SHORT -> OutisConf.defaultShortAnonymizer(),
      ANONYMIZER_INT -> OutisConf.defaultIntAnonymizer(),
      ANONYMIZER_LONG -> OutisConf.defaultLongAnonymizer(),
      ANONYMIZER_FLOAT -> OutisConf.defaultFloatAnonymizer(),
      ANONYMIZER_DOUBLE -> OutisConf.defaultDoubleAnonymizer(),
      ANONYMIZER_BIGDECIMAL -> OutisConf.defaultBigDecimalAnonymizer(),
      ANONYMIZER_DATE -> OutisConf.defaultDateAnonymizer(),
      ANONYMIZER_TIMESTAMP -> OutisConf.defaultTimestampAnonymizer(),
      ANONYMIZER_DATE_STRING -> OutisConf.defaultDateStringAnonymizer()
    )
  }

  def defaultStringAnonymizer(): OutisProperty[String] = {
    OutisProperty[String]("io.saagie.outis.core.anonymize.AnonymizeString", "substitute", List(ColumnValue))
  }

  def defaultByteAnonymizer(): OutisProperty[Byte] = {
    OutisProperty[Byte]("io.saagie.outis.core.anonymize.AnonymizeNumeric", "substituteByte", List(ColumnValue))
  }

  def defaultShortAnonymizer(): OutisProperty[Short] = {
    OutisProperty[Short]("io.saagie.outis.core.anonymize.AnonymizeNumeric", "substituteShort", List(ColumnValue))
  }

  def defaultIntAnonymizer(): OutisProperty[Int] = {
    OutisProperty[Int]("io.saagie.outis.core.anonymize.AnonymizeNumeric", "substituteInt", List(ColumnValue))
  }

  def defaultLongAnonymizer(): OutisProperty[Long] = {
    OutisProperty[Long]("io.saagie.outis.core.anonymize.AnonymizeNumeric", "substituteLong", List(ColumnValue))
  }

  def defaultFloatAnonymizer(): OutisProperty[Float] = {
    OutisProperty[Float]("io.saagie.outis.core.anonymize.AnonymizeNumeric", "substituteFloat", List(ColumnValue))
  }

  def defaultDoubleAnonymizer(): OutisProperty[Double] = {
    OutisProperty[Double]("io.saagie.outis.core.anonymize.AnonymizeNumeric", "substituteDouble", List(ColumnValue))
  }

  def defaultBigDecimalAnonymizer(): OutisProperty[BigDecimal] = {
    OutisProperty[BigDecimal]("io.saagie.outis.core.anonymize.AnonymizeNumeric", "substituteBigDecimal", List(ColumnValue))
  }

  def defaultDateAnonymizer(): OutisProperty[Date] = {
    OutisProperty[Date]("io.saagie.outis.core.anonymize.AnonymizeDate", "randomDate", List(ColumnValue))
  }

  def defaultTimestampAnonymizer(): OutisProperty[Timestamp] = {
    OutisProperty[Timestamp]("io.saagie.outis.core.anonymize.AnonymizeDate", "randomTimestamp", List(ColumnValue))
  }

  def defaultDateStringAnonymizer(): OutisProperty[String] = {
    OutisProperty[String]("io.saagie.outis.core.anonymize.AnonymizeDate", "randomString", List(ColumnValue))
  }
}
