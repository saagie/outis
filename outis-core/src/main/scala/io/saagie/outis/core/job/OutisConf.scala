package io.saagie.outis.core.job

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
case class OutisProperty(clazz: String, method: String, parameters: List[Any])

/**
  * Outis' configuration.
  *
  * @param properties List of anonymizers. The first parameter is the name of the anonymizer's type.
  */
case class OutisConf(properties: Map[String, OutisProperty] = OutisConf.defaultConf) {
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

  def defaultConf: Map[String, OutisProperty] = {
    Map(ANONYMIZER_STRING -> OutisConf.defaultStringAnonymizer())
  }

  def defaultStringAnonymizer(): OutisProperty = {
    OutisProperty("io.saagie.outis.core.anonymize.AnonymizeString", "substitute", List(""))
  }
}
