package io.saagie.outis.core.job

case object ColumnValue

case class OutisProperty(clazz: String, method: String, parameters: List[Any])

case class OutisConf(properties: Map[String, OutisProperty] = OutisConf.defaultConf) {
  def getClassFor(anonymizer: String): String = {
    properties(anonymizer).clazz
  }

  def getMethodFor(anonymizer: String): String = {
    properties(anonymizer).method
  }

  def getParameterClassesFor(anonymizer: String): List[Class[_]] = {
    properties(anonymizer).parameters.map(_.getClass)
  }

  def getParameters(anonimyzer: String): List[Any] = {
    properties(anonimyzer).parameters
  }
}

object OutisConf {
  val ANONYMIZER_STRING = "anonymizer.string"
  val ANONYMIZER_STRING_CLASS = "anonymizer.string.class"
  val ANONYMIZER_STRING_METHOD = "anonymizer.string.method"
  val ANONYMIZER_STRING_PARAMETERS = "anonymizer.string.parameters"

  def defaultConf: Map[String, OutisProperty] = {
    Map(ANONYMIZER_STRING -> OutisConf.defaultStringAnonymizer())
  }

  def defaultStringAnonymizer(): OutisProperty = {
    OutisProperty("io.saagie.outis.core.anonymize", "suppression", Nil)
  }
}
