package io.saagie.outis.link

import io.saagie.outis.core.SparkProgram
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Datagovernance client parameters.
  *
  * @param datagovUrl   The Datagovernance endpoint to retrieve datasets to anonymize.
  * @param notification The Datagovernance's callback endpoint to notify updates.
  * @param hdfsUser     The hdfs user which will be used by the application to read files.
  * @param thriftServer Hive's metastore thrift server url.
  */
case class Parameters(datagovUrl: String = "", notification: String = "", hdfsUser: String = "", thriftServer: String = "")

/**
  * Example.
  */
object DatagovClient extends App {
  val applicationName = "Datagov anonymizer"

  val parser = new scopt.OptionParser[Parameters](applicationName) {
    head(applicationName, "")

    arg[String]("<datagov url>") required() action ((s, p) => {
      p.copy(datagovUrl = s)
    }) text "The Datagovernance endpoint to retrieve datasets to anonymize."

    arg[String]("<datagov notification url>") required() action ((s, p) => {
      p.copy(notification = s)
    }) text "The Datagovernance's callback endpoint to notify updates."

    opt[String]('u', "<hdfs user>") required() action ((s, p) => {
      p.copy(hdfsUser = s)
    }) text "The hdfs user which will be used by the application to read files."

    opt[String]('t', "<thrift server>") required() action ((s, p) => {
      p.copy(thriftServer = s)
    }) text "Hive's metastore thrift server url."
  }

  /**
    * Starts anonymization spark job.
    *
    * @param config The datagov, hdfs user and thrift server configuration.
    */
  private def startAnonymization(config: Parameters): Unit = {
    val link = DatagovLink(config.datagovUrl, config.notification)
    System.setProperty("HADOOP_USER_NAME", config.hdfsUser)
    System.setProperty("hive.metastore.uris", config.thriftServer)
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(applicationName)
      .config("hive.metastore.warehouse.dir", "hdfs://cluster/user/hive/warehouse")
      .config("spark.eventLog.dir", "hdfs://cluster/tmp/spark-events")
      .config("spark.eventLog.enabled", "true")
      .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
      .enableHiveSupport()
      .getOrCreate()
    SparkProgram(link).launchAnonymisation()
    sparkSession.stop()
  }

  parser.parse(args, Parameters()) match {
    case Some(config) =>
      startAnonymization(config)
    case None =>
  }
}
