package io.saagie.outis.core

import io.saagie.job.Anonymize
import io.saagie.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser
case class CLIParams(datagovUrl: String = "", hiveMetastoreUri: String = "thrift://nn1:9083", hadoopUserName: String = "hdfs")

object Args {

  /**
    * Parse arguments from command line.
    *
    * @param appName application name.
    * @return parsed arguments.
    */
  def parseArgs(appName: String): OptionParser[CLIParams] = {

    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
      help("help") text "prints this usage text"

      opt[String]("datagovUrl") optional() action { (data, conf) =>
        conf.copy(datagovUrl = data)
      } text "Url of datagovernance."

      opt[String]("hiveMetastoreUri") optional() action { (data, conf) =>
        conf.copy(hiveMetastoreUri = data)
      } text "Uri of Hive Metastore."

      opt[String]("hadoopUserName") optional() action { (data, conf) =>
        conf.copy(hadoopUserName = data)
      } text "The identity of user used for job."

    }
  }

}

case class SparkProgram()(implicit sparkSession: SparkSession) {

  def launchAnonymisation(): Unit = {

    //TODO get datasets to anonymise from datagov (or another source)
    val datasets : List[DataSet] = List()

    datasets.foreach(dataset => {
      Anonymize(dataset).anonymise()
      // TODO call datagov
    })
  }
}

object Main extends App {

  val appName: String = "outis"
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val parser = Args.parseArgs(appName)
   parser.parse(args, CLIParams()) match {
    case Some(p) =>
      System.setProperty("HADOOP_USER_NAME", p.hadoopUserName)
      System.setProperty("HIVE_METASTORE_URIS", p.hiveMetastoreUri)
      val conf = new SparkConf().setAppName(appName)
      implicit val spark: SparkSession = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
      SparkProgram().launchAnonymisation()
      spark.stop()

    case None => logger.debug("invalid program arguments : {}", args)
  }

}
