package io.saagie

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

object Main extends App {

  val appName: String = "outis"
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val parser = Args.parseArgs(appName)
   parser.parse(args, CLIParams()) match {
    case Some(p) =>
      System.setProperty("HADOOP_USER_NAME", p.hadoopUserName)
      System.setProperty("HIVE_METASTORE_URIS", p.hiveMetastoreUri)
      val conf = new SparkConf().setAppName(appName)
      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    //TODO
    // 1) get datasets from datagov api or other (to define) ...
    // 2) for each dataset:
    //   - read data from hdfs repo/hive
    //   - anonymise each column to anonymise
    //   - write data in a temp repo/table
    //   - if every thing is OK : delete old repository / table, rename temp repo/table and prevent datagov
    //   - otherwhise delete temp repo and quit ?
      spark.stop()

    case None => logger.debug("invalid program arguments : {}", args)
  }

}
