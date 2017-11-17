package io.saagie.outis.core

import io.saagie.job.Anonymize
import io.saagie.outis.core.model.OutisLink
import org.apache.spark.sql.SparkSession
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

case class SparkProgram(outisLink: OutisLink)(implicit sparkSession: SparkSession) {

  def launchAnonymisation(): Unit = {
    outisLink.datasetsToAnonimyze() match {
      case Right(datasets) =>
        datasets.foreach(dataset => {
          Anonymize(dataset).anonymise()
          outisLink.notifyDatasetProcessed(dataset)
        })
      case Left(error) =>
    }
  }
}
