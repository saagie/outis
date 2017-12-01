package io.saagie.outis.core

import io.saagie.outis.core.job.AnonymizationJob
import io.saagie.outis.core.model.OutisLink
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class SparkProgram(outisLink: OutisLink)(implicit sparkSession: SparkSession) {
  val log: Logger = Logger.getRootLogger

  /**
    * Start anonymization.
    */
  def launchAnonymisation(): Unit = {
    outisLink.datasetsToAnonimyze() match {
      case Right(datasets) =>
        datasets.foreach(dataset => {
          AnonymizationJob(dataset).anonymize()
          outisLink.notifyDatasetProcessed(dataset)
        })
      case Left(error) => log.error(error)
    }
  }
}
