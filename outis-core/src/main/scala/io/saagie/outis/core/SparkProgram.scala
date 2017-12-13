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
          val result = AnonymizationJob(dataset).anonymize()
          result match {
            case Right(anonymizationResult) =>
              val notification = outisLink.notifyDatasetProcessed(anonymizationResult)
              notification match {
                case Right(answer) => log.info(s"Notification ok: $answer")
                case Left(error) => log.error("Error while notifying results", error)
              }
            case Left(error) => log.error("Error while anonymizing", error)
          }
        })
      case Left(error) => log.error("Error while retrieving datasets", error)
    }
  }
}
