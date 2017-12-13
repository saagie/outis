package io.saagie.outis.core.model

import io.saagie.model.DataSet
import io.saagie.outis.core.job.AnonymizationResult

/**
  * This trait has to be implemented by every connectors.
  */
trait OutisLink {
  /**
    * This method returns the list of datasets to anonymize.
    *
    * @return
    */
  def datasetsToAnonimyze(): Either[OutisLinkException, List[DataSet]]

  /**
    * The method which will be called when a dataset has been processed.
    *
    * @param anonymizationResult The processed result.
    * @return
    */
  def notifyDatasetProcessed(anonymizationResult: AnonymizationResult): Either[OutisLinkException, String]
}
