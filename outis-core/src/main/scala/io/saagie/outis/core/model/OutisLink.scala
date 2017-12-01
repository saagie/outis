package io.saagie.outis.core.model

import io.saagie.model.DataSet

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
    * @param dataSet The processed dataset.
    * @return
    */
  def notifyDatasetProcessed(dataSet: DataSet): Either[OutisLinkException, String]
}
