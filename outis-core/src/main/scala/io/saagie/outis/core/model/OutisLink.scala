package io.saagie.outis.core.model

import io.saagie.model.DataSet

/**
  * This trait has to be implemented by every connectors.
  */
trait OutisLink {
  def datasetsToAnonimyze(): Either[OutisLinkException, List[DataSet]]

  def notifyDatasetProcessed(dataSet: DataSet): Either[OutisLinkException, String]
}
