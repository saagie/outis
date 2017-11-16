package io.saagie.outis.core.model

import io.saagie.model.DataSet

/**
  * This trait has to be implemented by every connectors.
  */
trait OutisLink {
  def datasetsToAnonimyze(): List[DataSet]

  def notifyDatasetProcessed(): Unit
}
