package io.saagie.outis.link

import io.saagie.model.DataSet

sealed trait OutisLink {
  def getDatasetsToAnonimyze(): List[DataSet]
  def notifyDatasetProcessed(): Unit
}
