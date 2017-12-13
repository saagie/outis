package io.saagie.outis.core.job

import io.saagie.model.DataSet

case class AnonymizationResult(dataset: DataSet, anonymizedRows: Long, duration: Long)
