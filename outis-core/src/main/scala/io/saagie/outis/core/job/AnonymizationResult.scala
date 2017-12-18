package io.saagie.outis.core.job

import io.saagie.outis.core.model.DataSet

case class AnonymizationResult(dataset: DataSet, anonymizedRows: Long, duration: Long, rowsInError: Long)
