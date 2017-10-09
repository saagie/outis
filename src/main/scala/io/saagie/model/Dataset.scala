package io.saagie.model

import io.saagie.model.FormatType.FormatType

object FormatType extends Enumeration {
  type FormatType = Value
  val TEXTFILE, CSV, JSON, PARQUET, RCFILE, ORC, SEQUENCEFILE  = Value
}

sealed abstract case class Dataset(columnsToAnonymise : List[String],
                                   storageFormat: FormatType,
                                   hdfsUrl: Option[String] = None,
                                   hiveTable: Option[String] = None) {

  require((hdfsUrl.nonEmpty &&  hiveTable.isEmpty) || (hdfsUrl.isEmpty && hiveTable.nonEmpty))

}


case class TextFileDataset(override val columnsToAnonymise : List[String],
                           override val hdfsUrl: Option[String] = None,
                           override val hiveTable: Option[String] = None,
                           lineDelimiter: String = "\n",
                          ) extends Dataset(columnsToAnonymise, FormatType.TEXTFILE, hdfsUrl, hiveTable)

case class CsvDataset(override val columnsToAnonymise : List[String],
                      override val hdfsUrl: Option[String] = None,
                      override val hiveTable: Option[String] = None,
                     fieldDelimiter: String = "",
                      stringDelimiter: String = "\"\""
                     ) extends Dataset(columnsToAnonymise, FormatType.CSV, hdfsUrl, hiveTable) {
  require(hiveTable.isEmpty && hdfsUrl.nonEmpty)
}


case class JsonDataset(override val columnsToAnonymise : List[String],
                      override val storageFormat: FormatType,
                      override val hdfsUrl: Option[String] = None,
                      override val hiveTable: Option[String] = None
                     ) extends Dataset(columnsToAnonymise, FormatType.JSON, hdfsUrl, hiveTable) {
  require(hiveTable.isEmpty && hdfsUrl.nonEmpty)
}

case class ParquetDataset(override val columnsToAnonymise : List[String],
                       override val storageFormat: FormatType,
                       override val hdfsUrl: Option[String] = None,
                       override val hiveTable: Option[String] = None
                      ) extends Dataset(columnsToAnonymise, FormatType.PARQUET, hdfsUrl, hiveTable)


case class RCFileDataset(override val columnsToAnonymise : List[String],
                          override val storageFormat: FormatType,
                          override val hdfsUrl: Option[String] = None,
                          override val hiveTable: Option[String] = None
                         ) extends Dataset(columnsToAnonymise, FormatType.RCFILE, hdfsUrl, hiveTable)

case class OrcDataset(override val columnsToAnonymise : List[String],
                      override val storageFormat: FormatType,
                      override val hdfsUrl: Option[String] = None,
                      override val hiveTable: Option[String] = None
                     ) extends Dataset(columnsToAnonymise, FormatType.ORC, hdfsUrl, hiveTable)

case class SequenceFileDataset(override val columnsToAnonymise : List[String],
                      override val storageFormat: FormatType,
                      override val hdfsUrl: Option[String] = None,
                      override val hiveTable: Option[String] = None
                     ) extends Dataset(columnsToAnonymise, FormatType.SEQUENCEFILE, hdfsUrl, hiveTable)