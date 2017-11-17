package io.saagie.model

import io.saagie.model.FormatType.FormatType
import io.saagie.model.ParquetCompressionCodec.ParquetCompressionCodec


object FormatType extends Enumeration {
  type FormatType = Value
  val TEXTFILE: Value = Value("textfile")
  val CSV: Value = Value("csv")
  val JSON: Value = Value("json")
  val PARQUET: Value = Value("parquet")
  val RCFILE: Value = Value("rcfile")
  val ORC: Value = Value("orc")
  val SEQUENCEFILE: Value = Value("sequencefile")
  val AVRO: Value = Value("avro")
}

object ParquetCompressionCodec extends Enumeration {
  type ParquetCompressionCodec = Value
  val UNCOMPRESSED: Value = Value("uncompressed")
  val SNAPPY: Value = Value("snappy")
  val GZIP: Value = Value("gzip")
  val LZO: Value = Value("lzo")
}

sealed trait DataSet {
  def identifier: Any

  def columnsToAnonymise: List[String]

  def storageFormat: FormatType
}

sealed trait HdfsDataSet extends DataSet {
  def columnsToAnonymise: List[String]

  def storageFormat: FormatType

  def hdfsUrl: String

  def hdfsPath: String
}

case class CsvHdfsDataset(
                           identifier: Any,
                           columnsToAnonymise: List[String],
                           storageFormat: FormatType,
                           hdfsUrl: String,
                           hdfsPath: String,
                           fieldDelimiter: String = ",",
                           quoteDelimiter: String = "\"\"",
                           hasHeader: Boolean = true
                         ) extends HdfsDataSet


case class JsonHdfsDataset(
                            identifier: Any,
                            columnsToAnonymise: List[String],
                            storageFormat: FormatType,
                            hdfsUrl: String,
                            hdfsPath: String
                          ) extends HdfsDataSet


case class ParquetHdfsDataset(
                               identifier: Any,
                               columnsToAnonymise: List[String], storageFormat: FormatType,
                               hdfsUrl: String,
                               hdfsPath: String,
                               mergeSchema: Boolean = false,
                               compressionCodec: ParquetCompressionCodec = ParquetCompressionCodec.SNAPPY
                             ) extends HdfsDataSet

case class OrcHdfsDataset(
                           identifier: Any,
                           columnsToAnonymise: List[String],
                           storageFormat: FormatType,
                           hdfsUrl: String,
                           hdfsPath: String
                         ) extends HdfsDataSet

case class AvroHdfsDataset(
                            identifier: Any,
                            columnsToAnonymise: List[String],
                            storageFormat: FormatType,
                            hdfsUrl: String,
                            hdfsPath: String
                          ) extends HdfsDataSet


sealed trait HiveDataSet extends DataSet {
  def columnsToAnonymise: List[String]

  def storageFormat: FormatType

  def table: String
}


case class ParquetHiveDataset(
                               identifier: Any,
                               columnsToAnonymise: List[String], storageFormat: FormatType,
                               table: String,
                               mergeSchema: Boolean = false
                             ) extends HiveDataSet

case class TextFileHiveDataset(
                                identifier: Any,
                                columnsToAnonymise: List[String], storageFormat: FormatType,
                                table: String,
                                fieldDelimiter: String = "\u0001",
                                escapeDelimiter: String = "\"\"",
                                lineDelimiter: String = "\n",
                                collectionDelimiter: String = "\u0002",
                                mapKeyDelimiter: String = "\u0003",
                                serdeClass: String
                              ) extends HiveDataSet


case class AvroHiveDataset(
                            identifier: Any,
                            columnsToAnonymise: List[String],
                            storageFormat: FormatType,
                            table: String
                          ) extends HiveDataSet


case class RcFileHiveDataset(
                              identifier: Any,
                              columnsToAnonymise: List[String],
                              storageFormat: FormatType,
                              table: String,
                              serdeClass: String
                            ) extends HiveDataSet

case class OrcHiveDataset(
                           identifier: Any,
                           columnsToAnonymise: List[String],
                           storageFormat: FormatType,
                           table: String
                         ) extends HiveDataSet

case class SequenceFileHiveDataset(
                                    identifier: Any,
                                    columnsToAnonymise: List[String],
                                    storageFormat: FormatType,
                                    table: String,
                                    serdeClass: String
                                  ) extends HiveDataSet
