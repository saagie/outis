package io.saagie.outis.core.model

import io.saagie.outis.core.model.FormatType.FormatType
import io.saagie.outis.core.model.ParquetCompressionCodec.ParquetCompressionCodec


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

case class Column(name: String, columnType: String, format: Option[String] = None, delay: Option[Int] = None)

sealed trait DataSet {
  def identifier: Any

  def columnsToAnonymize: List[Column]

  def storageFormat: FormatType

  def entryDate: Column
}

sealed trait HdfsDataSet extends DataSet {
  def columnsToAnonymize: List[Column]

  def entryDate: Column

  def storageFormat: FormatType

  def hdfsUrl: String

  def hdfsPath: String
}

case class CsvHdfsDataset(
                           identifier: Any,
                           columnsToAnonymize: List[Column],
                           storageFormat: FormatType,
                           hdfsUrl: String,
                           hdfsPath: String,
                           entryDate: Column,
                           fieldDelimiter: String = ",",
                           quoteDelimiter: String = "\"\"",
                           hasHeader: Boolean = true
                         ) extends HdfsDataSet

case class JsonHdfsDataset(
                            identifier: Any,
                            columnsToAnonymize: List[Column],
                            storageFormat: FormatType,
                            hdfsUrl: String,
                            hdfsPath: String,
                            entryDate: Column
                          ) extends HdfsDataSet

case class ParquetHdfsDataset(
                               identifier: Any,
                               columnsToAnonymize: List[Column],
                               storageFormat: FormatType,
                               hdfsUrl: String,
                               hdfsPath: String,
                               entryDate: Column,
                               mergeSchema: Boolean = false,
                               compressionCodec: ParquetCompressionCodec = ParquetCompressionCodec.SNAPPY
                             ) extends HdfsDataSet

case class OrcHdfsDataset(
                           identifier: Any,
                           columnsToAnonymize: List[Column],
                           storageFormat: FormatType,
                           hdfsUrl: String,
                           hdfsPath: String,
                           entryDate: Column
                         ) extends HdfsDataSet

case class AvroHdfsDataset(
                            identifier: Any,
                            columnsToAnonymize: List[Column],
                            storageFormat: FormatType,
                            hdfsUrl: String,
                            hdfsPath: String,
                            entryDate: Column
                          ) extends HdfsDataSet


sealed trait HiveDataSet extends DataSet {
  def columnsToAnonymize: List[Column]

  def entryDate: Column

  def storageFormat: FormatType

  def table: String
}

case class ParquetHiveDataset(
                               identifier: Any,
                               columnsToAnonymize: List[Column],
                               storageFormat: FormatType,
                               table: String,
                               entryDate: Column,
                               mergeSchema: Boolean = false
                             ) extends HiveDataSet

case class TextFileHiveDataset(
                                identifier: Any,
                                columnsToAnonymize: List[Column],
                                storageFormat: FormatType,
                                table: String,
                                serdeClass: String,
                                entryDate: Column,
                                fieldDelimiter: String = "\u0001",
                                escapeDelimiter: String = "\"\"",
                                lineDelimiter: String = "\n",
                                collectionDelimiter: String = "\u0002",
                                mapKeyDelimiter: String = "\u0003",
                              ) extends HiveDataSet


case class AvroHiveDataset(
                            identifier: Any,
                            columnsToAnonymize: List[Column],
                            storageFormat: FormatType,
                            table: String,
                            entryDate: Column
                          ) extends HiveDataSet


case class RcFileHiveDataset(
                              identifier: Any,
                              columnsToAnonymize: List[Column],
                              storageFormat: FormatType,
                              table: String,
                              serdeClass: String,
                              entryDate: Column
                            ) extends HiveDataSet

case class OrcHiveDataset(
                           identifier: Any,
                           columnsToAnonymize: List[Column],
                           storageFormat: FormatType,
                           table: String,
                           entryDate: Column
                         ) extends HiveDataSet

case class SequenceFileHiveDataset(
                                    identifier: Any,
                                    columnsToAnonymize: List[Column],
                                    storageFormat: FormatType,
                                    table: String,
                                    serdeClass: String,
                                    entryDate: Column
                                  ) extends HiveDataSet
