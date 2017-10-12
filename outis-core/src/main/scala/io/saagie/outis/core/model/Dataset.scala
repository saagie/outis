package io.saagie.model

import io.saagie.model.FormatType.FormatType
import io.saagie.model.ParquetCompressionCodec.ParquetCompressionCodec


object FormatType extends Enumeration {
  type FormatType = Value
  val TEXTFILE = Value("textfile")
  val CSV = Value("csv")
  val JSON = Value("json")
  val PARQUET = Value("parquet")
  val RCFILE = Value("rcfile")
  val ORC = Value("orc")
  val SEQUENCEFILE  = Value("sequencefile")
  val AVRO = Value("avro")
}

object ParquetCompressionCodec extends Enumeration {
  type ParquetCompressionCodec = Value
  val UNCOMPRESSED = Value("uncompressed")
  val SNAPPY = Value("snappy")
  val GZIP = Value("gzip")
  val LZO = Value("lzo")
}

sealed trait DataSet {
  def columnsToAnonymise : List[String]
  def storageFormat: FormatType
}

sealed trait HdfsDataSet extends DataSet {
  def columnsToAnonymise : List[String]
  def storageFormat: FormatType
  def hdfsUrl: String
  def hdfsPath: String
}

case class CsvHdfsDataset(
                           columnsToAnonymise : List[String],
                           storageFormat: FormatType,
                           hdfsUrl: String,
                           hdfsPath: String,
                           fieldDelimiter: String = ",",
                           quoteDelimiter: String = "\"\"",
                           hasHeader : Boolean = true
                         ) extends HdfsDataSet


case class JsonHdfsDataset(
                            columnsToAnonymise : List[String],
                            storageFormat: FormatType,
                            hdfsUrl: String,
                            hdfsPath: String
                          ) extends HdfsDataSet


case class ParquetHdfsDataset(
                               columnsToAnonymise : List[String], storageFormat: FormatType,
                               hdfsUrl: String,
                               hdfsPath: String,
                               mergeSchema: Boolean = false ,
                               compressionCodec: ParquetCompressionCodec = ParquetCompressionCodec.SNAPPY
                             ) extends HdfsDataSet

case class OrcHdfsDataset(
                           columnsToAnonymise : List[String],
                           storageFormat: FormatType,
                           hdfsUrl: String,
                           hdfsPath: String
                         ) extends HdfsDataSet

case class AvroHdfsDataset(
                            columnsToAnonymise : List[String],
                            storageFormat: FormatType,
                            hdfsUrl: String,
                            hdfsPath: String
                          ) extends HdfsDataSet


sealed trait HiveDataSet extends DataSet {
  def columnsToAnonymise : List[String]
  def storageFormat: FormatType
  def table: String
}


case class ParquetHiveDataset(
                               columnsToAnonymise : List[String], storageFormat: FormatType,
                               table: String,
                               mergeSchema: Boolean = false
                             ) extends HiveDataSet

case class TextFileHiveDataset(
                                columnsToAnonymise : List[String], storageFormat: FormatType,
                                table: String,
                                fieldDelimiter: String = "\001",
                                escapeDelimiter: String = "\"\"",
                                lineDelimiter: String = "\n",
                                collectionDelimiter: String = "\002",
                                mapKeyDelimiter : String = "\003",
                                serdeClass: String
                          ) extends HiveDataSet


case class AvroHiveDataset(
                            columnsToAnonymise : List[String],
                            storageFormat: FormatType,
                            table: String
                          ) extends HiveDataSet



case class RcFileHiveDataset(
                              columnsToAnonymise : List[String],
                              storageFormat: FormatType,
                              table: String,
                              serdeClass: String
                            ) extends HiveDataSet

case class OrcHiveDataset(
                           columnsToAnonymise : List[String],
                           storageFormat: FormatType,
                           table: String
                     ) extends HiveDataSet

case class SequenceFileHiveDataset(
                                    columnsToAnonymise : List[String],
                                    storageFormat: FormatType,
                                    table: String,
                                    serdeClass: String
                                  ) extends HiveDataSet
