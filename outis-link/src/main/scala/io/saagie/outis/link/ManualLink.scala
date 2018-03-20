package io.saagie.outis.link

import io.saagie.outis.core.job.AnonymizationResult
import io.saagie.outis.core.model._
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

case class ManualLink() extends OutisLink {
  /**
    * This method returns the list of datasets to anonymize.
    *
    * @return
    */
  override def datasetsToAnonimyze(): Either[OutisLinkException, List[DataSet]] = {
    Right(List(ParquetHiveDataset(
      "0",
      "anonymization.table",
      List(
        Column("name", "string"),
        Column("byte_value", "byte"),
        Column("short_value", "short"),
        Column("int_value", "int"),
        Column("long_value", "long"),
        Column("float_value", "float"),
        Column("double_value", "double"),
        Column("decimal_value", "decimal"),
        Column("timestamp_value", "timestamp"),
        Column("date_value", "date"),
        Column("date_string_value", "date", Some("yyyy-MM-dd"))),
      FormatType.PARQUET,
      "anonymization.test_table",
      Column("creation_date", "date", None, Some(-1))
    ),
      TextFileHiveDataset(
        "1",
        "anonymization.test_time",
        List(
          Column("time", "double"),
          Column("comment", "string"),
          Column("user_id", "bigint")
        ),
        FormatType.TEXTFILE,
        "anonymization.test_time",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        Column("date", "date", Some("yyyy-MM-dd HH:mm:ss.SSS"), Some(200)),
        "\u0001",
        "\u0001"
      )))
  }

  /**
    * The method which will be called when a dataset has been processed.
    *
    * @param anonymizationResult The processed result.
    * @return
    */
  override def notifyDatasetProcessed(anonymizationResult: AnonymizationResult): Either[OutisLinkException, String] = {
    implicit val formats = Serialization.formats(NoTypeHints)
    Right(write(anonymizationResult))
  }
}
