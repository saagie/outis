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
    Right(List(TextFileHiveDataset(
      "0",
      "anonymization.table",
      List(
        Column("name", "string", None),
        Column("byte_value", "byte", None),
        Column("short_value", "short", None),
        Column("int_value", "int", None),
        Column("long_value", "long", None),
        Column("float_value", "float", None),
        Column("double_value", "double", None),
        Column("decimal_value", "decimal"),
        Column("timestamp_value", "date", None),
        Column("date_value", "date", None),
        Column("date_string_value", "date", Some("yyyy-MM-dd"))),
      FormatType.TEXTFILE,
      "anonymization.test_table",
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
      Column("creation_date", "date", None, Some(-1)),
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
