package io.saagie.outis.link

import io.saagie.model.{DataSet, FormatType, ParquetHiveDataset, TextFileHiveDataset}
import io.saagie.outis.core.model.OutisLink
import okhttp3.{OkHttpClient, Request}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read

case class Dataset(name: String,
                   `type`: String,
                   columnsToAnonymize: Option[List[String]],
                   storageFormat: Option[String],
                   fieldDelimiter: Option[String],
                   escapeDelimiter: Option[String],
                   lineDelimiter: Option[String],
                   collectionDelimiter: Option[String],
                   mapKeyDelimiter: Option[String],
                   serdeClass: Option[String]
                  )

case class DatagovLink(datagovUrl: String, datagovNotification: String) extends OutisLink {
  override def datasetsToAnonimyze(): List[DataSet] = {
    val okHttpClient = new OkHttpClient.Builder()
      .build()

    val request = new Request.Builder()
      .url(datagovUrl)
      .get()
      .build()

    val response = okHttpClient
      .newCall(request)
      .execute()
    implicit val formats = Serialization.formats(NoTypeHints)
    if (response.isSuccessful) {
      val datasets = read[List[Dataset]](response.body().string())
        .filter { ds => ds.columnsToAnonymize.nonEmpty && ds.columnsToAnonymize.get.nonEmpty }
        .map { ds =>
          ds.`type` match {
            case "TABLE" =>
              ds.storageFormat match {
                case Some("CSV") =>
                  TextFileHiveDataset(
                    ds.columnsToAnonymize.getOrElse(List()),
                    FormatType.CSV,
                    ds.name,
                    ds.fieldDelimiter.getOrElse("\u0001"),
                    ds.escapeDelimiter.getOrElse("\u0001"),
                    ds.lineDelimiter.getOrElse("\n"),
                    ds.collectionDelimiter.getOrElse("\u0002"),
                    ds.mapKeyDelimiter.getOrElse("\u0003"),
                    ds.serdeClass.get
                  )
                case Some("PARQUET") =>
                  ParquetHiveDataset(
                    ds.columnsToAnonymize.get,
                    FormatType.PARQUET,
                    ds.name
                  )
                case _ =>
              }
            case _ =>
          }
        }
        .filter {
          _ != ((): Unit)
        }
        .asInstanceOf[List[DataSet]]
      datasets
    } else {
      throw new Exception(s"Error code: ${response.code()}")
    }
  }

  override def notifyDatasetProcessed(): Unit = ???
}
