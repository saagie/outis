package io.saagie.outis.link

import io.saagie.model.{DataSet, FormatType, ParquetHiveDataset, TextFileHiveDataset}
import io.saagie.outis.core.job.AnonymizationResult
import io.saagie.outis.core.model.{OutisLink, OutisLinkException}
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

/**
  * Datagov's dataset description.
  *
  * @param id
  * @param name
  * @param `type`
  * @param columnsToAnonymize
  * @param storageFormat
  * @param fieldDelimiter
  * @param escapeDelimiter
  * @param lineDelimiter
  * @param collectionDelimiter
  * @param mapKeyDelimiter
  * @param serdeClass
  */
case class DatagovDataset(id: String,
                          name: String,
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

/**
  * Datagov's callback response.
  *
  * @param datasetId
  * @param timestamp
  * @param rowsAnonymized
  * @param duration
  * @param rowsInError
  */
case class DatagovNotification(datasetId: String, timestamp: Long, rowsAnonymized: Long, duration: Long = 0, rowsInError: Int = 0)

object DatagovNotification {
  def apply(anonymizationResult: AnonymizationResult): DatagovNotification = new DatagovNotification(
    anonymizationResult.dataset.identifier.asInstanceOf[String],
    System.currentTimeMillis(),
    anonymizationResult.anonymizedRows,
    anonymizationResult.duration)
}

case class DatagovLink(datagovUrl: String, datagovNotificationUrl: String) extends OutisLink {

  import DatagovLink.JSON_MEDIA_TYPE

  /**
    * @inheritdoc
    */
  override def datasetsToAnonimyze(): Either[OutisLinkException, List[DataSet]] = {
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
      val body = response.body().string()
      val datasets = read[List[DatagovDataset]](body)
        .filter { ds => ds.columnsToAnonymize.nonEmpty && ds.columnsToAnonymize.get.nonEmpty }
        .map { ds =>
          ds.`type` match {
            case "TABLE" =>
              ds.storageFormat match {
                case Some("TEXT_FILE") | Some("CSV") =>
                  TextFileHiveDataset(
                    ds.id,
                    ds.columnsToAnonymize.getOrElse(List()),
                    FormatType.TEXTFILE,
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
                    ds.id,
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
      println(s"Datasets : $datasets")
      Right(datasets)
    } else {
      Left(OutisLinkException(s"Error while retrieving datasets to anonymize: ${response.code()}, ${response.message()}"))
    }
  }

  /**
    * @inheritdoc
    */
  override def notifyDatasetProcessed(anonymizationResult: AnonymizationResult): Either[OutisLinkException, String] = {
    val okHttpClient = new OkHttpClient.Builder()
      .build()

    implicit val formats = Serialization.formats(NoTypeHints)

    val request = new Request.Builder()
      .url(datagovNotificationUrl)
      .post(RequestBody.create(JSON_MEDIA_TYPE, write(DatagovNotification(anonymizationResult))))
      .build()

    val response = okHttpClient
      .newCall(request)
      .execute()

    if (response.isSuccessful) {
      Right(response.body().string())
    } else {
      Left(OutisLinkException(s"Problem with notification: ${response.code()}, ${response.message()}"))
    }
  }
}

object DatagovLink {
  val JSON_MEDIA_TYPE: MediaType = MediaType.parse("application/json")
}
