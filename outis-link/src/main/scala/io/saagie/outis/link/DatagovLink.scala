package io.saagie.outis.link

import io.saagie.outis.core.job.AnonymizationResult
import io.saagie.outis.core.model._
import okhttp3._
import org.apache.log4j.Logger
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

/**
  * Datagov column
  *
  * @param name   name of the column.
  * @param `type` type of the column, used only to specify date in String formats.
  * @param format Optional, must be provided for String dates.
  */
case class DatagovColumn(name: String, `type`: String, format: Option[String])

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
                          delay: Int,
                          entryDate: Option[DatagovColumn],
                          columnsToAnonymize: Option[List[DatagovColumn]],
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
case class DatagovNotification(datasetId: String, timestamp: Long, rowsAnonymized: Long, duration: Long = 0, rowsInError: Long = 0)

/**
  * Datagov's parameters. User and password are mandatory for unified API usage.
  *
  * @param url
  * @param notificationUrl
  * @param user
  * @param password
  */
case class DatagovConfiguration(url: String, notificationUrl: String, user: String, password: String)

object DatagovNotification {
  def apply(anonymizationResult: AnonymizationResult): DatagovNotification = new DatagovNotification(
    anonymizationResult.dataset.identifier.asInstanceOf[String],
    System.currentTimeMillis(),
    anonymizationResult.anonymizedRows,
    anonymizationResult.duration,
    anonymizationResult.rowsInError)
}

case class DatagovLink(configuration: DatagovConfiguration) extends OutisLink {
  val log: Logger = Logger.getRootLogger
  val builder: OkHttpClient.Builder = {
    val b = new OkHttpClient.Builder()
    if (configuration.user.nonEmpty && configuration.password.nonEmpty) {
      b.authenticator(new Authenticator {
        override def authenticate(route: Route, response: Response): Request = {
          response
            .request()
            .newBuilder()
            .header("Authorization", Credentials.basic(configuration.user, configuration.password))
            .build()
        }
      })
    }
    b
  }

  val okHttpClient: OkHttpClient = builder.build()

  import DatagovLink.JSON_MEDIA_TYPE

  /**
    * @inheritdoc
    */
  override def datasetsToAnonimyze(): Either[OutisLinkException, List[DataSet]] = {
    val request = new Request.Builder()
      .url(configuration.url)
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
                    ds.name,
                    ds.columnsToAnonymize.getOrElse(List()).map(c => Column(c.name, c.`type`, c.format)),
                    FormatType.TEXTFILE,
                    ds.name,
                    ds.serdeClass.get,
                    Column(ds.entryDate.get.name, ds.entryDate.get.`type`, ds.entryDate.get.format, Some(ds.delay)),
                    ds.fieldDelimiter.getOrElse("\u0001"),
                    ds.escapeDelimiter.getOrElse("\u0001"),
                    ds.lineDelimiter.getOrElse("\n"),
                    ds.collectionDelimiter.getOrElse("\u0002"),
                    ds.mapKeyDelimiter.getOrElse("\u0003")
                  )
                case Some("PARQUET") =>
                  ParquetHiveDataset(
                    ds.id,
                    ds.name,
                    ds.columnsToAnonymize.get.map(c => Column(c.name, c.`type`, c.format)),
                    FormatType.PARQUET,
                    ds.name,
                    Column(ds.entryDate.get.name, ds.entryDate.get.`type`, ds.entryDate.get.format, Some(ds.delay))
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
      log.info(s"Datasets: $datasets")
      Right(datasets)
    } else {
      Left(OutisLinkException(s"Error while retrieving datasets to anonymize: ${response.code()}, ${response.message()}"))
    }
  }

  /**
    * @inheritdoc
    */
  override def notifyDatasetProcessed(anonymizationResult: AnonymizationResult): Either[OutisLinkException, String] = {
    implicit val formats = Serialization.formats(NoTypeHints)

    val content = write(DatagovNotification(anonymizationResult))
    val body = RequestBody.create(JSON_MEDIA_TYPE, content)

    log.info(s"Body: $content")

    val request = new Request.Builder()
      .url(configuration.notificationUrl)
      .put(body)
      .build()

    val response = okHttpClient
      .newCall(request)
      .execute()

    if (response.isSuccessful) {
      Right(response.body().string())
    } else {
      Left(OutisLinkException(s"Problem with notification: ${response.code()}, ${response.message()}, ${response.body().string()}"))
    }
  }
}

object DatagovLink {
  val JSON_MEDIA_TYPE: MediaType = MediaType.parse("application/json")
}
