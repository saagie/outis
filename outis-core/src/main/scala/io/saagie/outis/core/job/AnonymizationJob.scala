package io.saagie.outis.core.job

import java.lang.reflect.Method

import com.databricks.spark.avro._
import io.saagie.model._
import io.saagie.outis.core.anonymize.{AnonymizationException, AnonymizeString}
import io.saagie.outis.core.util.HdfsUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success, Try}

case class AnonymizationJob(dataset: DataSet, outisConf: OutisConf = OutisConf())(implicit spark: SparkSession) {

  import org.apache.spark.sql.functions.col

  def anonymize(): Either[AnonymizationException, AnonymizationResult] = {
    dataset match {
      case d: HiveDataSet => anonymiseFromHive(d)
      case d: HdfsDataSet => anonymizeFromHdfs(d)
    }
  }

  private def anonymizeFromHdfs[T <: HdfsDataSet](dataset: T): Either[AnonymizationException, AnonymizationResult] = {
    val start = System.currentTimeMillis()

    val path = Path.mergePaths(new Path(dataset.hdfsUrl), new Path(dataset.hdfsPath)).toString

    val possibleDf: Either[AnonymizationException, DataFrame] = dataset match {
      case d: CsvHdfsDataset => Right(spark.read.option("delimiter", d.fieldDelimiter).option("quote", d.quoteDelimiter).option("header", d.hasHeader).csv(path))
      case d: ParquetHdfsDataset =>
        spark.sql(s"SET spark.sql.parquet.compression.codec = ${d.compressionCodec.toString}")
        Right(spark.read.option("mergeSchema", d.mergeSchema).parquet(path))
      case _: OrcHdfsDataset => Right(spark.read.orc(path))
      case _: JsonHdfsDataset => Right(spark.read.json(path))
      case _: AvroHdfsDataset => Right(spark.read.avro(path))
      case _ => Left(AnonymizationException("Format not supported by HdfsDataSet."))
    }

    if (possibleDf.isRight) {
      val df = possibleDf.right.get
      val anonymizedRows = spark.sparkContext.longAccumulator("anonymizedRows")
      val columnsNonAnonymised = df.columns.filter(c => !(dataset.columnsToAnonymise contains c))
      val tmpPath = dataset.hdfsUrl + "-tmp"
      //val condition = df("")

      val numberOfRowsToProcess = df
        //.where(condition)
        .count()

      val anodf = df.select(
        columnsNonAnonymised.map(c => col(c).alias(c))
          .union(dataset.columnsToAnonymise.map(c => {
            val column = col(c).alias(c)
            anonymizedRows.add(1)
            column
          })
          ): _*)
      //.where(condition)

      val numberOfRowsProcessed = anodf.count()

      anodf
        .write
        .format(dataset.storageFormat.toString)
        .save(tmpPath)

      HdfsUtils(dataset.hdfsUrl).deleteFiles(List(new Path(dataset.hdfsUrl)))
      Right(AnonymizationResult(dataset, anonymizedRows.value, System.currentTimeMillis() - start, numberOfRowsToProcess - numberOfRowsProcessed))
    } else {
      Left(possibleDf.left.get)
    }
  }

  /**
    * Loads anonymization string method.
    *
    * @return
    */
  private def getStringAnonymization: Either[AnonymizationException, Method] = {
    ScalaClassLoader(getClass.getClassLoader).tryToLoadClass(outisConf.getClassFor(OutisConf.ANONYMIZER_STRING)) match {
      case Some(x: Class[_]) =>
        Try(x.getDeclaredMethod(outisConf.getMethodFor(OutisConf.ANONYMIZER_STRING), outisConf.getParameterClassesFor(OutisConf.ANONYMIZER_STRING): _*)) match {
          case Success(m) => Right(m)
          case Failure(e) => Left(AnonymizationException(s"Impossible to load method: ${outisConf.getMethodFor(OutisConf.ANONYMIZER_STRING)}", e))
        }
      case None => Left(AnonymizationException(s"Anonymization class not found: ${outisConf.getClassFor(OutisConf.ANONYMIZER_STRING)}"))
    }
  }

  /**
    * Anonymize hive datasets.
    *
    * @param dataset The dataset to anonymize
    * @tparam T Dataset class, must be an HiveDataSet.
    * @return
    */
  private def anonymiseFromHive[T <: HiveDataSet](dataset: T): Either[AnonymizationException, AnonymizationResult] = {
    import org.apache.spark.sql.functions._
    val start = System.currentTimeMillis()
    val Array(database, table) = dataset.table.split('.')

    val tmpTable = s"$database.${table}_outis_tmp"

    val sparkTmpTable = s"spark_$table"

    //val condition = $""

    val df: DataFrame = spark.sql(s"SELECT * FROM $database.$table")
    val numberOfRowsToProcess = df
      //.where(condition)
      .count()

    //TODO: Make this serializable
    /*    val anonymizeString = getStringAnonymization.right.map(method => spark.sqlContext.udf.register("anonymizeString",
          (s: String) =>
            method.invoke(null, outisConf.getParameters(OutisConf.ANONYMIZER_STRING).map({
              case ColumnValue => s
              case x => x
            }).asInstanceOf[Seq[Object]]: _*).asInstanceOf[String]
        ))*/
    val anonymizeString = Right(spark.udf.register("anonymizeString", (s: String) => AnonymizeString.substitute(s)))
    if (anonymizeString.isRight) {
      val stringAnonimyzer = anonymizeString.right.get
      val columnsNonAnonymized = df.columns.filter(c => !(dataset.columnsToAnonymise contains c))
      val anodf = df.select(
        columnsNonAnonymized.map(c => col(c).alias(c))
          .union(dataset.columnsToAnonymise
            .map(c => {
              df.schema(c).dataType match {
                case StringType => stringAnonimyzer(col(c)).alias(c)
                case _ => col(c).alias(c)
              }
            })
          ): _*)
      //.where(condition)

      val numberOfRowsProcessed = anodf.count()
      anodf.show()
      anodf.createOrReplaceTempView(sparkTmpTable)

      val options: String = dataset match {
        case d: TextFileHiveDataset => s"ROW FORMAT DELIMITED FIELDS TERMINATED BY '${d.fieldDelimiter}' ESCAPED BY '${d.escapeDelimiter}' LINES TERMINATED BY '${d.lineDelimiter}' STORED AS ${d.storageFormat.toString}"
        case d: ParquetHiveDataset => s"STORED AS ${d.storageFormat.toString}"
        case _ => s"OPTIONS(fileFormat '${dataset.storageFormat.toString}')"
      }

      val createTmpTable = s"CREATE TABLE $tmpTable $options AS SELECT ${df.columns.mkString(",")} FROM $sparkTmpTable"
      spark.sql(createTmpTable)

      val dropTable = s"DROP TABLE $database.$table"
      spark.sql(dropTable)
      val alterTable = s"ALTER TABLE $tmpTable RENAME TO $database.$table"
      spark.sql(alterTable)
      Right(AnonymizationResult(dataset, numberOfRowsProcessed, System.currentTimeMillis() - start, numberOfRowsToProcess - numberOfRowsProcessed))
    } else {
      Left(AnonymizationException("Anonymization not present"))
    }
  }
}
