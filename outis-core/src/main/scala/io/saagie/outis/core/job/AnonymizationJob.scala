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

  def anonymize(): Unit = {

    dataset match {
      case d: HiveDataSet => anonymiseFromHive(d)
      case d: HdfsDataSet => anonymizeFromHdfs(d)
    }
  }

  private def anonymizeFromHdfs[T <: HdfsDataSet](dataset: T): Unit = {

    val path = Path.mergePaths(new Path(dataset.hdfsUrl), new Path(dataset.hdfsPath)).toString

    val df: DataFrame = dataset match {
      case d: CsvHdfsDataset => spark.read.option("delimiter", d.fieldDelimiter).option("quote", d.quoteDelimiter).option("header", d.hasHeader).csv(path)
      case d: ParquetHdfsDataset =>
        spark.sql(s"SET spark.sql.parquet.compression.codec = ${d.compressionCodec.toString}")
        spark.read.option("mergeSchema", d.mergeSchema).parquet(path)
      case _: OrcHdfsDataset => spark.read.orc(path)
      case _: JsonHdfsDataset => spark.read.json(path)
      case _: AvroHdfsDataset => spark.read.avro(path)
      case _ => throw new Exception("Format not supported for HdfsDataSet.")
    }

    val columnsNonAnonymised = df.columns.filter(c => !(dataset.columnsToAnonymise contains c))

    val tmpPath = dataset.hdfsUrl + "-tmp"

    df.select(columnsNonAnonymised.map(c => col(c).alias(c)).union(dataset.columnsToAnonymise.map(c => col(c).alias(c))): _*)
      .write.format(dataset.storageFormat.toString).save(tmpPath)

    HdfsUtils(dataset.hdfsUrl).deleteFiles(List(new Path(dataset.hdfsUrl)))
  }

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

  private def anonymiseFromHive[T <: HiveDataSet](dataset: T) = {

    val table = dataset.table
    val tmpTable = table.split('.')(1) + "_tmp"
    val sparkTmpTable = "spark_" + tmpTable

    val df: DataFrame = spark.sql(s"SELECT * FROM $table")

    df.show()

    //TODO: Make this serializable
    /*    val anonymizeString = getStringAnonymization.right.map(method => spark.sqlContext.udf.register("anonymizeString",
          (s: String) =>
            method.invoke(null, outisConf.getParameters(OutisConf.ANONYMIZER_STRING).map({
              case ColumnValue => s
              case x => x
            }).asInstanceOf[Seq[Object]]: _*).asInstanceOf[String]
        ))*/

    val anonymizeString = Right(spark.sqlContext.udf.register("anonymizeString", (s: String) => AnonymizeString.substitute(s)))

    if (anonymizeString.isRight) {
      df.show()
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
      anodf.show()
      anodf.createOrReplaceTempView(sparkTmpTable)

      val options: String = dataset match {
        //        case d: TextFileHiveDataset => s"OPTIONS(fileFormat '${dataset.storageFormat.toString}', fieldDelim '${d.fieldDelimiter}', escapeDelim '${d.escapeDelimiter}', collectionDelim '${d.collectionDelimiter}', mapkeyDelim '${d.mapKeyDelimiter}', lineDelim '${d.lineDelimiter}')"
        case d: TextFileHiveDataset => s"ROW FORMAT DELIMITED FIELDS TERMINATED BY '${d.fieldDelimiter}' ESCAPED BY '${d.escapeDelimiter}' LINES TERMINATED BY '${d.lineDelimiter}' STORED AS ${dataset.storageFormat.toString}"
        case _ => s"OPTIONS(fileFormat '${dataset.storageFormat.toString}')"
      }

      val createTmpTable = s"CREATE TABLE $tmpTable $options AS SELECT * FROM  $sparkTmpTable"
      spark.sql(createTmpTable)

      val dropTable = s"DROP TABLE $table"
//      spark.sql(dropTable)
    } else {
      //      anonymizeString.left.get.printStackTrace()
    }
  }
}
