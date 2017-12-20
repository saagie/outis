package io.saagie.outis.core.job

import java.lang.reflect.Method
import java.sql.{Date, Timestamp}

import com.databricks.spark.avro._
import io.saagie.outis.core.anonymize.{AnonymizationException, AnonymizeDate, AnonymizeNumeric, AnonymizeString}
import io.saagie.outis.core.model._
import io.saagie.outis.core.util.HdfsUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success, Try}

case class AnonymizationJob(dataset: DataSet, outisConf: OutisConf = OutisConf())(implicit spark: SparkSession) {

  import org.apache.spark.sql.functions.col

  def anonymize(): Either[AnonymizationException, AnonymizationResult] = {
    Try {
      dataset match {
        case d: HiveDataSet => anonymiseFromHive(d)
        case d: HdfsDataSet => anonymizeFromHdfs(d)
      }
    } match {
      case Success(s) => s
      case Failure(e) =>
        Left(AnonymizationException(s"Impossible to anonymize dataset: ${dataset.identifier} : ${dataset.name}", e))
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
      val columnsNonAnonymised = df.columns.filter(c => !(dataset.columnsToAnonymize contains c))
      val tmpPath = dataset.hdfsUrl + "-tmp"

      val numberOfRowsToProcess = df
        .count()

      val anodf = df.select(
        columnsNonAnonymised.map(c => col(c).alias(c))
          .union(dataset.columnsToAnonymize.map(c => {
            val column = col(c.name).alias(c.name)
            anonymizedRows.add(1)
            column
          })
          ): _*)

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
    import spark.implicits._

    val start = System.currentTimeMillis()
    val Array(database, table) = dataset.table.split('.')

    val tmpTable = s"$database.${table}_outis_tmp"

    val sparkTmpTable = s"spark_$table"

    val entryDate = dataset.entryDate.format match {
      case Some(f) =>
        to_date(unix_timestamp(col(dataset.entryDate.name), f))
      case None => to_date(col(dataset.entryDate.name))
    }

    val condition = date_add(entryDate, dataset.entryDate.delay.get) < current_date()

    val df: DataFrame = spark
      .sql(s"SELECT * FROM $database.$table")

    //TODO: Make this serializable
    /*    val anonymizeString = getStringAnonymization.right.map(method => spark.sqlContext.udf.register("anonymizeString",
          (s: String) =>
            method.invoke(null, outisConf.getParameters(OutisConf.ANONYMIZER_STRING).map({
              case ColumnValue => s
              case x => x
            }).asInstanceOf[Seq[Object]]: _*).asInstanceOf[String]
        ))*/
    val errorAccumulator: LongAccumulator = spark.sparkContext.longAccumulator("errors")

    val anonymizeString = Right(spark.udf.register("anonymizeString", (s: String) => AnonymizeString.substitute(s, errorAccumulator)))
    val anonymizeByte = Right(spark.udf.register("anonymizeByte", (b: Byte) => AnonymizeNumeric.substituteByte(b, errorAccumulator)))
    val anonymizeShort = Right(spark.udf.register("anonymizeShort", (s: Short) => AnonymizeNumeric.substituteShort(s, errorAccumulator)))
    val anonymizeInt = Right(spark.udf.register("anonymizeInt", (i: Int) => AnonymizeNumeric.substituteInt(i, errorAccumulator)))
    val anonymizeLong = Right(spark.udf.register("anonymizeLong", (l: Long) => AnonymizeNumeric.substituteLong(l, errorAccumulator)))
    val anonymizeFloat = Right(spark.udf.register("anonymizeFloat", (f: Float) => AnonymizeNumeric.substituteFloat(f, errorAccumulator)))
    val anonymizeDouble = Right(spark.udf.register("anonymizeDouble", (d: Double) => AnonymizeNumeric.substituteDouble(d, errorAccumulator)))
    val anonymizeBigDecimal = Right(spark.udf.register("anonymize", (bd: BigDecimal) => AnonymizeNumeric.substituteBigDecimal(bd, errorAccumulator)))
    val anonymizeDate = Right(spark.udf.register("anonymizeDate", (d: Date) => AnonymizeDate.randomDate(d, errorAccumulator)))
    val anonymizeTimestamp = Right(spark.udf.register("anonymizeTimestamp", (d: Timestamp) => AnonymizeDate.randomTimestamp(d, errorAccumulator)))
    val anonymizeDateString = Right(spark.udf.register("anonymizeDateString", (d: String, pattern: String) => AnonymizeDate.randomString(d, pattern, errorAccumulator)))

    if (anonymizeString.isRight) {
      val stringAnonymizer = anonymizeString.right.get
      val byteAnonymizer = anonymizeByte.right.get
      val shortAnonymizer = anonymizeShort.right.get
      val intAnonymizer = anonymizeInt.right.get
      val longAnonymizer = anonymizeLong.right.get
      val floatAnonymizer = anonymizeFloat.right.get
      val doubleAnonymizer = anonymizeDouble.right.get
      val bigDecimalAnonymizer = anonymizeBigDecimal.right.get
      val dateAnonymizer = anonymizeDate.right.get
      val timestampAnonymizer = anonymizeTimestamp.right.get
      val dateStringAnonymizer = anonymizeDateString.right.get

      val columnsNonAnonymized = df.columns.filter(c => !(dataset.columnsToAnonymize.map(_.name) contains c))
      val anodf = df.select(
        columnsNonAnonymized.map(c => col(c).alias(c))
          .union(dataset.columnsToAnonymize
            .map(c => {
              df.schema(c.name).dataType match {
                case StringType => c.columnType match {
                  case "date" => dateStringAnonymizer(col(c.name), lit(c.format.get)).alias(c.name)
                  case _ => stringAnonymizer(col(c.name)).alias(c.name)
                }
                case ByteType => byteAnonymizer(col(c.name)).alias(c.name)
                case ShortType => shortAnonymizer(col(c.name)).alias(c.name)
                case IntegerType => intAnonymizer(col(c.name)).alias(c.name)
                case LongType => longAnonymizer(col(c.name)).alias(c.name)
                case FloatType => floatAnonymizer(col(c.name)).alias(c.name)
                case DoubleType => doubleAnonymizer(col(c.name)).alias(c.name)
                case DecimalType() => bigDecimalAnonymizer(col(c.name)).alias(c.name)
                case TimestampType => timestampAnonymizer().alias(c.name)
                case DateType => dateAnonymizer().alias(c.name)
                case _ => col(c.name).alias(c.name)
              }
            })
          ): _*)
        .where(condition)
        .union(
          df
            .select(columnsNonAnonymized.union(dataset.columnsToAnonymize.map(_.name)).map(col):_*)
            .where(not(condition)))

      val numberOfRowsProcessed = anodf.count() - errorAccumulator.value
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

      Right(AnonymizationResult(dataset, numberOfRowsProcessed, System.currentTimeMillis() - start, errorAccumulator.value))
    } else {
      Left(AnonymizationException("Anonymization not present"))
    }
  }
}
