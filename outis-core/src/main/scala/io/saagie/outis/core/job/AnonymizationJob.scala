package io.saagie.outis.core.job

import java.sql.{Date, Timestamp}

import com.databricks.spark.avro._
import io.saagie.outis.core.anonymize.AnonymizationException
import io.saagie.outis.core.model._
import io.saagie.outis.core.util.HdfsUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.reflect.api.JavaUniverse
import scala.util.{Failure, Success, Try}

case class AnonymizationJob(dataset: DataSet, outisConf: OutisConf = OutisConf())(implicit spark: SparkSession) {

  def logger: Logger = Logger.getRootLogger

  import org.apache.spark.sql.functions.col

  private def loadAnonymizer(anonymizerName: String): Either[AnonymizationException, () => JavaUniverse#MethodMirror] = {
    Try {
      () =>
        lazy val ru = scala.reflect.runtime.universe
        lazy val mirror = ru.runtimeMirror(getClass.getClassLoader)
        lazy val name = outisConf.getClassFor(anonymizerName)
        lazy val moduleMirror = mirror.reflectModule(mirror.staticClass(name).companion.asModule)
        lazy val instanceMirror = mirror.reflect(moduleMirror.instance)
        lazy val method = instanceMirror.symbol.toType.decl(ru.TermName(outisConf.getMethodFor(anonymizerName)))
        instanceMirror.reflectMethod(method.asMethod).asInstanceOf[scala.reflect.api.JavaUniverse#MethodMirror]
    } match {
      case Success(m) => Right(m)
      case Failure(e) => Left(AnonymizationException(s"Impossible to load method: ${outisConf.getMethodFor(anonymizerName)}", e))
    }
  }

  private def createUdfs[T <: HiveDataSet](errorAccumulator: LongAccumulator, anonymizers: Map[String, Either[AnonymizationException, () => JavaUniverse#MethodMirror]]) = {
    anonymizers
      .map(t => (t._1, t._2.right.get))
      //TODO: Replace with generic type
      .map(t => {
      (t._1, t._1 match {
        case OutisConf.ANONYMIZER_STRING =>
          spark.udf.register("anonymizeString", (s: String) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_STRING).map({
              case ColumnValue => s
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[String]
          })
        case OutisConf.ANONYMIZER_BYTE =>
          spark.udf.register("anonymizeByte", (b: Byte) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_BYTE).map({
              case ColumnValue => b
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Byte]
          })
        case OutisConf.ANONYMIZER_SHORT =>
          spark.udf.register("anonymizeShort", (s: Short) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_BYTE).map({
              case ColumnValue => s
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Short]
          })
        case OutisConf.ANONYMIZER_INT =>
          spark.udf.register("anonymizeInt", (i: Int) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_INT).map({
              case ColumnValue => i
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Int]
          })
        case OutisConf.ANONYMIZER_LONG =>
          spark.udf.register("anonymizeLong", (l: Long) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_LONG).map({
              case ColumnValue => l
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Long]
          })
        case OutisConf.ANONYMIZER_FLOAT =>
          spark.udf.register("anonymizeFloat", (f: Float) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_FLOAT).map({
              case ColumnValue => f
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Float]
          })
        case OutisConf.ANONYMIZER_DOUBLE =>
          spark.udf.register("anonymizeDouble", (d: Double) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_DOUBLE).map({
              case ColumnValue => d
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Double]
          })
        case OutisConf.ANONYMIZER_BIGDECIMAL =>
          spark.udf.register("anonymizeBigDecimal", (bd: java.math.BigDecimal) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_BIGDECIMAL).map({
              case ColumnValue => bd
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[java.math.BigDecimal]
          })
        case OutisConf.ANONYMIZER_DATE =>
          spark.udf.register("anonymizeDate", (d: Date) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_DATE).map({
              case ColumnValue => d
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Date]
          })
        case OutisConf.ANONYMIZER_TIMESTAMP =>
          spark.udf.register("anonymizeTimestamp", (ts: Timestamp) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_TIMESTAMP).map({
              case ColumnValue => ts
              case x => x
            }) :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[Timestamp]
          })
        case OutisConf.ANONYMIZER_DATE_STRING =>
          spark.udf.register("anonymizeDateString", (s: String, pattern: String) => {
            val params = outisConf.getParameters(OutisConf.ANONYMIZER_DATE_STRING).map({
              case ColumnValue => s
              case x => x
            }) :+ pattern :+ errorAccumulator
            t._2()(params: _*).asInstanceOf[String]
          })
      })
    })
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
        to_date(from_unixtime(unix_timestamp(col(dataset.entryDate.name), f)))
      case None =>
        to_date(col(dataset.entryDate.name))
    }

    val condition = date_add(entryDate, dataset.entryDate.delay.get) <= current_date()

    val dfSelect: DataFrame = spark
      .sql(s"SELECT * FROM $database.$table")

    val df = dfSelect.select(dfSelect.columns.map(col) :+ (monotonically_increasing_id() as "outis_ordering"): _*)

    val errorAccumulator: LongAccumulator = spark.sparkContext.longAccumulator("errors")

    val anonymizers = outisConf.properties.keys.map(key => Map(key -> loadAnonymizer(key))).reduce(_ ++ _)
    if (anonymizers.values.map(_.isRight).reduce(_ && _)) {
      val udfs = createUdfs(errorAccumulator, anonymizers)
      val columnsToAnonymiseSet = dataset.columnsToAnonymize.map(_.name)
      val columnsNonAnonymized = df
        .columns
        .filter(c => !(columnsToAnonymiseSet contains c))

      val anodf = df.select(
        columnsNonAnonymized
          .map(c => col(c).alias(c))
          .union(dataset
            .columnsToAnonymize
            .map(c => {
              df.schema(c.name).dataType match {
                case StringType => c.columnType match {
                  case "date" => udfs(OutisConf.ANONYMIZER_DATE_STRING)(col(c.name), lit(c.format.get)).alias(c.name)
                  case _ => udfs(OutisConf.ANONYMIZER_STRING)(col(c.name)).alias(c.name)
                }
                case ByteType => udfs(OutisConf.ANONYMIZER_BYTE)(col(c.name)).alias(c.name)
                case ShortType => udfs(OutisConf.ANONYMIZER_SHORT)(col(c.name)).alias(c.name)
                case IntegerType => udfs(OutisConf.ANONYMIZER_INT)(col(c.name)).alias(c.name)
                case LongType => udfs(OutisConf.ANONYMIZER_LONG)(col(c.name)).alias(c.name)
                case FloatType => udfs(OutisConf.ANONYMIZER_FLOAT)(col(c.name)).alias(c.name)
                case DoubleType => udfs(OutisConf.ANONYMIZER_DOUBLE)(col(c.name)).alias(c.name)
                case DecimalType() => udfs(OutisConf.ANONYMIZER_BIGDECIMAL)(col(c.name)).alias(c.name)
                case DateType => udfs(OutisConf.ANONYMIZER_DATE)(col(c.name)).alias(c.name)
                case TimestampType => udfs(OutisConf.ANONYMIZER_TIMESTAMP)(col(c.name)).alias(c.name)
                case _ => col(c.name).alias(c.name)
              }
            })
          ): _*)
        .where(condition)

      val numberOfRowsProcessed = anodf.count()

      anodf
        .union(df
          .select(columnsNonAnonymized.union(dataset.columnsToAnonymize.map(_.name)).map(col): _*)
          .where(condition.isNull or not(condition)))
        .orderBy($"outis_ordering")
        .createOrReplaceTempView(sparkTmpTable)

      val options: String = dataset match {
        case d: TextFileHiveDataset => s"ROW FORMAT DELIMITED FIELDS TERMINATED BY '${d.fieldDelimiter}' ESCAPED BY '${d.escapeDelimiter}' LINES TERMINATED BY '${d.lineDelimiter}' STORED AS ${d.storageFormat.toString}"
        case d: ParquetHiveDataset => s"STORED AS ${d.storageFormat.toString}"
        case _ => s"OPTIONS(fileFormat '${dataset.storageFormat.toString}')"
      }

      val createTmpTable = s"CREATE TABLE $tmpTable $options AS SELECT ${dfSelect.columns.mkString(",")} FROM $sparkTmpTable"
      spark.sql(createTmpTable)

      val dropTable = s"DROP TABLE $database.$table"
      spark.sql(dropTable)
      val alterTable = s"ALTER TABLE $tmpTable RENAME TO $database.$table"
      spark.sql(alterTable)

      Right(AnonymizationResult(dataset, numberOfRowsProcessed, System.currentTimeMillis() - start, errorAccumulator.value))
    } else {
      anonymizers
        .filter(_._2.isLeft)
        .foreach(t => {
          t._2
            .left
            .foreach(logger.error(s"Unable to load Anonymizer: ${t._1}", _))
        })
      Left(AnonymizationException("Problem with anonymizer method(s)"))
    }
  }

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
}
