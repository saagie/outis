package io.saagie.job

import com.databricks.spark.avro._
import io.saagie.model._
import io.saagie.outis.core.anonymize.AnonymizeString
import io.saagie.util.HdfsUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Anonymize(dataset: DataSet)(implicit spark: SparkSession) {

  import org.apache.spark.sql.functions.col

  def anonymise(): Unit = {

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

  private def anonymiseFromHive[T <: HiveDataSet](dataset: T) = {

    val table = dataset.table
    val tmpTable = table + "-tmp"
    val sparkTmpTable = "spark-" + tmpTable

    val df: DataFrame = spark.sql(s"SELECT * FROM $table")

    val anonymize = spark.sqlContext.udf.register("anonymize", s => AnonymizeString.setToX(s))

    val columnsNonAnonymized = df.columns.filter(c => !(dataset.columnsToAnonymise contains c))
    df.select(columnsNonAnonymized.map(c => col(c).alias(c)).union(dataset.columnsToAnonymise.map(c => anonymize(col(c).alias(c)))): _*)

    df.createOrReplaceTempView(sparkTmpTable)

    val options: String = dataset match {
      case d: TextFileHiveDataset => s"OPTIONS(fileFormat '${dataset.storageFormat.toString}', fieldDelim '${d.fieldDelimiter}', escapeDelim '${d.escapeDelimiter}', collectionDelim '${d.collectionDelimiter}', mapkeyDelim '${d.mapKeyDelimiter}', lineDelim '${d.lineDelimiter}')"
      case _ => s"OPTIONS(fileFormat '${dataset.storageFormat.toString}')"
    }

    val createTmpTable = s"CREATE TABLE $tmpTable AS SELECT * FROM  $sparkTmpTable USING hive $options"
    spark.sql(createTmpTable)

    val dropTable = s"DROP TABLE $table"
    spark.sql(dropTable)
  }
}
