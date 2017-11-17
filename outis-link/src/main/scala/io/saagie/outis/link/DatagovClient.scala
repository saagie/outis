package io.saagie.outis.link

import io.saagie.outis.core.SparkProgram
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Example.
  */
object DatagovClient extends App {
  val link = DatagovLink("http://192.168.57.40:31427/api/v1/privacy/datasets", "http://192.168.57.40:31427/api/v1/privacy/events")
  System.setProperty("HADOOP_USER_NAME", "hdfs")
  System.setProperty("HIVE_METASTORE_URIS", "thrift://nn1:9083")
  val sparkConf = new SparkConf()
    .setAppName("Datagov anonymizer")
  implicit val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  SparkProgram(link).launchAnonymisation()
  sparkSession.stop()
}
