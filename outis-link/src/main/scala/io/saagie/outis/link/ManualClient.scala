package io.saagie.outis.link

import io.saagie.outis.core.SparkProgram
import org.apache.spark.sql.SparkSession

object ManualClient extends App {
  def anonymize(): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    System.setProperty("hive.metastore.uris", "thrift://nn1:9083")
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("Manual Anonymization")
      .master("local[*]")
      .config("hive.metastore.warehouse.dir", "hdfs://cluster/user/hive/warehouse")
      .config("spark.eventLog.dir", "hdfs://cluster/tmp/spark-events")
      .config("spark.eventLog.enabled", "true")
      .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
      .enableHiveSupport()
      .getOrCreate()
    SparkProgram(ManualLink()).launchAnonymisation()
    sparkSession.stop()
  }

  anonymize()
}
