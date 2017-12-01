package io.saagie.outis.core.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Utility class to manage hdfs.
  *
  * @param hdfsNameNodeHost the namenode's host.
  */
case class HdfsUtils(hdfsNameNodeHost: String) {

  private def getHadoopFileSystem: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsNameNodeHost)
    FileSystem.get(conf)
  }

  def deleteFiles(paths: List[Path]): Unit = {
    val fs: FileSystem = getHadoopFileSystem
    paths.foreach(fs.delete(_, true))
    fs.close()
  }

}
