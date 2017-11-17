package io.saagie.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

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
