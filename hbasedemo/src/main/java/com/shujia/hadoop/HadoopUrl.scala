package com.shujia.hadoop

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

object HadoopUrl {
  /**
    * 用hadoop的URL读取数据，转换成buffer，一行行读取
    * @param args
    */

  def main(args: Array[String]): Unit = {

    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    val in = new URL("hdfs://192.168.0.206/test/dele.txt").openStream()
    val buf = new BufferedReader(new InputStreamReader(in))
    var x = buf.readLine()
    while(x != null ){
      println(x)
      x = buf.readLine()
    }
    /*对读取的数据做一个复制，然后输出
    IOUtils.copyBytes(in, System.out, 2048,false)
    IOUtils.closeStream(in)*/
  }
}