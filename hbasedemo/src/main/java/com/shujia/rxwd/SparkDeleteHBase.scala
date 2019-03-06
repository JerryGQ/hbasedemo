package com.shujia.rxwd

import java.io.{BufferedReader,InputStreamReader}
import java.net.URL
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Delete}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object SparkDeleteHBase {

  def main(args: Array[String]): Unit = {

    //设置spark和hbase的连接
    val sc = new SparkContext(new SparkConf().setAppName("SparkDeleteHBase"))//.setMaster("local[2]"))
    val conf = HBaseConfiguration.create()

    //设置集群地址，否则不能在本地执行，提交到集群运行不需要这个配置
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4")

    //获取HTable对象
    val conn = ConnectionFactory.createConnection(conf)
    val tablename = "student"
    val admin = conn.getAdmin()
    val table = conn.getTable(TableName.valueOf(tablename))

    //通过Hadoop的URL去读取hdfs上的文件
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    val in = new URL("hdfs://192.168.0.206/test/dele.txt").openStream()
    val buf = new BufferedReader(new InputStreamReader(in))
    var x = buf.readLine()
    while(x != null ){
      val rowkey = x
      deleteRecord(rowkey)
      x = buf.readLine()
    }
    buf.close()

    //通过rowkey去删除一行数据，用的是Htable的delete方法（也可以删除制定列的数据）
    def deleteRecord(rowkey: String/*, columnFamily: String, column: String*/) = {
      val info = new Delete(Bytes.toBytes(rowkey))
      //info.addColumn(columnFamily.getBytes(), column.getBytes())
      table.delete(info)
    }
    println("delete successful!!")
  }
}
