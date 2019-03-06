package com.shujia.rxwd

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.util.concurrent.Executors

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MohuDeleteHBase {

  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //设置SparkContext
    val sc = new SparkContext(
      new SparkConf().setAppName("MohuDeleteHBase")
    )
    //设置hbase
    val tablename = "student"

    val conf = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4")
    //设置需要查询的表
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    val exe = Executors.newFixedThreadPool(10)
    val conn = ConnectionFactory.createConnection(conf, exe)
    val table = conn.getTable(TableName.valueOf(tablename))

    //新建一个Scan扫描
    val scan = new Scan()
    //设置列族
    scan.addFamily("info".getBytes)

    //通过Hadoop的URL去读取hdfs上的文件
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    val in = new URL("hdfs://192.168.0.206/test/student_id/student_id.txt").openStream()
    val buf = new BufferedReader(new InputStreamReader(in))
    var x = buf.readLine()
    while (x != null) {
      //设置需要模糊查询的字段
      val filter = new PrefixFilter(x.getBytes)
      //模糊查询
      scan.setFilter(filter)
      val proto = ProtobufUtil.toScan(scan)
      val pro = Base64.encodeBytes(proto.toByteArray())
      conf.set(TableInputFormat.SCAN, pro)


      //把结果转化成RDD
      val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
      //统计一下行数
      //val count = stuRDD.count()
      //println("company_illegal_info RDD Count:" + count)

      stuRDD.foreachPartition({ x =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4")
        val exe = Executors.newFixedThreadPool(10)
        val conn = ConnectionFactory.createConnection(conf, exe)
        val table = conn.getTable(TableName.valueOf(tablename))
        x.foreach { case (_, rdd) =>
          var key = Bytes.toString(rdd.getRow)
          //println("Key:"+key)
          val info = new Delete(Bytes.toBytes(key))
          table.delete(info)
        }
        table.close()
        conn.close()

      })
      x = buf.readLine()
    }
    print("delete successfully!")
  }
}
