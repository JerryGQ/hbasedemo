package com.shujia.rxwd


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.log4j.{Level, Logger}

object MohuRead {


  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    //设置SparkContext
    val sc = new SparkContext(
      new SparkConf().setAppName("MohuRead")//.setMaster("local[2]")
    )
    //设置hbase
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4")
    //设置需要查询的表
    conf.set(TableInputFormat.INPUT_TABLE,"company_illegal_info")

    //新建一个Scan扫描
    val scan = new Scan()
    //设置列族
    scan.addFamily("info".getBytes)
    //scan.setCacheBlocks(false)
    //设置需要模糊查询的字段
    val filter = new PrefixFilter("75738448_".getBytes)
    //模糊查询
    scan.setFilter(filter)
    val proto = ProtobufUtil.toScan(scan)
    val pro = Base64.encodeBytes(proto.toByteArray())
    conf.set(TableInputFormat.SCAN,pro)

    //把结果转化成RDD
    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //统计一下行数
    val count = stuRDD.count()
    println("company_illegal_info RDD Count:" + count)

  }
}

