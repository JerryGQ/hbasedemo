package com.shujia.rxwd

import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object SparkWriteHBase {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SparkWriteHBase"))
    val tablename = "company_category_20170411"
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val conf = job.getConfiguration()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4")

    val startRDD = sc.textFile("/user/hive/warehouse/prism.db/company_category_20170411")
    startRDD.map{ _.split("\t") }.map(x=>{
      val put = new Put(Bytes.toBytes(x(0))) //行健的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("company_id"),Bytes.toBytes(x(0)))  //info:name列的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("category_code"),Bytes.toBytes(x(1)))  //info:gender列的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("status"),Bytes.toBytes(x(2))) //info:age列的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("updatetime"),Bytes.toBytes(x(3)))
      /*put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("put_department"),Bytes.toBytes(x(4)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("remove_reason"),Bytes.toBytes(x(5)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("remove_date"),Bytes.toBytes(x(6)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("remove_department"),Bytes.toBytes(x(7)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("type"),Bytes.toBytes(x(8)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("fact"),Bytes.toBytes(x(9)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("deleted"),Bytes.toBytes("0"))*/

      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(conf)
  }
}
