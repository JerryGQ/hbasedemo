package com.shujia.hadoop


import org.apache.spark.{SparkConf, SparkContext}

object ReadHadoop {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    //conf.set("spark.master", "local")
    conf.set("spark.app.name", "spark demo")
    val sc = new SparkContext(conf);
    // 读取hdfs数据
    val textFileRdd = sc.textFile("/test/student/students.txt")
    val fRdd = textFileRdd.flatMap { _.split(",") }

    // 写入数据到hdfs系统
    fRdd.saveAsTextFile("/test/wcresult")
  }
}