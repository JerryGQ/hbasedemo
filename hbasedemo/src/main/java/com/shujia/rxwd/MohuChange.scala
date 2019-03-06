package com.shujia.rxwd

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.Executors

import org.apache.hadoop.fs.{FileSystem, FsUrlStreamHandlerFactory, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{InvalidJobConfException, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object MohuChange {

  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //设置SparkContext
    val sc = new SparkContext(
      new SparkConf().setAppName("MohuChange")
    )
    //设置hbase
    val tablename = "company_illegal_info_new"

    val conf = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4")
    //设置需要查询的表
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    //新建一个Scan扫描
    val scan = new Scan()
    //设置列族
    scan.addFamily("info".getBytes)

    //通过Hadoop的URL去读取hdfs上的文件
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    val in = new URL("hdfs://192.168.0.206/test/id/result.txt").openStream()
    val buf = new BufferedReader(new InputStreamReader(in))
    var x = buf.readLine()
    while (x != null) {
      val id = x + "_"
      //val id = x
      //设置需要模糊查询的字段
      val filter = new PrefixFilter(id.getBytes)
      //模糊查询
      scan.setFilter(filter)
      val proto = ProtobufUtil.toScan(scan)
      val pro = Base64.encodeBytes(proto.toByteArray())
      conf.set(TableInputFormat.SCAN, pro)


      //把结果转化成RDD
      val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      val result = stuRDD.map({case(_,rdd)=>
        val key = Bytes.toString(rdd.getRow)
        (key,"1")
      })
      RDD.rddToPairRDDFunctions(result).partitionBy(new HashPartitioner(1))
        .saveAsHadoopFile("hdfs://192.168.0.206/test/out",
          classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

      x = buf.readLine()

    }


    /**
      * 自定义一个输出文件类
      */
    case class RDDMultipleTextOutputFormat() extends MultipleTextOutputFormat[Any, Any] {

      val currentTime: Date = new Date()
      val formatter = new SimpleDateFormat("yyyy-MM-dd-HHmmss");
      val dateString = formatter.format(currentTime);

      //自定义保存文件名
      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
        //key 和 value就是rdd中的(key,value)，name是part-00000默认的文件名
        //保存的文件名称，这里用字符串拼接系统生成的时间戳来区分文件名，可以自己定义
        "HTLXYFY" + dateString
      }

      override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
        val name: String = job.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR)
        var outDir: Path = if (name == null) null else new Path(name)
        //当输出任务不等于0 且输出的路径为空，则抛出异常
        if (outDir == null && job.getNumReduceTasks != 0) {
          throw new InvalidJobConfException("Output directory not set in JobConf.")
        }
        //当有输出任务和输出路径不为null时
        if (outDir != null) {
          val fs: FileSystem = outDir.getFileSystem(job)
          outDir = fs.makeQualified(outDir)
          outDir = new Path(job.getWorkingDirectory, outDir)
          job.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, outDir.toString)
          TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job)
          //下面的注释掉，就不会出现这个目录已经存在的提示了
          /* if (fs.exists(outDir)) {
               throw new FileAlreadyExistsException("Outputdirectory"
                       + outDir + "alreadyexists");
           }
        }*/
        }
      }
    }

  }
}

