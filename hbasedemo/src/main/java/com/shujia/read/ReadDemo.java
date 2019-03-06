package com.shujia.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class ReadDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        //TableInputFormat.INPUT_TABLE 相当于hbase.mapreduce.inputtable
        //设置mapreduce 读取的表明
        configuration.set("hbase.mapreduce.inputtable", "wctbl");
        //将该值改大，防止hbase超时退出
        configuration.set("dfs.socket.timeout", "18000");

        //创建扫描器
        Scan scan = new Scan();
        //设置一个缓存
        scan.setCaching(1024);
        scan.setCacheBlocks(false);
        //设置开始rowkey和结束rowkey
        /*scan.setStartRow(Bytes.toBytes("73037041-AA"));
        scan.setStopRow(Bytes.toBytes("73037045-AA"));*/

        Job job = new Job(configuration, "ScanHbaseJob");

        //设置mapreduces读取hbase的map类和表明
        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("wctbl"), scan, MyMapper.class, NullWritable.class,Text.class, job);

        String outpath ="/wc/out/wc";
        //设置数据输出路径
        FileOutputFormat.setOutputPath(job, new Path(outpath));

        job.waitForCompletion(true);


    }
}
