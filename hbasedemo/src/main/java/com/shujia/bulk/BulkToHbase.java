package com.shujia.bulk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BulkToHbase {
    static Logger logger = LoggerFactory.getLogger(com.shujia.bulk.BulkToHbase.class);

    //map段函数，reduce段函数hbase自带，可以不写
    public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //把数据分割
            String[] valueStrSplit = value.toString().split("\t");
            //这里设置rowkey，用annualreport_id_id做rowkey
            String hkey = valueStrSplit[0];

            //转换一下类型，输出key类型是ImmutableBytesWritable
            final byte[] rowKey = Bytes.toBytes(hkey);
            final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);

            // byte[] cell = Bytes.toBytes(hvalue);
            // HPut.add(Bytes.toBytes(family), Bytes.toBytes(column), cell);
            //KeyValue kv = new KeyValue(rowKey, Bytes.toBytes(family), Bytes.toBytes(column1),Bytes.toBytes(hvalue1));

            //设置列和列值，输出value类型是put（也可以是KeyValue等格式）
            Put put = new Put(Bytes.toBytes(hkey));   //ROWKEY
            //put.addColumn("info".getBytes(), "company_id".getBytes(), valueStrSplit[0].getBytes());
            put.addColumn("info".getBytes(), "category_code".getBytes(), valueStrSplit[1].getBytes());
            put.addColumn("info".getBytes(), "status".getBytes(), valueStrSplit[2].getBytes());
            put.addColumn("info".getBytes(), "updatetime".getBytes(), valueStrSplit[3].getBytes());

            //结果传给reduce端
            context.write(HKey, put);
        }
    }

    public static void main(String[] args) throws Exception {

        //设置hbase的表名
        String name = "company_category_20170411";
        String tablename = "prism:"+name;

        //设置hbase
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4");

        //String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String inputPath = dfsArgs[0];

        //设置hdfs的文件位置和输出文件位置
        String inputPath = "hdfs://192.168.0.206/prism/old/"+name;
        System.out.println("source: " + inputPath);
        //String outputPath = dfsArgs[1];
        String outputPath = "hdfs://192.168.0.206/test/"+name;
        System.out.println("dest: " + outputPath);
        HTable hTable = null;
        try {
            Job job = Job.getInstance(conf, "Test Import HFile & Bulkload");
            //设置jar包
            job.setJarByClass(com.shujia.bulk.BulkToHbase.class);
            //指定map类
            job.setMapperClass(com.shujia.bulk.BulkToHbase.BulkLoadMap.class);
            //设置maps输出的key类型和value类型
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            // speculation
            job.setSpeculativeExecution(false);
            job.setReduceSpeculativeExecution(false);
            // in/out format
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            hTable = new HTable(conf, tablename);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable);

            if (job.waitForCompletion(true)) {
                FsShell shell = new FsShell(conf);
                try {
                    shell.run(new String[] { "-chmod", "-R", "777", outputPath });
                } catch (Exception e) {
                    logger.error("Couldnt change the file permissions ", e);
                    //throw new IOException(e);
                }
                // 加载到hbase表
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                // 两种方式都可以
                // 方式一
                String[] loadArgs = { outputPath, tablename };
                loader.run(loadArgs);
                // 方式二
                // loader.doBulkLoad(new Path(outputPath), hTable);
            } else {
                logger.error("loading failed.");
                System.exit(1);
            }

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            if (hTable != null) {
                hTable.close();
            }
        }
    }
}