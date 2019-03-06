package com.shujia.bulk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BulkLoadJob {
    static Logger logger = LoggerFactory.getLogger(BulkLoadJob.class);

    public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] valueStrSplit = value.toString().split(",");
            String hkey = valueStrSplit[0];

            //String hvalue6 = valueStrSplit[5];
            final byte[] rowKey = Bytes.toBytes(hkey);
            final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
            // Put HPut = new Put(rowKey);
            // byte[] cell = Bytes.toBytes(hvalue);
            // HPut.add(Bytes.toBytes(family), Bytes.toBytes(column), cell);
            //KeyValue kv = new KeyValue(rowKey, Bytes.toBytes(family), Bytes.toBytes(column1),Bytes.toBytes(hvalue1));
            Put put = new Put(Bytes.toBytes(valueStrSplit[0]));   //ROWKEY
            put.addColumn("info".getBytes(), "name".getBytes(), valueStrSplit[1].getBytes());
            put.addColumn("info".getBytes(), "age".getBytes(), valueStrSplit[2].getBytes());
            put.addColumn("info".getBytes(), "gender".getBytes(), valueStrSplit[3].getBytes());
            put.addColumn("info".getBytes(), "class".getBytes(), valueStrSplit[4].getBytes());

            context.write(HKey, put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String tablename = "students";
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "master,node1,node2,node3,node4");
        //conf.set("hbase.master", "master:50070");
        //String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String inputPath = dfsArgs[0];
        String inputPath = "hdfs://192.168.0.206/test";
        System.out.println("source: " + inputPath);
        //String outputPath = dfsArgs[1];
        String outputPath = "hdfs://192.168.0.206/test/output";
        System.out.println("dest: " + outputPath);
        HTable hTable = null;
        try {
            Job job = Job.getInstance(conf, "Test Import HFile & Bulkload");
            job.setJarByClass(BulkLoadJob.class);
            job.setMapperClass(BulkLoadJob.BulkLoadMap.class);
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