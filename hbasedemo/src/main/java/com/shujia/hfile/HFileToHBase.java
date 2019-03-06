package com.shujia.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 生成hfile文件
 *
 */
public class HFileToHBase {
    public static class HFileToHBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split("\t");

            //构建rowkey
            byte[] rowkey = values[0].getBytes();
            ImmutableBytesWritable k = new ImmutableBytesWritable(rowkey);

            //cf1：列族，count:列名
            KeyValue kvProtocol = new KeyValue(rowkey, "cf1".getBytes(), "count".getBytes(), values[1].getBytes());

            context.write(k, kvProtocol);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "TestHFileToHBase");
        job.setJarByClass(HFileToHBase.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        //增加自定义mapper类
        job.setMapperClass(HFileToHBaseMapper.class);
        //KeyValueSortReducer  将数据rowkey进行排序
        job.setReducerClass(KeyValueSortReducer.class);

        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        //获取table的元数据
        HTable table = new HTable(conf, "hua");
        HFileOutputFormat.configureIncrementalLoad(job, table);

        FileInputFormat.addInputPath(job, new Path("/wc/out/wc"));
        FileOutputFormat.setOutputPath(job, new Path("/wc/hbase/wc"));

        job.waitForCompletion(true);
    }
}
