package com.shujia.wc;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		//统计每个单词数量
		for (IntWritable i : iterable) {
			sum += i.get();
		}

		//text.getBytes() rowkey
		Put put = new Put(text.getBytes());
		//插入数据
		put.add("cf1".getBytes(), "count".getBytes(), String.valueOf(sum).getBytes());

		//将数据插入到hbase表
		context.write(null, put);
	}

}