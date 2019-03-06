package com.shujia.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WCJob {

    /**
     * 往hbase写入数据
     *
     * @param args
     * @throws Exception
     */

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(WCJob.class);
		job.setJobName("word count");

		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//设置hbase输出reduce类
		TableMapReduceUtil.initTableReducerJob("wctbl", WCReducer.class, job, null, null, null, null, false);
		
		Path inputPath = new Path("/wc/input/wc");
		FileInputFormat.addInputPath(job, inputPath);
		
		
		Boolean flag = job.waitForCompletion(true);
		if(flag) {
			System.out.println("wc run success!!");
		}
	}

}
