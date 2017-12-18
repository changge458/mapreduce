package com.qst.wc.simple;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

	public static void main(String[] args) throws Exception {

		//System.setProperty("HADOOP_USER_NAME", "centos");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// mapreduce.job.jar
		job.setJarByClass(App.class);

		// mapreduce.job.name
		job.setJobName("word count");

		// 设置输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		

		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置输出kv
		// mapreduce.job.output.key.class
		// mapreduce.job.output.value.class
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		

		// 指定map和reduce的类
		// mapreduce.job.map.class
		// mapreduce.job.reduce.class
		job.setMapperClass(WCMapper.class);
		
		
		//job.setCombinerClass(WCCombiner.class);
		job.setReducerClass(WCReducer.class);

		// Iterator<Entry<String, String>> it = conf.iterator();
		// while(it.hasNext()){
		// Entry<String, String> entry = it.next();
		// System.out.println(entry.getKey() + " = " + entry.getValue());
		// }
		
		//FileInputFormat.setMaxInputSplitSize(job, 200);
		//FileInputFormat.setMinInputSplitSize(job, 500);
		System.exit((job.waitForCompletion(true) ? 0 : 1));
	}

}
