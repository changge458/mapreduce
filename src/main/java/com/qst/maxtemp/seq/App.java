package com.qst.maxtemp.seq;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		//获取job实例
		Job job = Job.getInstance();
		//定义入口函数所在的类
		job.setJarByClass(App.class);
		//给job起名
		job.setJobName("Max temperature");
		//设置输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//设置map和reduce的类
		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		//设置reduce的输出kv
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//开始执行作业
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
