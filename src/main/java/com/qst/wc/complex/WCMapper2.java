package com.qst.wc.complex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] arr = line.split("\t");
		
		String word = arr[0];
		
		Integer num = Integer.parseInt(arr[1]);	
				
		context.write(new Text(word), new IntWritable(num));
		
	}
	
	

}