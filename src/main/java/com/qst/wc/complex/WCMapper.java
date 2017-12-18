package com.qst.wc.complex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] arr = line.split(" ");
		
		for(String word :arr){
			
			context.write(new Text(word), new IntWritable(1));
		}
		
	}
	
	

}
