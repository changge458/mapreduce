package com.qst.inputformat.seqfile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<Text, IntWritable, Text, IntWritable> {


	@Override
	protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(key, value);
		
	}

}
