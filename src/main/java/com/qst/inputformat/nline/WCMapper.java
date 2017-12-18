package com.qst.inputformat.nline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String nline = value.toString();
		System.out.println(nline);
		
		StringTokenizer st = new StringTokenizer(nline, "[ \n\r]");
	    while(st.hasMoreTokens()){
	    	String word = st.nextToken();
	    	context.write(new Text(word), new IntWritable(1));
	    }
		
	}
	
	
	

}
