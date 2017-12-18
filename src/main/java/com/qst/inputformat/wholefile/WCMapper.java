package com.qst.inputformat.wholefile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {


	@Override
	protected void map(NullWritable key, BytesWritable value, Mapper<NullWritable, BytesWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//将字节数组转化为String
		byte[] b = value.copyBytes();
		String str = new String(b);
		
		//使用BufferedReader的读行技术
		StringReader sr = new StringReader(str);
		BufferedReader br = new BufferedReader(sr);
		
		String line = null ;
		
		while((line = br.readLine()) != null){
			String[] arr = line.split(" ");
			for(String word : arr){
				context.write(new Text(word), new IntWritable(1));
				
			}
		}
		
		
		
		
		
	}
	
	
	

}
