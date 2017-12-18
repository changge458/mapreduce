package com.qst.inputformat.wholefile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values ,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		//hello [1,1]
		Integer num = 0; 
		
		for( IntWritable value : values){
			num += value.get();
		}
		
		context.write(key, new IntWritable(num));
	}
	
}
