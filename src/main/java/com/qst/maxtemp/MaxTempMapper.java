package com.qst.maxtemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//取出一行数据
		String line = value.toString();
		//取出年份
		String year = line.substring(15, 19);
		
		int airTemperature;
		
		//截取气温数值，如果有+，则截取加号后的数值作为气温值
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus
										// signs
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		//
		String quality = line.substring(92, 93);

		if (airTemperature != MISSING && quality.matches("[01459]")) {
			//向reduce中传输数据
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}

}
