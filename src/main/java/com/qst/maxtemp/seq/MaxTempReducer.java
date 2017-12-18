package com.qst.maxtemp.seq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		// 定义maxValue为int最小值
		int maxValue = Integer.MIN_VALUE;
		
		//通过循环比较赋值，产生最大气温值
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		//向输出写入
		context.write(key, new IntWritable(maxValue));
	}

}
