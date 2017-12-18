package com.qst.inputformat.combine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {

	private static final int MISSING = 9999;

	@Override
	protected void map(NullWritable key, BytesWritable value,
			Mapper<NullWritable, BytesWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 将字节数组转化为String
		byte[] b = value.copyBytes();
		String str = new String(b);

		// 使用BufferedReader的读行技术
		StringReader sr = new StringReader(str);
		BufferedReader br = new BufferedReader(sr);

		String line = null;

		while ((line = br.readLine()) != null) {
			// 取出年份
			String year = line.substring(15, 19);
			
			int airTemperature;
			
			// 截取气温数值，如果有+，则截取加号后的数值作为气温值
			if (line.charAt(87) == '+') { // parseInt doesn't like leading plus
				// signs
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
			//
			String quality = line.substring(92, 93);
			
			if (airTemperature != MISSING && quality.matches("[01459]")) {
				// 向reduce中传输数据
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}

}
