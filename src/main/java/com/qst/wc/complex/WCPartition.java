package com.qst.wc.complex;

import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPartition extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		
		Random r = new Random();
		return r.nextInt(numPartitions);
		
//		if(numPartitions == 1){
//			return 0;
//		}
//		
//		return (key.hashCode() & Integer.MAX_VALUE) % (numPartitions / 2 ) ;

//		try {
//			Float.parseFloat(key.toString());
//			return 0;
//		} catch (Exception e) {
//			return 1;
//		}
		
		
	}
}
