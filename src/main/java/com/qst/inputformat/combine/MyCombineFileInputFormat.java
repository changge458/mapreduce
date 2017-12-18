package com.qst.inputformat.combine;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;


public class MyCombineFileInputFormat extends CombineFileInputFormat<NullWritable, BytesWritable> {

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		try {
			MyCombineRecordReader reader = new MyCombineRecordReader();
			reader.initialize(split, context);
			return reader;
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	
}
