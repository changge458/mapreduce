package com.qst.inputformat.combine;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class MyCombineRecordReader extends RecordReader<NullWritable, BytesWritable> {
	private CombineFileSplit fileSplit;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private boolean processed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (CombineFileSplit) split;
		this.conf = context.getConfiguration();
	}

	//TODO
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			int i = 0;
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path[] files = fileSplit.getPaths();
			for(Path file : files){
				byte[] content = new byte[(int)new File(file.toString()).length()];
				FileSystem fs = file.getFileSystem(conf);
				FSDataInputStream in = null;
				try {
					in = fs.open(file);
					IOUtils.readFully(in, contents, 0, content.length );
					value.set(contents, 0, contents.length);
					i++;
				} finally {
					IOUtils.closeStream(in);
				}
				processed = true;
				return true;
			}
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}
}