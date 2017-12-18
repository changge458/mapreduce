package com.qst.inputformat.seqfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class CreateSeq {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("D:/seq/1.seq");
		
		Writer w = SequenceFile.createWriter(fs, conf, p, Text.class, IntWritable.class);
		
		w.append(new Text("tom"), new IntWritable(3));
		w.append(new Text("tomas"), new IntWritable(2));
		w.append(new Text("tomson"), new IntWritable(1));
		w.append(new Text("tomasLee"), new IntWritable(4));
		w.append(new Text("tomason"), new IntWritable(6));
		w.append(new Text("tom1"), new IntWritable(100));
		w.append(new Text("tom2"), new IntWritable(13));
		w.append(new Text("tom3"), new IntWritable(14));
		w.append(new Text("tom4"), new IntWritable(16));
		w.append(new Text("tom5"), new IntWritable(17));
		w.append(new Text("tom6"), new IntWritable(18));
		w.append(new Text("tom"), new IntWritable(19));
		w.append(new Text("tomas"), new IntWritable(12));
		w.append(new Text("tomson"), new IntWritable(14));
		w.append(new Text("tomasLee"), new IntWritable(51));
		w.append(new Text("tomason"), new IntWritable(16));
		w.append(new Text("tom1"), new IntWritable(17));
		w.append(new Text("tom2"), new IntWritable(18));
		w.append(new Text("tom3"), new IntWritable(19));
		w.append(new Text("tom4"), new IntWritable(10));
		w.append(new Text("tom5"), new IntWritable(1));
		w.append(new Text("tom6"), new IntWritable(1));
		
		w.close();

	}

}
