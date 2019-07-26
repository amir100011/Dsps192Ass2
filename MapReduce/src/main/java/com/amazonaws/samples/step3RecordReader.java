package com.amazonaws.samples;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class step3RecordReader extends RecordReader<secondStepKey, DoubleWritable> {

	secondStepKey key;
	DoubleWritable value;
	LineRecordReader reader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
	}

	public step3RecordReader() {
		reader = new LineRecordReader(); 
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!reader.nextKeyValue()) {
			return false;
		}
		String line = reader.getCurrentValue().toString();
		if(line.length()==0)
			return false;
		String[] keyValue = line.split("\t");
		String[] keyFields = keyValue[0].split(" ");
		String[] valueFields = keyValue[1].split(" ");
		key = new secondStepKey(keyFields[0],keyFields[1], Integer.parseInt(keyFields[2]),Double.parseDouble(keyFields[3]));
		value = new DoubleWritable(Double.parseDouble(valueFields[0])); 
		return true;
	}

	@Override
	public secondStepKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public DoubleWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void close() throws IOException {
		reader.close();

	}

}