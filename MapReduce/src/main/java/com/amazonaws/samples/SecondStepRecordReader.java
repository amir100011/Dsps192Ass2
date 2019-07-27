package com.amazonaws.samples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SecondStepRecordReader extends RecordReader<FirstStepKey, FirstStepValue> {

	FirstStepKey key;
	FirstStepValue value;
	LineRecordReader reader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
	}

	public SecondStepRecordReader() {
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
		key = new FirstStepKey(new Text(keyFields[0]), new Text(keyFields[1]), new IntWritable(Integer.parseInt(keyFields[2])));
		value = new FirstStepValue(new LongWritable(Integer.parseInt(valueFields[0])),new LongWritable(Integer.parseInt(valueFields[1])));
		return true;
	}

	@Override
	public FirstStepKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public FirstStepValue getCurrentValue() throws IOException, InterruptedException {
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
