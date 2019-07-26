package com.amazonaws.samples;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class step3InputFormat extends FileInputFormat<secondStepKey, DoubleWritable> {
	
	@Override
	public RecordReader<secondStepKey, DoubleWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new step3RecordReader();
	}

	@Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
          new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
      }
}