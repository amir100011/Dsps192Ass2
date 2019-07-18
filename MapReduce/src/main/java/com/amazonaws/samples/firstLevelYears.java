package com.amazonaws.samples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.amazonaws.samples.FirstStepKey;

public class firstLevelYears {

	public static class MapForWordCount extends Mapper<LongWritable, Text, FirstStepKey, IntWritable>{

		//	private Text firstWordKey = new Text(); //structure (key = (firstWord,decade) value = decade)
		//	private Text secondWordKey = new Text(); //structure (key = (firstWord,decade) value = numberOFApperances)
		//	private Text PairKey = new Text(); //structure (key = (firstWord,secondWord,decade) value = numberOFApperances)
		private int decade  = 0;
		private FirstStepKey initialKey = new FirstStepKey();

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
		{
			String line = nopunct(value.toString());
			StringTokenizer tokenizer = new StringTokenizer(line);

			String firstWord = tokenizer.toString();
			String secondWord = tokenizer.nextToken().toString();
			decade = (Integer.valueOf(tokenizer.nextToken().toString()))/10;
			IntWritable numberofAppearences = new IntWritable (Integer.valueOf(tokenizer.nextToken().toString()));
			//String numberofBooks = tokenizer.nextToken().toString();			


			initialKey.setFields(firstWord,secondWord, decade);
			//	PairKey.set(firstWord + "," + secondWord + "," + decade);
			output.write(initialKey, numberofAppearences);

			//
			//		//	PairKey.set(firstWord + "," + "*," + decade);
			//			initialKey.setFields(firstWord,"*", decade);
			//			output.write(initialKey, numberofAppearences);
			//
			//		//	PairKey.set(secondWord + "," + "+," + decade);
			//			initialKey.setFields(secondWord,"+", decade);
			//			output.write(initialKey, numberofAppearences);

		}
	}

	public static String nopunct(String s) {
		Pattern pattern = Pattern.compile("[^0-9 a-z A-Z]");
		Matcher matcher = pattern.matcher(s);
		String number = matcher.replaceAll(" ");
		return number;
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, FirstStepKey, IntWritable>{
		public void reduce(FirstStepKey word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		Path input=new Path("s3://amirtzurmapreduce/input2");
		Path output=new Path("s3://amirtzurmapreduce/output/");

		@SuppressWarnings("deprecation")
		Job job=new Job(conf,"firstLevelYears");
		job.setJarByClass(firstLevelYears.class);
		job.setMapperClass(MapForWordCount.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setOutputKeyClass(FirstStepKey.class);
		job.setMapOutputKeyClass(FirstStepKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}


