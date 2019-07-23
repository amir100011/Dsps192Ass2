package com.amazonaws.samples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.regions.Regions;
import com.amazonaws.samples.FirstJob.CombinerClass;
import com.amazonaws.samples.FirstJob.MapperClass;
import com.amazonaws.samples.FirstJob.PartitionerClass;
import com.amazonaws.samples.FirstJob.ReducerClass;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class firstLevelYears {

	public static class MapForWordCount extends Mapper<LongWritable, Text, FirstStepKey, IntWritable>{
		
		private int decade  = 0;
		private FirstStepKey initialKey;

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
		{
			String line = nopunct(value.toString());
			StringTokenizer tokenizer = new StringTokenizer(line);


			while(tokenizer.hasMoreTokens()) {
				String firstWord = tokenizer.nextToken().toString();
				String secondWord = tokenizer.nextToken().toString();
				
				decade = (Integer.valueOf(tokenizer.nextToken().toString()))/10;
				IntWritable numberofAppearences = new IntWritable (Integer.valueOf(tokenizer.nextToken().toString()));	
				IntWritable Decade = new IntWritable(decade);

				initialKey = new FirstStepKey(firstWord,secondWord, Decade);
				//initialKey.setFields(firstWord,secondWord, decade);
				output.write(initialKey, numberofAppearences);


				initialKey = new FirstStepKey(firstWord,"*", Decade);
			//	initialKey.setFields(firstWord,"*", decade);
				output.write(initialKey, numberofAppearences);

				initialKey = new FirstStepKey("*",secondWord, Decade);
				//initialKey.setFields("*", secondWord, decade);
				output.write(initialKey, numberofAppearences);

				initialKey = new FirstStepKey("*","*", Decade);
				//initialKey.setFields("*", "*", decade);
				output.write(initialKey, numberofAppearences);

			}
		}
	}

	public static String nopunct(String s) {
		Pattern pattern = Pattern.compile("[^0-9 a-z A-Z]");
		Matcher matcher = pattern.matcher(s);
		String number = matcher.replaceAll(" ");
		return number;
	}

	public static class ReduceForWordCount extends Reducer<FirstStepKey, IntWritable, FirstStepKey, firstStepValue>{

		private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		private firstStepValue FSValue = new firstStepValue();
		private FirstStepKey FSKey;
		private long CW1;
		private boolean total ;
		private boolean secWord ;
		private boolean firstWord ;
		private boolean couple;
		int sum;

		@Override
		public void reduce(FirstStepKey Key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			//System.out.println(Key.getFirstWord() + " " + Key.getSecondWord() + " " + Key.getDecade());
			
			sum = 0;
			total = (Key.getFirstWord().toString().equals("*")) && (Key.getSecondWord().toString().equals("*"));
			secWord = (Key.getFirstWord().toString().equals("*")) && (!Key.getSecondWord().toString().equals("*"));
			firstWord = (!Key.getFirstWord().toString().equals("*") ) && (Key.getSecondWord().toString().equals("*"));
			couple = (!Key.getFirstWord().toString().equals("*") ) && (!Key.getSecondWord().toString().equals("*"));
			for(IntWritable value : values)
			{
				System.out.println(value);
				sum += value.get();
			}

			if (total) {
				String path = con.getConfiguration().get("tempFilesPath");
				String file = "";
				InputStream is = new ByteArrayInputStream( file.getBytes());
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(file.getBytes().length);
				String resultFileName = Key.getDecade().toString() + "@" + sum;
				PutObjectRequest req = new PutObjectRequest(path, resultFileName, is ,metadata);       
				s3.putObject(req);   
				
			}else if(secWord) {
				FSKey = new FirstStepKey(Key.getSecondWord().toString(), Key.getFirstWord().toString(), Key.getDecade());
				FSValue.setValues(sum, 0);
				con.write(FSKey,FSValue);
			}else if(firstWord) {
				CW1 = sum;
			}else if(couple) {
				FSValue.setValues(CW1, sum);
				con.write(Key,FSValue);
			}
		}
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		Path input=new Path("s3://amirtzurmapreduce/input.txt");
		Path output=new Path("s3://amirtzurmapreduce/output/");
		
		String tempFilesPath = "amirtzurmapreduce/N/";
		conf.set("tempFilesPath", tempFilesPath);

		@SuppressWarnings("deprecation")
		Job job=new Job(conf,"firstLevelYears");
		job.setJarByClass(firstLevelYears.class);
		job.setMapperClass(MapForWordCount.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setOutputKeyClass(FirstStepKey.class);
	//	job.setCombinerClass(CombinerClass.class);
		job.setMapOutputKeyClass(FirstStepKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);//SequenceFileInputFormat
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	 public static class PartitionerClass extends Partitioner<FirstStepKey,IntWritable> {
		 
			@Override
			public int getPartition(FirstStepKey key, IntWritable value, int num) {
				return Math.abs(key.getCode()) % num; 
			}  
	    }
	 

//		public static class CombinerClass extends Reducer<FirstJobKey,IntWritable,FirstJobKey,IntWritable> {
//
//			private IntWritable occurrences = new IntWritable();
//
//			@Override
//			public void reduce(FirstJobKey key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
//				int newOccurrences = 0;
//				for (IntWritable value : values) {
//					newOccurrences += value.get();
//				}
//				this.occurrences.set(newOccurrences);
//				context.write(key, this.occurrences);  
//			}
//		}
}




