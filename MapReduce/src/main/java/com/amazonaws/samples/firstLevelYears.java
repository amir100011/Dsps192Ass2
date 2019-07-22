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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class firstLevelYears {

	public static class MapForWordCount extends Mapper<LongWritable, Text, FirstStepKey, IntWritable>{

		//	private Text firstWordKey = new Text(); //structure (key = (firstWord,decade) value = decade)
		//	private Text secondWordKey = new Text(); //structure (key = (firstWord,decade) value = numberOFApperances)
		//	private Text PairKey = new Text(); //structure (key = (firstWord,secondWord,decade) value = numberOFApperances)
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
				//String numberofBooks = tokenizer.nextToken().toString();			


				initialKey = new FirstStepKey(firstWord,secondWord, decade);
				//initialKey.setFields(firstWord,secondWord, decade);
				//	PairKey.set(firstWord + "," + secondWord + "," + decade);
				output.write(initialKey, numberofAppearences);


				//	PairKey.set(firstWord + "," + "*," + decade);
				initialKey = new FirstStepKey(firstWord,"*", decade);
			//	initialKey.setFields(firstWord,"*", decade);
				output.write(initialKey, numberofAppearences);

				//	PairKey.set(* + "," + "secondWord," + decade);
				initialKey = new FirstStepKey("*", secondWord, decade);
				//initialKey.setFields("*", secondWord, decade);
				output.write(initialKey, numberofAppearences);

				initialKey = new FirstStepKey("*", "*", decade);
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

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, FirstStepKey, firstStepValue>{

		private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		private firstStepValue FSValue = new firstStepValue();
		private FirstStepKey FSKey = new FirstStepKey();
		private long CW1;
		private boolean total ;
		private boolean secWord ;
		private boolean firstWord ;
		private boolean couple;
		int sum;

		public void reduce(FirstStepKey Key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			System.out.println(Key.getFirstWord() + " " + Key.getSecondWord() + " " + Key.getDecade());
			
			sum = 0;
			total = (Key.getFirstWord().toString() == "*" ) && (Key.getSecondWord().toString() == "*");
			secWord = (Key.getFirstWord().toString() == "*" ) && (Key.getSecondWord().toString() != "*");
			firstWord = (Key.getFirstWord().toString() != "*" ) && (Key.getSecondWord().toString() == "*");
			couple = (Key.getFirstWord().toString() != "*" ) && (Key.getSecondWord().toString() != "*");
			for(IntWritable value : values)
			{
				System.out.println(value);
				sum += value.get();
			}

			if (total) {
				String path = con.getConfiguration().get("nPath");
				String file = "";
				InputStream is = new ByteArrayInputStream( file.getBytes());
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(file.getBytes().length);
				String resultFileName = Key.getDecade().toString() + "@" + sum;
				PutObjectRequest req = new PutObjectRequest(path, resultFileName, is ,metadata);       
				s3.putObject(req);      
			}else if(secWord) {
				FSKey.setFields(Key.getSecondWord().toString(), Key.getFirstWord().toString(), Key.getDecade().get());
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

		@SuppressWarnings("deprecation")
		Job job=new Job(conf,"firstLevelYears");
		job.setJarByClass(firstLevelYears.class);
		job.setMapperClass(MapForWordCount.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setOutputKeyClass(FirstStepKey.class);
		job.setMapOutputKeyClass(FirstStepKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);//SequenceFileInputFormat
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	 public static class PartitionerClass extends Partitioner<FirstStepKey,LongWritable> {
		 
			@Override
			public int getPartition(FirstStepKey key, LongWritable value, int num) {
				return Math.abs(key.getCode()) % num; 
			}  
	    }
}




