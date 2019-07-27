package com.amazonaws.samples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


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


public class FirstLevel {

	public static class MapForWordCount extends Mapper<LongWritable, Text, FirstStepKey, LongWritable>{

		private int decade  = 0;
		private FirstStepKey initialKey;

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
		{
			// value format is : ngram TAB year TAB match_count TAB volume_count NEWLINE
			String valueSplited[] = value.toString().split("\t");
			String ngram[] = valueSplited[0].split(" ");
			if(ngram.length != 2) return;
			//Replacing all non-alphanumeric characters with empty strings
			String firstWord = ngram[0].replaceAll("[^A-Za-z0-9]","").toLowerCase();
			String secondWord = ngram[1].replaceAll("[^A-Za-z0-9]","").toLowerCase();
			IntWritable Decade = new IntWritable(((Integer.parseInt(valueSplited[1]))/10)*10);
			LongWritable numberofAppearences = new LongWritable (Long.parseLong(valueSplited[2]));	

			initialKey = new FirstStepKey(firstWord,secondWord, Decade);
			output.write(initialKey, numberofAppearences);


			initialKey = new FirstStepKey(firstWord,"*", Decade);
			output.write(initialKey, numberofAppearences);

			initialKey = new FirstStepKey("*",secondWord, Decade);
			output.write(initialKey, numberofAppearences);

			initialKey = new FirstStepKey("*","*", Decade);
			output.write(initialKey, numberofAppearences);

		}
	}


	public static class ReduceForWordCount extends Reducer<FirstStepKey, LongWritable, FirstStepKey, FirstStepValue>{

		private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		private FirstStepValue FSValue = new FirstStepValue();
		private FirstStepKey FSKey;
		private long CW1;
		private boolean total ;
		private boolean secWord ;
		private boolean firstWord ;
		private boolean couple;
		int sum;

		@Override
		public void reduce(FirstStepKey Key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
		{

			sum = 0;
			total = (Key.getFirstWord().toString().equals("*")) && (Key.getSecondWord().toString().equals("*"));
			secWord = (Key.getFirstWord().toString().equals("*")) && (!Key.getSecondWord().toString().equals("*"));
			firstWord = (!Key.getFirstWord().toString().equals("*") ) && (Key.getSecondWord().toString().equals("*"));
			couple = (!Key.getFirstWord().toString().equals("*") ) && (!Key.getSecondWord().toString().equals("*"));
			for(LongWritable value : values)
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
				PutObjectRequest req = new PutObjectRequest(path, Key.getDecade().toString() + " " + sum, is ,metadata);       
				s3.putObject(req);   

			}else if(secWord) {
				FSKey = new FirstStepKey(Key.getSecondWord().toString(), Key.getFirstWord().toString(), Key.getDecade());
				FSValue.setValues(sum, 0);
				con.write(FSKey,FSValue);
			}else if(firstWord) {
				CW1 = sum;
			}else if(couple) {
				FSValue.setValues(CW1, sum);
				con.write(new FirstStepKey(Key.getSecondWord().toString(),Key.getFirstWord().toString(),Key.getDecade()),FSValue);
			}
		}
	}

	public static class PartitionerClass extends Partitioner<FirstStepKey,LongWritable> {

		@Override
		public int getPartition(FirstStepKey key, LongWritable value, int num) {
			return Math.abs(key.getCode()) % num; 
		}  
	}


	public static class CombinerClass extends Reducer<FirstStepKey,LongWritable,FirstStepKey,LongWritable> {

		private LongWritable occurrences = new LongWritable();

		@Override
		public void reduce(FirstStepKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			long newOccurrences = 0;
			for (LongWritable value : values) {
				newOccurrences += value.get();
			}
			this.occurrences.set(newOccurrences);
			context.write(key, this.occurrences);  
		}
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		Path input=new Path(args[0]);
		Path output=new Path("s3://amirtsurmapreduce/FirstLevelOutput/");

		String uuid = args[1];

		String tempFilesPath = "amirtsurmapreduce/tempFiles/" + uuid;
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf,"FirstLevel");
		job.setJarByClass(FirstLevel.class);
		job.setMapperClass(MapForWordCount.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setOutputKeyClass(FirstStepKey.class);
		job.setCombinerClass(CombinerClass.class);
		job.setMapOutputKeyClass(FirstStepKey.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);//TextInputFormat
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);

	}
}




