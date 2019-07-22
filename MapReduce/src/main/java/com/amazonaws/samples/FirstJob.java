package com.amazonaws.samples;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class FirstJob {
	public static class MapperClass extends Mapper<LongWritable,Text,FirstJobKey,LongWritable> {

		private Text firstWord = new Text();
		private Text secondWord = new Text();
		private LongWritable occurrences = new LongWritable();
		private IntWritable decade = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			// value format is : ngram TAB year TAB match_count TAB volume_count NEWLINE
			String valueSplited[] = value.toString().split("\t");
			String ngram[] = valueSplited[0].split(" ");
			if(ngram.length != 2) return;
			//Replacing all non-alphanumeric characters with empty strings
			String firstword = ngram[0].replaceAll("[^A-Za-z0-9]","").toLowerCase();
			String secondword = ngram[1].replaceAll("[^A-Za-z0-9]","").toLowerCase();
			this.firstWord.set(firstword);
			this.secondWord.set(secondword);
			//set occurrences
			occurrences.set(Long.parseLong(valueSplited[2]));
			//set decade
			int year = Integer.parseInt(valueSplited[1]);
			decade.set(((int)(year/10))*10);
			Text asterisk = new Text("*");
			// for each pair we wake 4 <kay,value>
			// we sort by: decade => first word => second word.
			// * is befor others
			// 1. key - (first word, second word, decade), value - occurrences
			// 2. key - (first word, *, decade), value - occurrences
			// 3. key - (*, second word, decade), value - occurrences
			// 4. key - (*, *, decade), value - occurrences
			context.write(new FirstJobKey(this.firstWord, this.secondWord, this.decade), this.occurrences);
			context.write(new FirstJobKey(this.firstWord, asterisk, this.decade), this.occurrences);
			context.write(new FirstJobKey(asterisk, this.secondWord, this.decade), this.occurrences);
			context.write(new FirstJobKey(asterisk, asterisk, this.decade), this.occurrences);
		}
	}

	public static class ReducerClass extends Reducer<FirstJobKey,LongWritable,FirstJobKey,FirstJobValue> {

		private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		private LongWritable firstWordOccurrences = new LongWritable();
		private LongWritable occurrences = new LongWritable();

		@Override
		public void reduce(FirstJobKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			long acc = 0;
			Text asterisk = new Text("*");
			for (LongWritable value : values) {
				acc += value.get();
			}
			this.occurrences.set(acc);
			if(key.getFirstWord().equals(asterisk) && key.getSecondWord().equals(asterisk)) {
				String path = context.getConfiguration().get("nPath");
				String file = "";
				InputStream is = new ByteArrayInputStream( file.getBytes());
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(file.getBytes().length);
				String resultFileName = key.getDecade().toString() + "@" + acc;
				PutObjectRequest req = new PutObjectRequest(path, resultFileName, is ,metadata);       
				s3.putObject(req);  
			} else if(key.getFirstWord().equals(asterisk)) {
				// key is {*,word2, deacade}
				// we need to send {word2,*,decace} for the next MapReduce job
				FirstJobKey newKey = new FirstJobKey(key.getSecondWord(),asterisk, key.getDecade());
				FirstJobValue value = new FirstJobValue(new LongWritable(0), this.occurrences);
				context.write(newKey,value);
			} else if(key.getSecondWord().equals(asterisk)) {
				//key is {word1, *}
				//first word occurrences (c(w1)) get before the pair KV - we need to save the value for adding KV
				this.firstWordOccurrences.set(acc);
			} else {
				// key is {word1,word2, decacde}
				// we got first word occurrences
				// next key {word2,word1, decade} for right recieive order is second Job (get after {word2,*}) 
				FirstJobKey newKey = new FirstJobKey(key.getSecondWord(),key.getFirstWord(), key.getDecade());
				FirstJobValue value = new FirstJobValue(this.occurrences, this.firstWordOccurrences);
				context.write(newKey, value);
			}
		}
	}

	public static class CombinerClass extends Reducer<FirstJobKey,LongWritable,FirstJobKey,LongWritable> {

		private LongWritable occurrences = new LongWritable();

		@Override
		public void reduce(FirstJobKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			long newOccurrences = 0;
			for (LongWritable value : values) {
				newOccurrences += value.get();
			}
			this.occurrences.set(newOccurrences);
			context.write(key, this.occurrences);  
		}
	}

	public static class PartitionerClass extends Partitioner<FirstJobKey,LongWritable> {

		@Override
		public int getPartition(FirstJobKey key, LongWritable value, int numPartitions) {
			return Math.abs(key.getCode()) % numPartitions;
		}

	}


	public static void main(String[] args) throws Exception {
		System.out.println("===========================================");
		System.out.println("                First Job                  ");
		System.out.println("===========================================");

		String inputFilePath = "s3://amirtzurmapreduce/input.txt";
		String output = "s3://amirtzurmapreduce/output/";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "firstJob");
		job.setJarByClass(FirstJob.class);
		job.setMapperClass(MapperClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(CombinerClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(FirstJobKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(FirstJobKey.class);
		job.setOutputValueClass(FirstJobValue.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFilePath));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		while(true);
	}
}