package com.amazonaws.samples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;



public class thirdStep {

	public static class MapForWordCount extends Mapper<secondStepKey, DoubleWritable, secondStepKey, DoubleWritable>{		
		//no need to cmmit any action, the given <Key,Set> elements are already ready
		public void map(secondStepKey key, DoubleWritable  value, Context con) throws IOException, InterruptedException
		{
			con.write(key, value);
		}
	}

	public static class ReduceForWordCount extends Reducer<secondStepKey, DoubleWritable, Text, Text>{

		private double decadeNpmi=0; // to sum all nmpi of couples in a certain decade 
		private double coupleNpmi=0; // to sum all nmpi of a certain couple
		private double minPmi;
		private double relMinPmi;
		Text keyText;
		Text npmiText;

		public void setup(Context context) {
			this.minPmi = 0.5;//Double.parseDouble(context.getConfiguration().get("minPmi"));
			this.relMinPmi = 0.2; // = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
		}

		public void reduce(secondStepKey Key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{


			coupleNpmi = 0;
			for(DoubleWritable value : values) // sums the elements in the list of values for the current key 
			{
				coupleNpmi += value.get();
			}			
			if(Key.getFirstWord().toString().equals("*")) { // a decade npmi sum
				decadeNpmi = coupleNpmi;
				con.write(keyText = new Text(Key.getFirstWord().toString()+ " " + Key.getSecondWord().toString()+ " " + Key.getDecade().toString() + " " + Key.getNpmi().toString()),new Text(Double.toString(decadeNpmi)));
			}else {
				double mResult = coupleNpmi;
				double rResult = coupleNpmi/decadeNpmi;				
				if(mResult >= minPmi || rResult>= relMinPmi) {
					keyText = new Text(Key.getFirstWord().toString()+ " " + Key.getSecondWord().toString()+ " " + Key.getDecade().toString());
					npmiText = new Text("npmi: " + mResult +" relative npmi: " + rResult);
					con.write(keyText,npmiText);
					//con.write(keyText,new Text(Double.toString(decadeNpmi)));



				}				
			}			
		}
	}

	public static class CombinerClass extends Reducer<secondStepKey, DoubleWritable, secondStepKey, DoubleWritable> {

		@Override
		public void reduce(secondStepKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));   	
		}
	}

	public static class PartitionerClass extends Partitioner<secondStepKey,DoubleWritable> {

		@Override 
		public int getPartition(secondStepKey key, DoubleWritable value, int num) {
			return Math.abs(key.getCode()) % num; 
		}  

	}
	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		Path input=new Path("s3://amirtzurmapreduce/outputStep2/");
		Path output=new Path("s3://amirtzurmapreduce/outputStep3/");

		Job job = Job.getInstance(conf, "thirdStep");
		job.setJarByClass(thirdStep.class);
		job.setMapperClass(MapForWordCount.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setMapOutputKeyClass(secondStepKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setCombinerClass(CombinerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(step3InputFormat.class);//SequenceFileInputFormat
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
		while(true);
	}
}