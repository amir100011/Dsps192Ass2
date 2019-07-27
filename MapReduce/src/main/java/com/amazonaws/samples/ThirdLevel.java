package com.amazonaws.samples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;



public class ThirdLevel {

	public static class MapForWordCount extends Mapper<SecondStepKey, DoubleWritable, SecondStepKey, DoubleWritable>{		
		//no need to commit any action, the given <Key,Set> elements are already ready
		public void map(SecondStepKey key, DoubleWritable  value, Context con) throws IOException, InterruptedException
		{
			con.write(key, value);
		}
	}

	public static class ReduceForWordCount extends Reducer<SecondStepKey, DoubleWritable, Text, Text>{

		private double decadeNpmi = 0; // to sum all nmpi of couples in a certain decade 
		private double coupleNpmi = 0; // to sum all nmpi of a certain couple
		private double minPmi;
		private double relMinPmi;
		Text keyText;
		Text npmiText;

		public void setup(Context con) {
			this.minPmi = Double.parseDouble(con.getConfiguration().get("minPmi","0.5"));
			this.relMinPmi = Double.parseDouble(con.getConfiguration().get("relMinPmi","0.2"));
		}

		public void reduce(SecondStepKey Key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{


			coupleNpmi = 0;
			// sums the elements in the list of values for the current key
			for(DoubleWritable value : values)  
			{
				coupleNpmi += value.get();
			}		
			// a decade npmi sum
			if(Key.getFirstWord().toString().equals("*")) { 
				decadeNpmi = coupleNpmi;
			//	con.write(keyText = new Text(Key.getFirstWord().toString()+ " " + Key.getSecondWord().toString()+ " " + Key.getDecade().toString() + " " + Key.getNpmi().toString()),new Text(Double.toString(decadeNpmi)));
			}else {
				double mResult = coupleNpmi;
				double rResult = coupleNpmi/decadeNpmi;				
				if(mResult >= this.minPmi || rResult >= this.relMinPmi) {
					keyText = new Text(Key.getFirstWord().toString()+ " " + Key.getSecondWord().toString()+ " " + Key.getDecade().toString());
					npmiText = new Text("npmi: " + mResult +" relative npmi: " + rResult);
					con.write(keyText,npmiText);
				}				
			}			
		}
	}

	public static class CombinerClass extends Reducer<SecondStepKey, DoubleWritable, SecondStepKey, DoubleWritable> {

		@Override
		public void reduce(SecondStepKey key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			con.write(key, new DoubleWritable(sum));   	
		}
	}

	public static class PartitionerClass extends Partitioner<SecondStepKey,DoubleWritable> {

		@Override 
		public int getPartition(SecondStepKey key, DoubleWritable value, int num) {
			return Math.abs(key.getCode()) % num; 
		}  

	}
	public static void main(String [] args) throws Exception
	{
		
		String minPmi = args[0];
		String relMinPmi = args[1];
		System.out.println(args[0]);
		System.out.println(args[1]);
		Configuration conf=new Configuration();
		conf.set("minPmi", minPmi);
		conf.set("relMinPmi",relMinPmi);
		Path input=new Path("s3://amirtsurmapreduce/SecondStepOutput/");
		Path output=new Path("s3://amirtsurmapreduce/ThirdStepOutput/");

	
		
		Job job = Job.getInstance(conf, "ThirdLevel");

		job.setJarByClass(ThirdLevel.class);
		job.setMapperClass(MapForWordCount.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setMapOutputKeyClass(SecondStepKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setCombinerClass(CombinerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(ThirsStepInputFormat.class);
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
		while(true);
	}
}