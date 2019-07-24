package com.amazonaws.samples;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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


public class secondLevele {
	public static class MapForWordCount extends Mapper<FirstStepKey, firstStepValue, FirstStepKey, firstStepValue>{		
//no need to cmmit any action, the given <Key,Set> elements are already ready
		public void map(FirstStepKey key, firstStepValue value, Context con) throws IOException, InterruptedException
		{
			con.write(key, value);
		}
	}

	public static class ReduceForWordCount extends Reducer<FirstStepKey, firstStepValue, secondStepKey, LongWritable>{
		
		private HashMap<Integer,Long> decadeMap; // the growing rate of decade is a lot lower then that of the <Key,Value> pairs
		private long CW2; // counter for reset W2
		private long sumCw1; // counter for the first word
		private long sumCw1w2; // counter for the couples
		private int currentDecade; // the current decade of the couples we process 
		private long DecadeNpmi; // counter for the npmi of the entire couples in the decade  

		  @Override
		 public void setup(Reducer<FirstStepKey, firstStepValue, secondStepKey, LongWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
				currentDecade =-1; // initial value
				DecadeNpmi = 0; // initial value
				decadeMap = new HashMap<Integer,Long>(); // create the table of the N in each decade
				AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
				String path = context.getConfiguration().get("tempFilesPath");
		        ObjectListing olist = s3.listObjects("ass2-bucket", path);
		        for (S3ObjectSummary summary : olist.getObjectSummaries()) {
		        	String filePath = summary.getKey();
		        	String[] filePathArr = filePath.split("/");
		        	if(filePathArr.length > 1) {
		        		String filename = filePath.split("/")[2];
		 	            int decade = Integer.parseInt((filename.split(" ")[0]));
		 	            long occurrences = Long.parseLong((filename.split(" ")[1]));
		 	            decadeMap.put(decade, occurrences);
		        	}
		        }			
		}		  
		  
		@Override
		public void reduce(FirstStepKey Key, Iterable<firstStepValue> values, Context con) throws IOException, InterruptedException
		{	
			 
			sumCw1=0;
			sumCw1w2=0;
			//if we switch to another decade, send the total sum of the decade's npmi 
			if(Key.getDecade().get() != currentDecade && currentDecade!= -1) {				
				con.write(new secondStepKey("*", "*",currentDecade,DecadeNpmi), new LongWritable(DecadeNpmi));
				DecadeNpmi = 0;
			}			
			for(firstStepValue value : values) // sums the elements in the list of values for the current key 
			{
				sumCw1 += value.getCW1().get();
				sumCw1w2 += value.getCW1W2().get();
			}			
			if(Key.getSecondWord().toString().equals("*")) { // if we received a new second word counter 
				CW2 = sumCw1;
				sumCw1w2 = 0;
				currentDecade = Key.getDecade().get();
			}else { // if we received a couple to calculate it's npmi 
				long npmi = npmi(sumCw1,CW2,sumCw1w2,decadeMap.get(Key.getDecade().get())); //npmi calculating
				currentDecade = Key.getDecade().get();
				con.write(new secondStepKey(Key.getSecondWord().toString(), Key.getFirstWord().toString(),currentDecade,npmi), new LongWritable(npmi)); // switches the w1 and w2 to return to the original couple
				DecadeNpmi += npmi; // adding to the sum of decade's npmi
			}
		}
		@Override
		public void cleanup(Reducer<FirstStepKey, firstStepValue, secondStepKey, LongWritable>.Context context) throws IOException, InterruptedException {
			context.write(new secondStepKey("*", "*",currentDecade,DecadeNpmi), new LongWritable(DecadeNpmi));
		}
		
		public long npmi(long Cw1, long Cw2, long Cw1w2, long N ) {
			long pmi = (long) (Math.log(Cw1w2)+Math.log(N)-Math.log(Cw1) -Math.log(Cw1));
			long logPw1w2 = (long) -Math.log(Cw1w2/N); 
			return pmi/logPw1w2; 
		}
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		Path input=new Path("s3://amirtzurmapreduce/output/");
		Path output=new Path("s3://amirtzurmapreduce/output/Step2/");
		
		String tempFilesPath = "amirtzurmapreduce/N/";
		conf.set("tempFilesPath", tempFilesPath);

		@SuppressWarnings("deprecation")
		Job job=new Job(conf,"secondLevele");
		job.setJarByClass(secondLevele.class);
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





