package com.amazonaws.samples;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

	public static class ReduceForWordCount extends Reducer<FirstStepKey, firstStepValue, secondStepKey, DoubleWritable>{

		private HashMap<Integer,Long> decadeMap; // the growing rate of decade is a lot lower then that of the <Key,Value> pairs
		private long CW2; // counter for reset W2
		private long sumCw1; // counter for the first word
		private long sumCw1w2; // counter for the couples
		private int currentDecade = -1; // the current decade of the couples we process 
		private double DecadeNpmi = 0; // counter for the npmi of the entire couples in the decade  

		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			decadeMap = new HashMap<Integer,Long>(); // create the table of the N in each decade
			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
			String path = context.getConfiguration().get("tempFilesPath");
			ObjectListing olist = s3.listObjects("amirtzurmapreduce", path);
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
				con.write(new secondStepKey("*", "*",currentDecade,DecadeNpmi), new DoubleWritable(DecadeNpmi));
				DecadeNpmi = 0;
				currentDecade = Key.getDecade().get();
				
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
				long N = decadeMap.get(Key.getDecade().get());
				double pmi = pmi(sumCw1, CW2, sumCw1w2,N); //npmi calculating
				double normalizer = normalizer(sumCw1w2,N);
				double logPw1w2 = Math.log10(normalizer); 
				double npmi = pmi/(-logPw1w2); //log(1/x) = -log(x)
				currentDecade = Key.getDecade().get();
				con.write(new secondStepKey(Key.getSecondWord().toString(), Key.getFirstWord().toString(), currentDecade, npmi), new DoubleWritable(npmi)); // switches the w1 and w2 to return to the original couple
				DecadeNpmi += npmi; // adding to the sum of decade's npmi
			}
		}
		@Override
		public void cleanup(Reducer<FirstStepKey, firstStepValue, secondStepKey, DoubleWritable>.Context context) throws IOException, InterruptedException {
			context.write(new secondStepKey("*", "*",currentDecade,DecadeNpmi), new DoubleWritable(DecadeNpmi));
		}

		public double pmi(double Cw1, double Cw2, double Cw1w2, double N ) {
			double pmi = (Math.log10(Cw1w2) + Math.log10(N) + Math.log10(1/Cw1) + Math.log10(1/Cw2));
			return pmi;
		}
		
		public double normalizer(double Cw1w2, double N ) {
			double normalizer = Cw1w2/N;
	        if (normalizer  == 0.0) 
	        	normalizer = 0.00001;
	        else if (normalizer == 1.0) 
	        	normalizer = 0.99999;
			return normalizer;
		}
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		Path input=new Path("s3://amirtzurmapreduce/output/");
		Path output=new Path("s3://amirtzurmapreduce/outputStep2/");

		String tempFilesPath = "N/" + 1;
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf, "secondLevele");
		job.setJarByClass(secondLevele.class);
		job.setMapperClass(MapForWordCount.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setMapOutputKeyClass(FirstStepKey.class);
		job.setMapOutputValueClass(firstStepValue.class);
		job.setOutputKeyClass(secondStepKey.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(step2InputFormat.class);//SequenceFileInputFormat
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
		while(true);
		

	}

	public static class PartitionerClass extends Partitioner<FirstStepKey,firstStepValue> {

		@Override
		public int getPartition(FirstStepKey key, firstStepValue value, int num) {
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





