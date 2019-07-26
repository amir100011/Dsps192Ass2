package com.amazonaws.samples;

import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Main {

	public static void main(String[] args) {

		//		String uuid = UUID.randomUUID().toString();
		//		String minPmi = args[0];
		//		String relMinPmi = args[1];
		//String bigramUrl = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";

		// get AWS credentials
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

		AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
				.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(credentialsProvider)
				.build();

		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
				.withJar("s3n://amirtzurmapreduce/stepOne.jar")
				.withArgs(); 

		StepConfig stepConfig1 = new StepConfig()
				.withName("step1")
				.withHadoopJarStep(hadoopJarStep1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
				.withJar("s3n://amirtzurmapreduce/stepTwo.jar")
				.withArgs();

		StepConfig stepConfig2 = new StepConfig()
				.withName("step2")
				.withHadoopJarStep(hadoopJarStep2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
				.withJar("s3n://amirtzurmapreduce/stepThree.jar")
				.withArgs();

		StepConfig stepConfig3 = new StepConfig()
				.withName("step3")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(5)
				.withMasterInstanceType(InstanceType.M1Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M1Xlarge.toString())
				.withHadoopVersion("2.6.0")
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withEc2KeyName("amazonKey")
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("ass2")
				.withReleaseLabel("emr-5.14.0")
				.withInstances(instances)
				.withSteps(stepConfig1, stepConfig2, stepConfig3)
				.withLogUri("s3n://amirtzurmapreduce/logs/")
				.withJobFlowRole("EMR_ROLE")
				.withServiceRole("EMR_REG");

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}
}
