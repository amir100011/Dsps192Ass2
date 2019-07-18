package com.amazonaws.samples;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Trial {
	
	public static void main(String[] args) {
	AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	@SuppressWarnings("deprecation")
	AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
     
    HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
        .withJar("s3://dsps192mapreduce/jartmp.jar") // This should be a full map reduce application.
        .withMainClass("some.pack.MainClass")
        .withArgs("s3://dsps192mapreduce/jartmp.jar", "s3n://yourbucket/output/");
     
    StepConfig stepConfig = new StepConfig()
        .withName("stepname")
        .withHadoopJarStep(hadoopJarStep)
        .withActionOnFailure("TERMINATE_JOB_FLOW");
     
    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
        .withInstanceCount(2)
        .withMasterInstanceType(InstanceType.M1Small.toString())
        .withSlaveInstanceType(InstanceType.M1Small.toString())
        .withHadoopVersion("2.6.0").withEc2KeyName("amazonKey")
        .withKeepJobFlowAliveWhenNoSteps(false)
        .withPlacement(new PlacementType("us-east-1a"));
     
    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
        .withName("jobname")
        .withInstances(instances)
        .withSteps(stepConfig)
        .withLogUri("s3n://yourbucket/logs/");
     
    RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
    String jobFlowId = runJobFlowResult.getJobFlowId();
    System.out.println("Ran job flow with id: " + jobFlowId);
}
}
