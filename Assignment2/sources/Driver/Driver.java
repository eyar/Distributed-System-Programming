import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3Client;

public class Driver {
	private static PropertiesCredentials _credentials;
	private static String _propertiesFilePath = "credentials.properties";

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		if (args.length != 1) {
		      System.err.println("Please only provide k and only k");
		      System.exit(-1);
		}
		_credentials = new PropertiesCredentials(Driver.class.getResourceAsStream(_propertiesFilePath));
		System.out.println("Credentials created.");
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(_credentials);
        String inputPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data";

        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3://ass2jars/step1.jar") // This should be a full map reduce application.
                .withMainClass("Step1Driver")
                .withArgs(inputPath, "s3://ass2wordrelatedness/output1");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://ass2jars/step2.jar") // This should be a full map reduce application.
                .withMainClass("Step2Driver")
                .withArgs("s3://ass2wordrelatedness/output1", "s3://ass2wordrelatedness/output2");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://ass2jars/step3.jar") // This should be a full map reduce application.
                .withMainClass("Step3Driver")
                .withArgs("s3://ass2wordrelatedness/output2", "s3://ass2wordrelatedness/output3", args[0]);

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.2.0").withEc2KeyName("ass2")
                .withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));
        
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Ass2Flow")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3)
                .withLogUri("s3n://ass2wordrelatedness/logs/");
        
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");

        ClusterSummary theCluster = null;

        ListClustersRequest clustRequest = new ListClustersRequest().withClusterStates(ClusterState.WAITING);

        ListClustersResult clusterList = mapReduce.listClusters(clustRequest);
        for (ClusterSummary cluster : clusterList.getClusters()) {
            if (cluster != null)
                theCluster = cluster;
        }
        
        if (theCluster != null) {
			AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
					.withJobFlowId(theCluster.getId())
					.withSteps(stepConfig1, stepConfig2, stepConfig3);
			AddJobFlowStepsResult runJobFlowResult = mapReduce.addJobFlowSteps(request);
			String jobFlowId = theCluster.getId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		} else {
			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		}
        AmazonS3Client s3 = new AmazonS3Client(_credentials);
        for(int i=1;i<=3;i++){
	        try{
	        	s3.deleteObject("ass2wordrelatedness", "output"+i);
	        }catch(Exception e){
	        	System.out.println("Folder output" + i + " already deleted");
	        }
        }
	}
}
