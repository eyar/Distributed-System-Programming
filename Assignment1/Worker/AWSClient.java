import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;

public class AWSClient {
	public static PropertiesCredentials credentials;
	public static String GLOBAL_BUCKET = "tutorials3test";
	public static String _propertiesFilePath = "credentials.properties";
	public static AmazonEC2Client _ec2Client;
	public static AmazonS3Client _s3Client;
	public static AmazonSQSClient _sqsClient;
	private static final int RUNNING = 16;
    private static final int PENDING = 0;
	private static final String SECURITY_GROUP = "part1";
	private static final String KEY_PAIR_NAME = "tutorilal";
	private static final String AMI_ID = "ami-146e2a7c";
	AWSClient(){
		try {
    		credentials = new PropertiesCredentials(Worker.class.getResourceAsStream(_propertiesFilePath));
    		System.out.println("Credentials created.");
    		_ec2Client = new AmazonEC2Client(credentials);
    		_s3Client = new AmazonS3Client(credentials);
    		System.out.println("AmazonS3Client created.");
    		_sqsClient = new AmazonSQSClient(credentials);
        	Region usEast = Region.getRegion(Regions.US_EAST_1);
        	_ec2Client.setRegion(usEast);
        	_s3Client.setRegion(usEast);
        	_sqsClient.setRegion(usEast);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
		
	}
	
	public void uploadToS3(String inputFile) {
		// If the bucket doesnt exist - will create it.
		// Notice - this will create it in the default region :	Region.US_Standard
		if (!_s3Client.doesBucketExist(GLOBAL_BUCKET)) {
			_s3Client.createBucket(GLOBAL_BUCKET);
			System.out.println("Bucket Created.");
		}
		else{
			System.out.println("Bucket exist.");
		}
		File f = new File(inputFile);
		PutObjectRequest por = new PutObjectRequest(GLOBAL_BUCKET, f.getName(),f);
		// Upload the file
		_s3Client.putObject(por);
		System.out.println("File uploaded.");		
	}
	
	//Send one message to SQS queue
	public void sendMessage(String toQueue_name, Message msg) {
		System.out.println("Sending a message to " + toQueue_name + " .\n");
		_sqsClient.sendMessage((new SendMessageRequest(_sqsClient.getQueueUrl(toQueue_name).
				getQueueUrl(), msg.getBody())).withMessageAttributes(msg.getMessageAttributes()));		
	}

	//Batch send to SQS queue
	public void sendMessages(String toQueue_name, List<SendMessageBatchRequestEntry> messages) {
		System.out.println("Batch sending messages to " + toQueue_name + " .\n");
		SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(
				_sqsClient.getQueueUrl(toQueue_name).getQueueUrl(), messages);
		_sqsClient.sendMessageBatch(sendMessageBatchRequest );
	}
	
	// Download From S3
	public File S3download(String key, String filePath) throws IOException {
		  S3Object object = _s3Client.getObject(new GetObjectRequest(GLOBAL_BUCKET, key));
		  return AWSClient.createFileFromInputStream(filePath, object.getObjectContent());				
	}

	//************************************************************************************************//
	// Create a file from S3 input stream                                                             //
	//************************************************************************************************//
	public static File createFileFromInputStream(String filePath, InputStream input) throws IOException
	{	
		File file = new File(filePath);

	    Writer writer = new OutputStreamWriter(new FileOutputStream(file));            
	    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	    
	    int currChar;
	    while ((currChar = reader.read()) != -1)
	    {      
	      writer.write(currChar);      
	    }
	    
	    writer.close();
	    return file;
	}

	// Poll SQS Queue
	public List<Message> pollQueue(String fromQueue_name) {
		String queue_url = _sqsClient.getQueueUrl(fromQueue_name).getQueueUrl();
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queue_url);
		receiveMessageRequest = receiveMessageRequest.withMessageAttributeNames("All");
		receiveMessageRequest.setMaxNumberOfMessages(10);
		receiveMessageRequest.setWaitTimeSeconds(20);
        List<Message> messages;
		while(true){
		int tries = 100;
			try{
				messages = _sqsClient.receiveMessage(receiveMessageRequest).getMessages();
				break;
			}catch(Exception e){
				System.out.println("No internet connection");
				if(--tries==0) throw e;
			}
		}
		return messages;
	}

//	public List<Message> pollQueue(String fromQueue_name, int amount) {
//		String queue_url = _sqsClient.getQueueUrl(fromQueue_name).getQueueUrl();
//		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queue_url);
//		receiveMessageRequest = receiveMessageRequest.withMessageAttributeNames("All");
//		receiveMessageRequest.setMaxNumberOfMessages(amount);
//		List<Message> messages = _sqsClient.receiveMessage(receiveMessageRequest).getMessages();
//        return messages;
//	}

	public int ApproximateNumberOfMessages(String queue_name) {
		String queue_url = _sqsClient.getQueueUrl(queue_name).toString();
		Map<String, String> attr = _sqsClient.getQueueAttributes(new GetQueueAttributesRequest(queue_url)).getAttributes();
		return Integer.parseInt(attr.get("ApproximateNumberOfMessages"));
	}

	/**
	 * Checks if a node with the Tag "manager" is already active
	 * 
	 * @return true if the manager node is active. false otherwise.
	 */
	boolean isManagerNodeExists() {
		DescribeInstancesRequest dir = new DescribeInstancesRequest();
		List<Reservation> result = _ec2Client.describeInstances(dir)
				.getReservations();

		for (Reservation reservation : result)
			for (Instance instance : reservation.getInstances())
				if (instance.getState().getCode() == RUNNING// running
						|| instance.getState().getCode() == PENDING) // pending
					for (Tag tag : instance.getTags())
						if (tag.getValue().equals("manager")) {
							System.out
									.println("Manager Node already exists. Continuing...");
							return true;
						}
		return false;
	}

	public void startNode(String manager_or_worker) {
		if (!isManagerNodeExists()) {
			System.out
					.println("Manager Node does not exist yet. Starting Manager node...");
			RunInstancesRequest riReq = new RunInstancesRequest();
			riReq.setInstanceType(InstanceType.T2Micro.toString());
			riReq.setImageId(AMI_ID);
			riReq.setKeyName(KEY_PAIR_NAME);
			riReq.setMinCount(1);
			riReq.setMaxCount(1);
			riReq.withSecurityGroupIds(SECURITY_GROUP);
			switch(manager_or_worker){
				case "manager": riReq.setUserData(getManagerDataScript());
				case "worker": riReq.setUserData(getWorkerDataScript());
			}
			RunInstancesResult riRes = _ec2Client.runInstances(riReq);
			List<String> instanceIdList = new ArrayList<>();
			List<Tag> tagsList = new ArrayList<>();
			String insId = riRes.getReservation().getInstances().get(0)
					.getInstanceId();
			Tag newTag = null;
			switch(manager_or_worker){
				case "manager": newTag = new Tag(insId, "manager");
				case "worker": newTag = new Tag(insId, "worker");
			}
			tagsList.add(newTag);
			instanceIdList.add(insId);

			_ec2Client.createTags(new CreateTagsRequest(instanceIdList, tagsList));
			System.out.println(manager_or_worker + " Node running...");
		}

	}

	private static String getManagerDataScript() {
		ArrayList<String> lines = new ArrayList<>();
		lines.add("#! /bin/bash -ex");
        lines.add("wget https://s3.amazonaws.com/initialjarfilesforassignment1/Manager.zip");
        lines.add("unzip -P bubA2003 Manager.zip");
        lines.add("java -jar Manager.jar");

		return new String(Base64.encodeBase64(join(lines, "\n")
				.getBytes()));
	}

	private static String getWorkerDataScript() {
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash -ex");
        lines.add("wget https://s3.amazonaws.com/initialjarfilesforassignment1/Worker.zip");
        lines.add("unzip -P bubA2003 Worker.zip");
        lines.add("java -jar Worker.jar");

        return new String(Base64.encodeBase64(join(lines, "\n")
                .getBytes()));
    }
	
	static String join(Collection<String> s, String delimiter) {
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			builder.append(iter.next());
			if (!iter.hasNext()) {
				break;
			}
			builder.append(delimiter);
		}
		return builder.toString();
	}

	public void createQueue(String queueName) {
		_sqsClient.createQueue((CreateQueueRequest) (new CreateQueueRequest(queueName).withSdkRequestTimeout(60000)));
	}

	public ListQueuesResult listQueues() {
		return _sqsClient.listQueues();
	}

	public void deleteMessage(String queueUrl, Message message) {
		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
		deleteMessageRequest.setQueueUrl(queueUrl);
		deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
		deleteMessageRequest.setSdkRequestTimeout(300000);
		_sqsClient.deleteMessage(deleteMessageRequest);
//		_sqsClient.deleteMessage(queueUrl, message.getReceiptHandle());
	}

	public void setVTto0(String queueUrl, Message message) {
		_sqsClient.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 0);
	}
}
