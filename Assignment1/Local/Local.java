import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;


public class Local{
	PropertiesCredentials _credentials;
	static String _inputFile_path = null;
	static String _fileName = "imageLinks.txt";
	static AWSClient _awsClient = null;
	static String _outputFile_path = null;
    static String SQS_MANAGER = "Manager";
    static String sqsLocal;
	static String _urlsPerWorker;
	public static void main(String[] args) throws Exception{
		if(args.length == 3 || args.length == 4){
			System.out.println("Start time is " + (new SimpleDateFormat("HH:mm")).format(new Date()));
			_inputFile_path = args[0];
			_outputFile_path = args[1];
			_urlsPerWorker = args[2];
		}
		else{
			System.out.println("Wrong input arguments format");
			return;
		}
		String uuid = UUID.randomUUID().toString();
		_awsClient = new AWSClient();
		sqsLocal = "Local" + uuid;
		_awsClient.createQueue(sqsLocal);
		_awsClient.setQVT(sqsLocal,7);
		_awsClient.createQueue(SQS_MANAGER);
		_awsClient.setQVT(SQS_MANAGER,7);
		
		//Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the manager node.
		if(!_awsClient.isManagerNodeExists()){
			System.out.println("Manager Node does not exist yet. Starting Manager node...");
			_awsClient.startNode("manager");
		}
		
		//Uploads the file to S3.
		_awsClient.uploadToS3(_inputFile_path);

		//Sends a message to an SQS queue, stating the location of the file on S3
		Message message = new Message();
		message.setBody("start");
		message.addMessageAttributesEntry("location", new MessageAttributeValue()
				.withDataType("String").withStringValue(_fileName));
		message.addMessageAttributesEntry("sender", new MessageAttributeValue()
				.withDataType("String").withStringValue(uuid));
		message.addMessageAttributesEntry("ratio", new MessageAttributeValue()
				.withDataType("String").withStringValue(_urlsPerWorker));
		_awsClient.sendMessage(SQS_MANAGER, message);
		
		//Checks an SQS queue for a message indicating the process is done and the response (the summary file) is available on S3.
		boolean managerDone = false;
		boolean firstLoop = false;
		while(!managerDone){
			List<Message> messages = _awsClient.pollQueue(sqsLocal);
			if(!firstLoop){
				System.out.println("Waiting for finish message from manager");
				firstLoop = true;
			}
			while(!messages.isEmpty()){
				message = messages.remove(0);
	    		if(message.getBody().contains(uuid)){
	    			managerDone = true;
	    			_awsClient.deleteMessage(sqsLocal, message);
	    			//Downloads the summary file from S3, and create an html file representing the results.
	    			String downoadKey =  message.getMessageAttributes().get("location").getStringValue();
	    			_awsClient.S3download(downoadKey,"./" + downoadKey);
	    		}
	    	}
		}
		System.out.println("Manager is done, continuing to handle results");		
//		Load serialized hashtable
		FileInputStream fis = new FileInputStream("./" + uuid + ".ser");
        ObjectInputStream ois = new ObjectInputStream(fis);
        @SuppressWarnings("unchecked")
		Hashtable<String,String> results = (Hashtable<String,String>) ois.readObject();
        ois.close();
//        Create Html output
        StringBuilder htmlBuilder = new StringBuilder();
        htmlBuilder.append("<table>");

        for (Map.Entry<String, String> entry : results.entrySet()) {
            htmlBuilder.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
                    entry.getKey(), entry.getValue()));
        }

        htmlBuilder.append("</table>");

        String html = htmlBuilder.toString();
        
        PrintWriter out = new PrintWriter(_outputFile_path);
        out.println(html);
        out.close();
        
		//Sends a termination message to the Manager if it was supplied as one of its input arguments.
		if(args.length == 4){
			Message msg = new Message();
			msg.setBody("terminate");
			_awsClient.sendMessage(SQS_MANAGER, msg);
		}
		System.out.println("End time: " + (new SimpleDateFormat("HH:mm:")).format(new Date()));
	}
}