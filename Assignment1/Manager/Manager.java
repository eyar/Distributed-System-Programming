import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

public class Manager implements Runnable {
    public static String SQS_MANAGER = "Manager";
	public static final String JOB_QUEUE = "jobsQueue";
	private static final int MAX_ALLOWED_INSTANCES = 19;
	private static AWSClient _awsClient;
	private int _urlsPerWorker;
	private int _queueSerial;
	ArrayList<Thread> _jobHandlers;
//	public static void main(String[] args) throws Exception {
	Manager(){
		System.out.println("Manager started");
    	_awsClient = new AWSClient();
        _queueSerial = -1;
        _jobHandlers = new ArrayList<Thread>();
	}
	public static void main(String[] args) throws Exception{
		(new Manager()).run();
	}
	public void run(){
//    	checks a special SQS queue for messages from local applications. Once it receives a message it:
		List<Message> messages = _awsClient.pollQueue(SQS_MANAGER);
		String downloadedFile_name = null;
    	boolean terminate = false;
    	while(!terminate){
    		//Blocking on fetch from queue
			while(messages.isEmpty()){
				messages = _awsClient.pollQueue(SQS_MANAGER);
			}
			while(!messages.isEmpty()){
				Message message = messages.remove(0);
				String sender = null;
	    		if(message.getBody().contains("start")){
	    			System.out.println("New job from a Local app");
					_awsClient.deleteMessage(SQS_MANAGER, message);
	    			_queueSerial++;
	    			String downoadKey =  message.getMessageAttributes().get("location").getStringValue();
					sender = message.getMessageAttributes().get("sender").getStringValue();
					_urlsPerWorker = Integer.parseInt(message.getMessageAttributes().get("ratio").getStringValue());
	    			System.out.println("Urls per worker is " + _urlsPerWorker);
	    			//If the message is that of a new task it:
	    			//Downloads the input file from S3.
	    			try {
						_awsClient.S3download(downoadKey,"./input" + _queueSerial + ".txt");
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					downloadedFile_name = "./input" + _queueSerial + ".txt";
//		    		Distributes the operations to be performed on the images to the workers using SQS queue/s.
		    		FileInputStream fstream = null;
					try {
						fstream = new FileInputStream(downloadedFile_name);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
		    		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		    		String strLine;

		    		//Read File Line By Line
		    		ArrayList<String> urls = new ArrayList<String>();
		    		try {
						while ((strLine = br.readLine()) != null)   {
							urls.add(strLine); 
						}
			    		//Close the input stream
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
		    		int urlsTotal = urls.size();
		    		
//		    		Checks the SQS message count and starts Worker processes (nodes) accordingly.
//					The manager should create a worker for every n messages, if there are no running workers.
//					If there are k active workers, and the new job requires m workers, then the manager should create m-k new workers, if possible.
		    		_awsClient.createQueue(JOB_QUEUE);
		    		_awsClient.setQVT(JOB_QUEUE, 60);
		    		int neededNumOfWorkers = (urlsTotal/_urlsPerWorker);
		    		int currentlyRunningWorkers = _awsClient.getNumOfRunningInstances();
		    		System.out.println("Currently open are " + currentlyRunningWorkers + " workers");
		    		int workerThatWillOpen = Math.min(neededNumOfWorkers,MAX_ALLOWED_INSTANCES)-currentlyRunningWorkers;
		    		System.out.println("Will try to open " + workerThatWillOpen + " workers");
		    		if(workerThatWillOpen==0 && currentlyRunningWorkers==0){
		    			try{
		            		_awsClient.startNode("worker");
		            	}catch(Exception e){
		            		System.out.println(e);
		            	}
		    		}
		    		else if(workerThatWillOpen>0){
			    		for(int i=0;i<workerThatWillOpen;i++){
			            	try{
			            		_awsClient.startNode("worker");
			            	}catch(Exception e){
			            		System.out.println(e);
			            	}
			            }
		    		}
		            JobsHandler jobHandler = new JobsHandler(_queueSerial, new ArrayList<String>(urls), sender, _awsClient);
		            _jobHandlers.add(new Thread(jobHandler));
		            _jobHandlers.get(_queueSerial).start();
		            
		            int workNumber = 0;
		            List<SendMessageBatchRequestEntry> messageList = new  ArrayList<SendMessageBatchRequestEntry>();
		            while(!urls.isEmpty()){
		            	for(int i=0;i<10 && !urls.isEmpty();i++){
			    			String url = urls.remove(0);
			            	SendMessageBatchRequestEntry jobMessage = new SendMessageBatchRequestEntry(String.valueOf(workNumber),url);
			    			jobMessage.addMessageAttributesEntry("queueSerial", new MessageAttributeValue()
			    					.withDataType("String").withStringValue(String.valueOf(_queueSerial)));
			    			messageList.add(jobMessage);
			    			workNumber++;
		            	}
		            	_awsClient.sendMessages(JOB_QUEUE, messageList);
		            	messageList = new  ArrayList<SendMessageBatchRequestEntry>();
		    		}	
		            
	    		}
//				If the message is a termination message, then the manager:
//				Does not accept any more input files from local applications. However, it does serve the local application that sent the termination message.
	    		else if(message.getBody().contains("terminate")){
	    			System.out.println("Terminate message received, waiting for jobs to finish to finish");
//					Waits for all the workers to finish their job, and then terminates them.
	    			for(Thread t: _jobHandlers){
	    				try {
							t.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
	    			}
	    			System.out.println("All managing threads are done");
//					Creates response messages for the jobs, if needed.
	    			message = new Message();
	    			message.setBody("terminate");
	    			_awsClient.sendMessage(JOB_QUEUE, message);
	    			try {
	    			    Thread.sleep(10000);                 //1000 milliseconds is one second.
	    			} catch(InterruptedException ex) {
	    			    Thread.currentThread().interrupt();
	    			}
	    			System.out.println("Terminating all workers");
	    			_awsClient.TerminateAllWorkers();
	    			_awsClient.deleteAllQueues();
	    			terminate = true;
	    		}
	    	}
    	}
//		Terminates.
    	_awsClient.terminateManager();
    }
}

// This class will handle each new job in a separate thread
class JobsHandler implements Runnable {
	private String _resultsQueue;
    public static String SQS_LOCAL = "Local";
	private static AWSClient _awsClient;
	private ArrayList<String> _checkMap;
	private String _sender;
	public JobsHandler(int _queueSerial, ArrayList<String> checkMap, String sender, AWSClient client) {
		_awsClient = client;
		_checkMap = checkMap;
		_sender = sender;
		_resultsQueue = "resultsQueue" + _queueSerial;
	}
	@Override
	public void run() {
		Hashtable<String,String> results = new Hashtable<String,String>();
		_awsClient.createQueue(_resultsQueue);
		List<Message> answers = _awsClient.pollQueue(_resultsQueue);
//		After the manger receives response messages from the workers on all the files on an input file
		while(!_checkMap.isEmpty()){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			while(answers.isEmpty()){
				answers = _awsClient.pollQueue(_resultsQueue);
				System.out.println("Waiting for all messages from " + _resultsQueue);
			}
			while(!answers.isEmpty()){
				Message answer = answers.remove(0);
				_awsClient.deleteMessage(_resultsQueue, answer);
				String imageUrl = answer.getBody();
				_checkMap.remove(imageUrl);
				String group = answer.getMessageAttributes().get("group").getStringValue();
				results.put(imageUrl, group);
			}
		}
		System.out.println("Job collecting for " + _resultsQueue + " is done, proceeding to sending results");
//		Then it Creates a summary output file accordingly
		FileOutputStream fos = null;
        ObjectOutputStream oos = null;
		try {
			fos = new FileOutputStream("./" + _sender + ".ser");
			oos = new ObjectOutputStream(fos);
			oos.writeObject(results);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
//		Uploads the output file to S3
		_awsClient.uploadToS3("./" + _sender + ".ser");
//		Sends a message to the application with the location of the file.
		Message message = new Message();
		message.setBody(_sender);
		message.addMessageAttributesEntry("location", new MessageAttributeValue()
				.withDataType("String").withStringValue(_sender + ".ser"));
		_awsClient.sendMessage("Local" + _sender, message);
		}
}
