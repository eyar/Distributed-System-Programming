import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.imageio.ImageIO;

import org.joda.time.DateTime;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

// This class was rebuilt for optimizing sqs requests, and is now working with threads,
// batch receiving and batch sending messages.
public class Worker implements Runnable{
	static final String JOB_QUEUE = "jobsQueue";
	public static AWSClient _awsClient;
	String _id;
	DateTime _startTime;
	ArrayList<Long> _urlWorkingTimes;
	BlockingQueue<Message> _jobs = null;
	BlockingQueue<MessageGroupTime> _results = null;
	ArrayList<String> _succesfulUrls = null;
	ArrayList<UrlAndException> _failedUrls = null;
	ArrayList<Thread> threadsArray = null;
	ArrayList<ArrayBlockingQueue<rawMessage>> _forDelivery;
	public Worker() {
    	_awsClient = new AWSClient();
        _id = UUID.randomUUID().toString();
        _urlWorkingTimes = new ArrayList<Long>();
    	_jobs = new ArrayBlockingQueue<Message>(20);
    	_results = new ArrayBlockingQueue<MessageGroupTime>(20);
    	_succesfulUrls = new ArrayList<String>();
		_failedUrls = new ArrayList<UrlAndException>();
		threadsArray = new ArrayList<Thread>();
		_forDelivery = new ArrayList<ArrayBlockingQueue<rawMessage>>();
	}
	public static void main(String[] args) throws Exception{
//		Some prints to start with, mainly for debugging
		System.out.println("Start time is " + (new SimpleDateFormat("HH:mm:ss")).format(new Date()));
		long heapMaxSize = Runtime.getRuntime().maxMemory();
		long heapFreeSize = Runtime.getRuntime().freeMemory(); 
		System.out.println("Current heapMaxSize is " + heapMaxSize);
		System.out.println("Current heapFreeSize is " + heapFreeSize);
		(new Worker()).run();
	}
	
	@Override
	public void run() {
		_startTime = DateTime.now();
		boolean terminate = false;
//		Start all the working threads
		(new Thread(new resultsPrepare(_results, _urlWorkingTimes, _forDelivery, _awsClient))).start();
		(new Thread(new Sender(_forDelivery, _awsClient))).start();
		for(int i=0;i<3;i++){
			UrlWorker u = new UrlWorker(_jobs, _results, _succesfulUrls, _failedUrls);
			threadsArray.add(new Thread(u));
			threadsArray.get(i).start();
		}
		while(!terminate){
//			Thread sometimes paused and didn't continue unless I sent them to sleep for a little while
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Polling job queueu at " + (new SimpleDateFormat("HH:mm")).format(new Date()));
			List<Message> messages = _awsClient.pollQueue(JOB_QUEUE);
			while(!messages.isEmpty()){
				Message message = messages.remove(0);
//				Check if terminate message received
				if(message.getBody().contains("terminate")){
//					Preapare all statistics
					_awsClient.setVTto0(JOB_QUEUE, message);
			        StringBuilder statistics = new StringBuilder();
			        statistics.append("************ Statistics *************\n");
			        statistics.append("ID: " + _id + "\n");
			        statistics.append("Start time: " + _startTime + "\n");
			        Long averageRunTime = calculateAverage(_urlWorkingTimes);
			        statistics.append("Average run-time on a single url (Ms): " + averageRunTime + "\n");
			        statistics.append("Total number of urls handled " + _urlWorkingTimes.size() + "\n");
		        	statistics.append("Successful urls: (Total number is " + _succesfulUrls.size() + ")\n");
			        while(!_succesfulUrls.isEmpty()){
			        	statistics.append(_succesfulUrls.remove(0) + "\n");
			        }
			        statistics.append("Failed urls: (Total number is " + _failedUrls.size() + ")\n");
			        while(!_failedUrls.isEmpty()){
			        	UrlAndException uAe = _failedUrls.remove(0);
			        	statistics.append(uAe.url + "		" + uAe.e + "\n");
			        }
			        long diff = DateTime.now().getMillis() - _startTime.getMillis();
			        long diffMinutes = diff / (60 * 1000) % 60; 
			        statistics.append("Finish time (Minutes): " + diffMinutes + "\n");
			        PrintWriter out = null;
					try {
						out = new PrintWriter(_id + ".txt");
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
			        out.println(statistics.toString());
			        out.close();
			        _awsClient.uploadToS3(_id + ".txt");
			        terminate = true;
			        break;
				}
//				Local queue this message for process
				try {
					_jobs.put(message);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	private long calculateAverage(List <Long> time) {
		Long sum = (long) 0;
		if(!time.isEmpty()) {
		    for (Long mark : time) {
		        sum += mark;
		    }
		    return sum / time.size();
	  	}
	  	return sum;
		}
}

// Processing class - this class is responsible of fetching the image from the internet
// and classify it. 
class UrlWorker implements Runnable{
	protected BlockingQueue<Message> _jobs = null;
	protected BlockingQueue<MessageGroupTime> _results = null;
	protected ArrayList<String> _succesfulUrls = null;
	protected ArrayList<UrlAndException> _failedUrls = null;

	public UrlWorker(BlockingQueue<Message> jobs, BlockingQueue<MessageGroupTime> results,
			ArrayList<String> succesfulUrls, ArrayList<UrlAndException> failedUrls){
		this._jobs = jobs;
		this._succesfulUrls = succesfulUrls;
		this._failedUrls = failedUrls;
		this._results = results;
	}
	
	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Message message = null;
			try {
				message = _jobs.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} 
			long urlStartTime = System.currentTimeMillis();
			URL url = null;
			try {
				url = new URL(message.getBody());
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
			BufferedImage image = null;
			try {
				image = ImageIO.read(url);
				_succesfulUrls.add(url.toString());
			} catch (IOException e) {
				_failedUrls.add(new UrlAndException(url, e));
				System.out.println("Broken url");
			}
			String group = null;
//			Group classification
			if(image==null){
				group = "Broken";
			}
			else if(1024<image.getWidth() || 768<image.getHeight()){
				group = "Huge";
			}
			else if((640<image.getWidth() && image.getWidth()<=1024) || (480<image.getHeight() && image.getHeight()<=768)){
				group = "Large";
			}
			else if((256<image.getWidth() && image.getWidth()<=640) || (256<image.getHeight() && image.getHeight()<=480)){
				group = "Medium";
			}
			else if((64<image.getWidth() && image.getWidth()<=256) || (64<image.getHeight() && image.getHeight()<=256)){
				group = "Small";
			}
			else if((0<image.getWidth() && image.getWidth()<=64) || (0<image.getHeight() && image.getHeight()<=64)){
				group = "Thumbnail";
			}
			if(image!=null) {
				image.flush();
				image = null;
			}
			long heapFreeSize = Runtime.getRuntime().freeMemory(); 
			System.out.println("Current heapFreeSize is " + heapFreeSize); // For debugging
//			Forward to prepare results for delivery
			try {
				_results.put(new MessageGroupTime(message, group, urlStartTime));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

// This class prepare the results for delivery, queuing results in Arraylist of blocking queues according 
// to the job number (0,1,2...)
class resultsPrepare implements Runnable{
	AWSClient _awsClient;
	BlockingQueue<MessageGroupTime> _results;
	ArrayList<Long> _urlWorkingTimes;
	ArrayList<ArrayBlockingQueue<rawMessage>> _forDelivery;
	static final String JOB_QUEUE = "jobsQueue";
	
	resultsPrepare(BlockingQueue<MessageGroupTime> results, ArrayList<Long> urlWorkingTimes, ArrayList<ArrayBlockingQueue<rawMessage>> forDelivery, AWSClient awsClient){
		this._results = results;
		this._awsClient = awsClient;
		this._urlWorkingTimes = urlWorkingTimes;
		this._forDelivery = forDelivery;
	}
	
	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			MessageGroupTime mit = null;
			try {
				mit = _results.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} 
			Message message = mit.message;
			int queueNum = Integer.parseInt(message.getMessageAttributes().get("queueSerial").getStringValue());
			rawMessage rm = new rawMessage(message, mit.group, queueNum);
//			_forDelivery **        0        **       1         **        2        **
//						 ** BlockingQueue_0 ** BlockingQueue_1 ** BlockingQueue_2 ** ...
			if(!(_forDelivery.size()>queueNum)){
				_forDelivery.add(new ArrayBlockingQueue<rawMessage>(20));
			}
			try {
				_forDelivery.get(queueNum).put(rm);
			} catch (IndexOutOfBoundsException | InterruptedException e) {
				_forDelivery.add(new ArrayBlockingQueue<rawMessage>(20));
				try {
					_forDelivery.get(queueNum).put(rm);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
			long urlWorkingTime = System.currentTimeMillis() - mit.startTime;
			_urlWorkingTimes.add(new Long(urlWorkingTime));
		}
	}
}
// This class collect prepared messages and send them at batch 
class Sender implements Runnable {
	AWSClient _awsClient;
	static final String JOB_QUEUE = "jobsQueue";
	ArrayList<ArrayBlockingQueue<rawMessage>> _forDelivery;
	Sender(ArrayList<ArrayBlockingQueue<rawMessage>> forDelivery, AWSClient awsClient){
		this._awsClient = awsClient;
		this._forDelivery = forDelivery;
	}

	@Override
	public void run() {
		boolean b = true;
		while(true){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
//			Batch send to save AWS IO requests
			for(int i=0;i<_forDelivery.size();i++){
				if(!_forDelivery.get(i).isEmpty()){
					System.out.println("Some messages ready to be sent");
					int workNumber = 0;
					List<SendMessageBatchRequestEntry> messageList = new  ArrayList<SendMessageBatchRequestEntry>();
					ArrayList<Message> forDeletion = new ArrayList<Message>(); 
					while(!_forDelivery.get(i).isEmpty() && forDeletion.size()<10){ // 10 messages per batch send is amazon limit
	            		if(b){
	            			System.out.println("First ready batch is now: " + (new SimpleDateFormat("HH:mm:ss")).format(new Date()));
	            			b = false;
	            		}
						rawMessage m = null;
						try {
							m = _forDelivery.get(i).take();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
	            		forDeletion.add(m.message);
	            		SendMessageBatchRequestEntry jobMessage = new SendMessageBatchRequestEntry(String.valueOf(workNumber),m.message.getBody());
		    			jobMessage.addMessageAttributesEntry("group", new MessageAttributeValue()
		    					.withDataType("String").withStringValue(m.group));
		    			messageList.add(jobMessage);
		    			workNumber++;
					}
					String Queue_name = "resultsQueue" + i;
	            	_awsClient.sendMessages(Queue_name, messageList);
	            	System.out.println("Sent " + forDeletion.size() + " messages in batch");
	            	while(!forDeletion.isEmpty()){
	            		_awsClient.deleteMessage(JOB_QUEUE, forDeletion.remove(0));
	            	}
				}
    		}
		}
	}
}

class rawMessage{
	public Message message;
	public String group;
	public int forQueue; 
	rawMessage(Message message, String group, int forQueue){
		this.message = message;
		this.group = group;
		this.forQueue = forQueue;
	}
}

class MessageGroupTime{
	public Message message;
	public String group;
	public long startTime;
	public MessageGroupTime(Message m, String g, long st){
		this.message = m;
		this.group = g;
		this.startTime = st;
	}
}

class UrlAndException{
	public String url;
	public Exception e;
	public UrlAndException(URL url, Exception e){
		this.url = url.toString();
		this.e = e;
	}
}
