run instructions: 
					java -jar Local.jar inputFileName outputFileName n
										
					or, if you want to terminate the manager:
					java  -jar Local.jar inputFileName outputFileName n terminate
					
I used ami-146e2a7c t2.micro instances

In case needed binaries for Worker an Manager are on the following links:
Manager - https://s3.amazonaws.com/hvljhbl/Manager.jar
Worker - https://s3.amazonaws.com/hvljhbl/Worker.jar

I ran the program as follows: java -jar Local.jar ./imageLinks.txt output2.html 50 terminate	
meaning n=50 and the maximum number of workers will open (maximum allow by amazon which is 20)
In a test run I started the Local app at 15:53 and it finished at 16:07 so the runtime is 14 minutes, in differnet times it took less.

I followed tightly the points of the Local app, Manager, Worker and before each paragraph of code i pasted the point that descibred what it does.
Also there are comments and documentation where needed.
I conentrated all AWS functions(EC2, S3, SQS) into single class and that class is included in all of the apps.
One thing that was a choice of mine and wasn't in the assignment instructions was to run on the manager a thread for every Local app that is
running and that thread is finished when all jobs for the specific Local app is finished. Then main manager thread is listening for new messages
from Local apps constanly until a terminate message is being received.

**Important - make sure jars are public, queues are empty. Remeber to use terminate!!
			  example run: java -jar Local.jar imageLinks.txt output.html 50 terminate