import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class Step1Driver {
	public static void main(String[] args) throws IOException {
		try{
			Configuration conf = new Configuration();
	        Job job = new Job(conf, "Word Relatedness");
	        job.setJarByClass(Step1Driver.class);

	        System.out.println(args);
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    
		    job.setInputFormatClass(SequenceFileInputFormat.class);
		    
	        job.setMapperClass(Step1Mapper.class);
		    job.setReducerClass(Step1Reducer.class);
		    
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        
		    job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
          
	        job.setPartitionerClass(Step1Partitioner.class);

	        job.waitForCompletion(true) ;

		}catch(Exception e) {
		      e.printStackTrace(System.err);
//		      System.exit(1);
		}
	}
}
