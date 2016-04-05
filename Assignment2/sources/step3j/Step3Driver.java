import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step3Driver {
	public static void main(String[] args) throws IOException {
		   try{
				Configuration conf = new Configuration();
				conf.set("k", args[3]);
				Job job = new Job(conf, "Step3");
		        job.setJarByClass(Step3Driver.class);
	
			    FileInputFormat.addInputPath(job, new Path(args[1]));
		    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    	job.setInputFormatClass(TextInputFormat.class);
		        job.setMapperClass(Step3Mapper.class);
			    job.setReducerClass(Step3Reducer.class);
		        job.setMapOutputKeyClass(Text.class);
		        job.setMapOutputValueClass(Text.class);
			    job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
		        job.setPartitionerClass(Step3Partitioner.class);
		        job.setSortComparatorClass(ReverseComparator.class);

		        job.waitForCompletion(true) ;

			}catch(Exception e) {
			    e.printStackTrace(System.err);
			}
	}
}