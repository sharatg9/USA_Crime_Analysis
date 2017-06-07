
package task2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Driver {
	public static void main(String[] args) throws Exception {
		
		// Create a new Job
		Configuration conf = new Configuration();
		
		// Create a  Job
		Job job = new Job (conf ,"task2");
		
		// Setup MapReduce job
		job.setMapperClass(Map.class);
		job.setReducerClass(Reducer1.class);
		job.setJarByClass(Driver.class);
		
		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Input and Output
		FileInputFormat.addInputPath(job, new Path(args[0])) ;
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
	    // Execute job		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}


}
