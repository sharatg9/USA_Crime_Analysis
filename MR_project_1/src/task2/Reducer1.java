
package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> value , Context context) throws IOException,InterruptedException{
		
		int sum = 0;             //declaring an integer sum
		for (IntWritable val:value){
			
			sum= sum+val.get();         //we are storing and calculating the sum of the values
			
		}
		
		context.write(key, new IntWritable(sum));  // writes the respected key and the obtained sum as value to the context.
		

	}

}
