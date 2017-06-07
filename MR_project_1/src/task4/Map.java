
package task4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	
	@Override
	public void map(LongWritable key ,Text value , Context context ) throws IOException,InterruptedException {
		
		
		
		String line = value.toString();      // we are storing the line in a string variable ‘line’
		String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);    //split using regex expression
	
	    if(tokens.length >= 8 ){                //to avoid the ArrayIndexOutOfBoundsException 
	    	
	    	String[] date = tokens[02].substring(0,10).split("/");         //substring the date  
			int  month=Integer.parseInt(date[1]);			 				//converting date into int 
			int  year=Integer.parseInt(date[2]);                            //converting year into int 
			String arrest = tokens[8];                                        // we are storing the arrest which is in the 8th column  
			String count = "Count"; 
			
			if ((month >= 10 && year == 2014) ||(month <=10 && year == 2015)){        //checking the condition 
				
				
				if(arrest.equalsIgnoreCase("True"))                                  //checking arrested or not 
				{
						
					context.write(new Text(count) , new IntWritable(1));                  //we are writing the key and value into the context
				}
			}
	    	
	    	
	    }
		
		
	}
	

}
