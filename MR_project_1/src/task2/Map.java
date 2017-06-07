package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	
	@Override
	public void map(LongWritable key ,Text value , Context context ) throws IOException,InterruptedException {
		

		String line = value.toString();            // we are storing the line in a string variable ‘line’                  
		
		String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); //split using regex expression
		
		if(tokens.length >= 13){                           //to avoid the ArrayIndexOutOfBoundsException 
			
			String Fbi_Code = tokens[13];                  // we are storing the FBI_CODe which is in the 13th column                  
			
			if (Fbi_Code.equals("32")){                    //filtering only 32th fbi code 
			context.write(new Text(Fbi_Code) , new IntWritable(1));	
			}
		}
		
		
			
		
	}
	

}
