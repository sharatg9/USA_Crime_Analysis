package task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	
	@Override
	public void map(LongWritable key ,Text value , Context context ) throws IOException,InterruptedException {
		
		
		
		String line = value.toString();    // we are storing the line in a string variable ‘line’
		String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); //split using regex expression 
		
		if( tokens.length>=11){              //to avoid the ArrayIndexOutOfBoundsException 
			String type = tokens[5];                 // we are storing the Type which is in the 5th column
			String arrest = tokens[8];               // we are storing the arrest which is in the 8th column 
			String distic = tokens[11];                // we are storing the distic which is in the 11th column
			
			
			if(type.equalsIgnoreCase("theft") && arrest.equalsIgnoreCase("true")){              //checking type=theft and arrest is true 
				
				context.write(new Text(distic) , new IntWritable(1));     //we are writing the key and value into the context 
				
			}
			
		}
		

		
	}
	

}
