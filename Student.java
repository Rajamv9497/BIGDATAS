import java.io.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

	//data - student
	public class Student {
		
		public static class StudentMap extends Mapper<LongWritable,Text,Text,IntWritable>
		   {
		     public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		      {	    	  
		    	
		    	 String [] data = value.toString().split(",");  //"101,Raja,Tamil,89"
		    	 
		    	 String code=data[0]+","+data[1];
		    	 int val=Integer.parseInt(data[3]);
		    	 
		    	 con.write(new Text(code),new IntWritable(val));
		    	 
		            
		         }
		        
		      
		   }
		
		  public static class StudentReduce extends Reducer<Text,IntWritable,Text,IntWritable>
		   {
			    
			  public void reduce(Text k, Iterable<IntWritable> v,Context context) throws IOException, InterruptedException 
			    	{
			    		
			    	
			    	
			    		int sum=0;
			    	
			      
			      
			    		for(IntWritable i :v)
			    		{
			    			
			    			sum=sum+i.get();
			    						
			    		}
			    		
			    	 IntWritable t=new IntWritable(sum);
			    		context.write(k, t);
			    		
			    		
			 
			    	}
		   }
		
			    
			    
			    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
			    Configuration conf = new Configuration();
				conf.set("mapreduce.output.textoutputformat.separator", ",");
				Job job = Job.getInstance(conf, "Total Marks");
			    job.setJarByClass(Student.class);
			    job.setMapperClass(StudentMap.class);
			    //job.setCombinerClass(CityReduceClass.class);
			    job.setReducerClass(StudentReduce.class);
			   // job.setMapOutputKeyClass(Text.class);
			    //job.setMapOutputValueClass(FloatWritable.class);
			   job.setNumReduceTasks(2);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(IntWritable.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
	}
	
	
	
	
