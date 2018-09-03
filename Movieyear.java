//to find out the movies between 1945 and 1959
//movies.txt
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Movieyear {

	public static class MovieMap extends Mapper<LongWritable,Text,IntWritable,Text>
	   {
	     public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	      {	    	  
	    	
	    	 String [] movie = value.toString().split(","); 
	    	 
	    	 String code=movie[0]+","+movie[1]+","+movie[3];
	    	 int year=Integer.parseInt(movie[2]);
	    	 if(year>=1945 && year<=1959) {
	    	 
	    	 con.write(new IntWritable(year),new Text(code));
	    	 }
	            
	         }
	        
	      
	   }
	
	  public static class MovieReduce extends Reducer<IntWritable,Text,IntWritable,IntWritable>
	   {
		    
		  public void reduce(IntWritable k, Iterable<Text> v,Context context) throws IOException, InterruptedException 
		    	{
		    		
		    	
		    	
		    		int count=0;
		    	
		      
		      
		    		for(Text i :v)
		    		{
		    			
		    			count++;
		    						
		    		}
		    		
		    	
		    		context.write(k,new IntWritable(count) );
		    		
		    		
		 
		    	}
	   }
	
		    
		    
		    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		    Configuration conf = new Configuration();
			conf.set("mapreduce.output.textoutputformat.separator", ",");
			Job job = Job.getInstance(conf, "Total Marks");
		    job.setJarByClass(Movieyear.class);
		    job.setMapperClass(MovieMap.class);
		    //job.setCombinerClass(CityReduceClass.class);
		    job.setReducerClass(MovieReduce.class);
		   job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Text.class);
		   //job.setNumReduceTasks(1);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}





