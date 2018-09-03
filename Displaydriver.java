//to display the count of no of drivers based on hours and miles
//timesheet.csv
import java.io.*;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	//data - student
	public class Displaydriver {
		
		public static class FruitMap extends Mapper<LongWritable,Text,Text,IntWritable>
		   {
		     public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		      {	    	  
		       	final IntWritable  num =new  IntWritable(1);
       	    	 String [] data = value.toString().split(","); 
       	    	 Text basis=new Text(data[5]);
       	    	 con.write(basis, num);
		         }
		        
		      
		   }
		
		  public static class FruitReduce extends Reducer<Text,IntWritable,Text,IntWritable>
		   {
			    
			  public void reduce(Text key, Iterable<IntWritable> val,Context context) throws IOException, InterruptedException 
			    	{
			    	 int count=0;
			         for(IntWritable i :val)
			    		{
			        	  count=count+i.get();
			    		}
			    		
			    	 IntWritable t=new IntWritable(count);
			    		context.write(key, t);
			    		
			    		
			 
			    	}
		   }
		
			    
			    
			    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
			    Configuration conf = new Configuration();
				conf.set("mapreduce.output.textoutputformat.separator", ",");
				Job job = Job.getInstance(conf, "Total Marks");
			    job.setJarByClass(Displaydriver.class);
			    job.setMapperClass(FruitMap.class);
			    //job.setCombinerClass(CityReduceClass.class);
			    job.setReducerClass(FruitReduce.class);
			   // job.setMapOutputKeyClass(Text.class);
			    //job.setMapOutputValueClass(FloatWritable.class);
			    job.setNumReduceTasks(1);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(IntWritable.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
	}
	
	
	
	
