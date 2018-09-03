

//to find the average of all the students marks
//student.txt
import java.io.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
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
import org.apache.hadoop.io.FloatWritable;

	public class Studentaverage {
			
			public static class MapClass extends Mapper<LongWritable,Text,IntWritable,IntWritable>
			   {
	             IntWritable mark=new IntWritable();
	             IntWritable marks=new IntWritable(1); 
			      public void map(LongWritable key,Text value, Context context)throws IOException, InterruptedException
			      {	    	  
                   String studdata[]=value.toString().split(",");
                   mark.set(Integer.parseInt(studdata[2]));
                   context.write(marks,mark);
			      }

			   }
			  public static class ReduceClass extends Reducer<IntWritable,IntWritable,IntWritable,FloatWritable>
			   {
				  
				  
				  public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
				  {
					  int count=0;
					  int sum=0;
					  for(IntWritable i:values)
					  {
						  count++;
						  sum=sum+i.get();
					  }
					   float average=(float)sum/(float)count;
					  
				    context.write(key,new FloatWritable(average));
			       }
			   }
				    
				   
			  public static void main(String[] args) throws Exception {
				    Configuration conf = new Configuration();
				    conf.set("mapreduce.output.textoutputformat.seperator"," ,");
				    Job job = Job.getInstance(conf, "Studentaverage");
				    job.setJarByClass(Studentaverage.class);
				    job.setMapperClass(MapClass.class);
				    //b.setCombinerClass(ReduceClass.class);
				    job.setReducerClass(ReduceClass.class);
				    job.setNumReduceTasks(1);
				    job.setMapOutputKeyClass(IntWritable.class);
				    job.setMapOutputValueClass(IntWritable.class);
				    job.setOutputKeyClass(IntWritable.class);
				    job.setOutputValueClass(FloatWritable.class);
				    job.setInputFormatClass(TextInputFormat.class);
				    job.setOutputFormatClass(TextOutputFormat.class);
				    FileInputFormat.addInputPath(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
			   
			   }


	
		
