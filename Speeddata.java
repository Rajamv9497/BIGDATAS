

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

	public class Speeddata {
			
			public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
			   {
	             Text vehno=new Text();
	             IntWritable speed=new IntWritable(); 
			      public void map(LongWritable key,Text value, Context context)throws IOException, InterruptedException
			      {	    	  
                    String spdata[]=value.toString().split(",");
                    vehno.set(spdata[0]);
                    speed.set(Integer.parseInt(spdata[1]));
                    context.write(vehno,speed);
			      }

			   }
			  public static class ReduceClass extends Reducer<Text,IntWritable,Text,FloatWritable>
			   {
				  
				  
				  public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
					  
					  int count=0,count1=0;
					 
					  IntWritable speed1=new IntWritable();
					  for(IntWritable k:values)
					  {
						  count1++;
						 if(k.get()>65)
							 count++;	 
					  }
					  context.write(key,new FloatWritable(count*100/count1));
			
			    }
			   }
				    
				   
			  public static void main(String[] args) throws Exception {
				    Configuration conf = new Configuration();
				    conf.set("mapreduce.output.textoutputformat.seperator"," ,");
				    Job job = Job.getInstance(conf, "Speeddata");
				    job.setJarByClass(Speeddata.class);
				    job.setMapperClass(MapClass.class);
				    //b.setCombinerClass(ReduceClass.class);
				    job.setReducerClass(ReduceClass.class);
				    job.setNumReduceTasks(1);
				    job.setOutputKeyClass(Text.class);
				    job.setOutputValueClass(IntWritable.class);
				    job.setInputFormatClass(TextInputFormat.class);
				    job.setOutputFormatClass(TextOutputFormat.class);
				    FileInputFormat.addInputPath(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
			   
			   }


	
		