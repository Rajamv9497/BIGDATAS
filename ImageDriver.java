//to decrypt the output of binaryfilestohadoopsequencefile and to display the path of images without duplicates
//  O/P from binaryfilestohadoopsequencefile(input file)
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

import java.security.MessageDigest;

import java.security.NoSuchAlgorithmException;

//import org.apache.hadoop.conf.Configuration;

//import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.Text;

//import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

//import org.apache.hadoop.mapreduce.Reducer;

//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ImageDriver extends Configured implements Tool
{
	
       

        public static class ImageDuplicatesMapper extends Mapper<Text, BytesWritable, Text, Text>{

 

                public void map(Text key, BytesWritable value, Context context) throws IOException,InterruptedException {

                        //get the md5 for this specific file

                String md5Str;

                        try {

                                md5Str = calculateMd5(value.getBytes());

                        } catch (NoSuchAlgorithmException e) {

                                e.printStackTrace();

                                context.setStatus("Internal error - can't find the algorithm for calculating the md5");

                                return;

                        }

                        Text md5Text = new Text(md5Str);

               

                        //put the file in the map where the md5 is the key, so duplicates will

                        // be grouped together for the reduce function

                context.write(md5Text, key);

                }

               

               

                static String calculateMd5(byte[] imageData) throws NoSuchAlgorithmException {

                        //get the md5 for this specific data

                MessageDigest md = MessageDigest.getInstance("MD5");

                md.update(imageData);

                byte[] hash = md.digest();

 

                // Below code of converting Byte Array to hex

                String hexString = new String();

                for (int i=0; i < hash.length; i++) {

                        hexString += Integer.toString( ( hash[i] & 0xff ) + 0x100, 16).substring( 1 );

                }

                return hexString;

                }

           

        }


        public static class ImageDupsReducer extends Reducer<Text,Text,Text,NullWritable> {

        	 

            public void reduce(Text key, Iterable<Text> values, Context context)

                                                    throws IOException, InterruptedException {

                    //Key here is the md5 hash while the values are all the image files that

                    // are associated with it. for each md5 value we need to take only

                    // one file (the first)

                    Text imageFilePath = null;

                    for (Text filePath : values) {

                            imageFilePath = filePath;

                            break;//only the first one

                    }
                    context.write(new Text(imageFilePath), NullWritable.get());

                    
                    // In the result file the key will be again the image file path.

                    

            }

    } 





    public int run(String[] args) throws Exception
    {
          //getting configuration object and setting job name
          Configuration conf = getConf();
          Job job = Job.getInstance(conf, "Unique Images");

         // Job job = new Job(conf, "Duplicate Images");
    
      //setting the class names
      job.setJarByClass(ImageDriver.class);
      job.setMapperClass(ImageDuplicatesMapper.class);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      //job.setCombinerClass(WordCountReducer.class);
      job.setReducerClass(ImageDupsReducer.class);
      job.setNumReduceTasks(1);
      
      //setting the output data type classes
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);

      //to accept the hdfs input and outpur dir at run time
      FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
  }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ImageDriver(), args);
        System.exit(res);
    }
	}