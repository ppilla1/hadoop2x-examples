package org.disknotfound.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * MapReduce Driver
 *
 */
public class App 
{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private static Logger LOG = Logger.getLogger(Map.class);
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private static Logger LOG = Logger.getLogger(Reduce.class);
		
	}
	
    public static void main( String[] args ) throws Exception
    {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ClassViseSurvivorAggr");
		
		job.setJarByClass(App.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Path out = new Path(args[1]);
		out.getFileSystem(conf).deleteOnExit(out);
		
		job.waitForCompletion(true);
    }
}
