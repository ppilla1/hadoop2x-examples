/**
 * Description:
 * MapReduce Job to find Class vise survivor aggregation count
 */
package org.disknotfound.hadoop.mapreduce;

import java.io.IOException;

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
 * Input Data Schema:
 * PassengerId,Survived (0=Survived and 1=Died),PassengerClass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
 *
 */
public class App 
{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private static Logger LOG = Logger.getLogger(Map.class);

		private IntWritable one  = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			
			if (fields.length > 6 && fields[1].equals("0")){
				StringBuilder builder = new StringBuilder();
				builder.append(fields[2])
					   .append(",")
					   .append(fields[4])
					   .append(",")
					   .append(fields[5]);

				context.write(new Text(builder.toString()), one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private static Logger LOG = Logger.getLogger(Reduce.class);

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			int count=0;

			for(IntWritable value:values){
				count+=value.get();
			}
			
			if (count > 0){
				context.write(key, new IntWritable(count));
			}
		}
		
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
