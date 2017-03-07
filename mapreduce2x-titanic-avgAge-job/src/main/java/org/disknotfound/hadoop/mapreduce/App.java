/**
 * Description:
 * MapReduce Job to find Average Age of Male & Female
 * Survivor's
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
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			
			if (fields.length > 6 && fields[1].equals("0") && fields[5].matches("\\d+")){
				Text category = new Text(fields[4]);
				IntWritable age = new IntWritable(Integer.parseInt(fields[5]));
				
				LOG.info("[MAP] Category["+category.toString()+"]-Age["+age.get()+"]");
				context.write(category, age);
			}else{
				LOG.error("[MAP] Notting mapped for -> "+ value.toString());
			}
			
		}
		
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private static Logger LOG = Logger.getLogger(Reduce.class);

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

				int totalAge = 0;
				int count = 0;

				for(IntWritable age : values){
					totalAge+=age.get();
					count++;
				}
				
				if (count > 0){
					LOG.info("[REDUCE] Category["+key.toString()+"]-Age["+totalAge/count+"]");
					context.write(key, new IntWritable(totalAge/count));
				}
		}
		
		
	}
	
    public static void main( String[] args ) throws Exception
    {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AverageSurvivorAge");
		
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
