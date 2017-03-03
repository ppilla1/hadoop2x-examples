/**
 * Description:
 * MapReduce job using customType
 */
package org.disknotfound.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.disknotfound.hadoop.model.Player;
import org.disknotfound.hadoop.model.PlayerWritable;

/**
 * MapReduce Driver
 * Input Data Schema:
 * yearId,teamId,lgId,playerId,salary
 * 
 */
public class App 
{
	public static class Map extends Mapper<LongWritable, Text, PlayerWritable, Text>{
		private static final Logger LOG = Logger.getLogger(Map.class);

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PlayerWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			
			if(fields.length == 5 && fields[0].matches("\\d+") && fields[4].matches("\\d+")){
				Player player = new Player();
				player.setYearID(Integer.parseInt(fields[0]))
					  .setTeamID(fields[1])
					  .setLgID(fields[2])
					  .setPlayerID(fields[3])
					  .setSalary(Long.parseLong(fields[4]));
				
				LOG.info("[MAP] Processed -> "+player);
				context.write(new PlayerWritable(player), new Text(player.toString()));
			}
		}
	}
	
	public static class Reduce extends Reducer<PlayerWritable, Text, PlayerWritable, Text>{
		private static final Logger LOG = Logger.getLogger(Reduce.class);

		@Override
		protected void reduce(PlayerWritable key, Iterable<Text> values,
				Reducer<PlayerWritable, Text, PlayerWritable, Text>.Context context)
				throws IOException, InterruptedException {

			for(Text value:values){
				context.write(key, value);
			}
		}
}
    public static void main( String[] args ) throws Exception
    {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CustomTypeMapReduceJob");
		
		job.setJarByClass(App.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(PlayerWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(PlayerWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Path out = new Path(args[1]);
		out.getFileSystem(conf).deleteOnExit(out);
		
		job.waitForCompletion(true);
    }
}
