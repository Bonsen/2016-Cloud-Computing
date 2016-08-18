package com.hadoop.log.count;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class StatusCount extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception{
		if (args.length < 2) {
			System.err.println("There are no enough parameters! It requires at least 2");
			System.exit(1);
		}
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(StatusCount.class);
		job.setJobName("App");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(LogMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(StatusInterface.class);
	    
	    job.setCombinerClass(LogCombiner.class);
	    
	    job.setOutputFormatClass(AlphabetOutputFormat.class);
		job.setReducerClass(LogReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.waitForCompletion(true);
		return 0;
	}
	
	private static class LogMap 
		extends Mapper<Object, Text, IntWritable, StatusInterface>{
			
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
			String line = value.toString();
			Kpi kpi = KpiTool.Line2Kpi(line);
			int timeInt = Integer.parseInt(kpi.getTime_local().substring(12,14));
			Text status = new Text(kpi.getStatus());
			StatusInterface tempCount = new StatusInterface(status, new IntWritable(timeInt));
	
			context.write(new IntWritable(timeInt), tempCount);
		}
	}
	private static class LogCombiner 
		extends Reducer <IntWritable, StatusInterface, IntWritable, StatusInterface>{
	
		public void reduce(IntWritable key, Iterable<StatusInterface> values,Context context)
		throws IOException, InterruptedException {
		
			StatusInterface result = new StatusInterface();
			for(StatusInterface value:values){
				result.StatusSum(value);
			}
			
			context.write(new IntWritable(1), result);
		}
	}
	
	private static class LogReducer
		extends Reducer <IntWritable, StatusInterface, NullWritable, Text>{
		
		
		public void reduce(IntWritable key, Iterable<StatusInterface> values, Context context)
		throws IOException, InterruptedException{
			
			StatusInterface totalStatus = new StatusInterface();
			StatusInterface []table = new StatusInterface[24];
			for(int i=0;i<24;i++){
				table[i] = new StatusInterface();
			}
			StringBuilder sb = new StringBuilder();
			for(StatusInterface value:values){
				table[value.getTimeInt()].StatusSum(value);
				totalStatus.StatusSum(value);
			}
			for(int i=0;i<24;i++){
				sb.append(myTime.time2str(table[i].getTimeInt()) + "\t" + table[i].toString1());
				sb.append(System.getProperty("line.separator", "\n"));
			}
			
			
			context.write(NullWritable.get(), new Text(totalStatus.toString2()));
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}
	  

	public static class AlphabetOutputFormat extends MultipleOutputFormat<NullWritable, Text> {  

	@Override
	protected String generateFileNameForKeyValue(NullWritable key, Text value, Configuration conf) {  
		return "1.txt";
		}
	}

}
