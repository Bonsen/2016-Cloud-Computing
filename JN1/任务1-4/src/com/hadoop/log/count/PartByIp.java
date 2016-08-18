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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

public class PartByIp extends Configured implements Tool {
	   public static final String NAME = "named_output";

		@Override
		public int run(String[] arg0) throws Exception {
			
			if (arg0.length < 3) {
				System.err.println("There are no enough parameters! It requires at least 3");
				System.exit(1);
			}
			
			Job job = Job.getInstance(getConf());
			job.setJarByClass(PartByIp.class);
			job.setJobName("App");
			
			FileInputFormat.addInputPath(job, new Path(arg0[0]));
			FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
			
			job.setMapperClass(IpMapper.class);
			job.setMapOutputKeyClass(IpTime.class);
		    job.setMapOutputValueClass(IpInterface.class);
		    
		    job.setCombinerClass(IpCombiner.class);
		    
			job.setReducerClass(MultipleOutputRecorder.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			

		    job.setOutputFormatClass(AlphabetOutputFormat.class);
			
			job.waitForCompletion(true);
			return 0;
		}
		
		private static class IpMapper extends Mapper<Object, Text, IpTime, IpInterface>{	
			public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
				String line = value.toString();
				Kpi kpi = KpiTool.Line2Kpi(line);
				String Ip = kpi.getRemote_addr();
				int timeInt = Integer.parseInt(kpi.getTime_local().substring(12,14));
				IpInterface tempCount = new IpInterface(new Text(Ip), new IntWritable(timeInt));
				
				context.write(new IpTime(new Text(Ip), new IntWritable(timeInt)), tempCount);
			}
		}
		
		private static class IpCombiner extends Reducer<IpTime, IpInterface, IpTime, IpInterface>{
			public void reduce (IpTime key,Iterable<IpInterface> values, Context context)
					throws IOException, InterruptedException{
				IpInterface result = new IpInterface();
			
				for(IpInterface value : values){
					result.IpSum(value);
				}
				
				context.write(key, result);
			}
		}
		
		public static class AlphabetOutputFormat extends MultipleOutputFormat<NullWritable, Text> {  

			@Override
			protected String generateFileNameForKeyValue(NullWritable key, Text value, Configuration conf) {  
				return value.toString().split("\t")[0]+".txt";
			}
		} 
		
		private static class MultipleOutputRecorder extends Reducer<IpTime, IpInterface, NullWritable, Text>{
			private MultipleOutputs<NullWritable, Text> multipleOutputs;
			
			@Override
			protected void setup(Context context)
				throws IOException, InterruptedException{
				multipleOutputs = new MultipleOutputs<NullWritable, Text>(context); 
			}
			
			@Override
			public void reduce (IpTime key,Iterable<IpInterface> values, Context context)
				throws IOException, InterruptedException{
			
				IpInterface []sumIp = new IpInterface[24];
				IpInterface totalIp = new IpInterface();
				StringBuilder sb = new StringBuilder();
				StringBuilder result = new StringBuilder();
				
				for(int i=0;i<24;i++){
					sumIp[i] = new IpInterface(new IntWritable(i));
				}
				
				for(IpInterface value : values){
					sumIp[value.getTimeInt()].IpSum(value);
					totalIp.IpSum(value);
				}
				sb.append(totalIp.getIp() + "\t" + totalIp.toString());
				sb.append(System.getProperty("line.separator", "\n"));
				for(int i=0;i<24;i++){
					sb.append(myTime.time2str(sumIp[i].getTimeInt()) + "\t" + sumIp[i].toString());
					sb.append(System.getProperty("line.separator", "\n"));
				}
				
				multipleOutputs.write(NullWritable.get(), new Text(sb.toString()), totalIp.getIp().toString());
				
			}
			
			@Override
			protected void cleanup(Context context)
				throws IOException, InterruptedException{
				multipleOutputs.close();
			}
		}
		
}
