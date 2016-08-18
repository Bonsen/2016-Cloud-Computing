package com.hadoop.log.count;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class URLCount extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length < 4) {
			System.err.println("There are no enough parameters! It requires at least 4");
			System.exit(1);
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(URLCount.class);
		job.setJobName("URLCount");

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[3]));

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		job.setMapperClass(LogMapper.class);
		job.setCombinerClass(LogCombiner.class);
		job.setReducerClass(LogReducer.class);
		job.setNumReduceTasks(18);
		job.setGroupingComparatorClass(Comparators.GroupingComparator.class);
		job.setMapOutputKeyClass(URLInterface.class);
		job.setMapOutputValueClass(TimeCount.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

		Tools.changeFileName(new Path(arg0[3]), getConf());

		return 0;
	}

	private static class LogMapper extends Mapper<LongWritable, Text, URLInterface, TimeCount> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, URLInterface, TimeCount>.Context context)
						throws IOException, InterruptedException {
			URLInterface urlInter = new URLInterface();
			try {
				urlInter.setUrl(new Text(Tools.grabURL(value)));
				urlInter.setTime(new Text(Tools.grabTime(value)));
				Tools.parseToDate(urlInter.getTime());
			} catch (Exception e) {
				return;
			}
			context.write(urlInter, new TimeCount(urlInter.getTime().toString(), 1L));
		}

	}

	private static class LogCombiner extends Reducer<URLInterface, TimeCount, URLInterface, TimeCount> {

		@Override
		protected void reduce(URLInterface urlInter, Iterable<TimeCount> arg1,
				Reducer<URLInterface, TimeCount, URLInterface, TimeCount>.Context context)
						throws IOException, InterruptedException {
			long sum = 0L;
			for (TimeCount timeCount : arg1) {
				sum += timeCount.getCount().get();
			}
			context.write(urlInter, new TimeCount(urlInter.getTime().toString(), sum));
		}

	}

	private static class LogReducer extends Reducer<URLInterface, TimeCount, NullWritable, Text> {
		MultipleOutputs<NullWritable, Text> mos = null;

		@Override
		protected void setup(Reducer<URLInterface, TimeCount, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(URLInterface urlInter, Iterable<TimeCount> arg1,
				Reducer<URLInterface, TimeCount, NullWritable, Text>.Context context)
						throws IOException, InterruptedException {

			long sum = 0L, total = 0L;
			Text lastTime = null;
			StringBuilder sb = new StringBuilder();

			for (TimeCount currentTime : arg1) {
				if (lastTime != null && !lastTime.equals(currentTime.getTime())) {
					try {
						sb.append(Tools.parseToString3(lastTime) + "   " + total);
						sb.append(System.getProperty("line.separator", "\n"));
					} catch (ParseException e) {
						return;
					}
					total = 0L;
				}
				total += currentTime.getCount().get();
				lastTime = new Text(currentTime.getTime().toString());
				sum += currentTime.getCount().get();
			}

			try {
				sb.append(Tools.parseToString3(lastTime) + "   " + total);
				sb.append(System.getProperty("line.separator", "\n"));
			} catch (ParseException e) {
				return;
			}

			mos.write(NullWritable.get(),
					new Text("/" + urlInter.getUrl().toString() + ":   " + sum
							+ System.getProperty("line.separator", "\n") + sb.toString()),
					urlInter.getUrl().toString().replace("/", "-") + ".txt");
		}

		@Override
		protected void cleanup(Reducer<URLInterface, TimeCount, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}

	}

}
