package com.hadoop.log.count;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

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

public class URLAveCount extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length < 5) {
			System.err.println("There are no enough parameters! It requires at least 5");
			System.exit(1);
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(URLAveCount.class);
		job.setJobName("URLAveCount");

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[4]));

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		job.setMapperClass(LogMapper.class);
		job.setCombinerClass(LogCombiner.class);
		job.setReducerClass(LogReducer.class);
		job.setNumReduceTasks(18);
		job.setGroupingComparatorClass(Comparators.GroupingComparator.class);
		job.setMapOutputKeyClass(URLInterface.class);
		job.setMapOutputValueClass(AveRespond.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

		Tools.changeFileName(new Path(arg0[4]), getConf());

		return 0;
	}

	private static class LogMapper extends Mapper<LongWritable, Text, URLInterface, AveRespond> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, URLInterface, AveRespond>.Context context)
						throws IOException, InterruptedException {
			URLInterface urlInter = new URLInterface();
			double respond = 0.0;
			try {
				urlInter.setUrl(new Text(Tools.grabURL(value)));
				urlInter.setTime(new Text(Tools.grabHour(value)));
				respond = Tools.grabRespond(value);
				Tools.parseToDate2(urlInter.getTime());
			} catch (Exception e) {
				return;
			}
			context.write(urlInter, new AveRespond(urlInter.getTime().toString(), 1L, respond));
		}

	}

	private static class LogCombiner extends Reducer<URLInterface, AveRespond, URLInterface, AveRespond> {

		@Override
		protected void reduce(URLInterface urlInter, Iterable<AveRespond> arg1,
				Reducer<URLInterface, AveRespond, URLInterface, AveRespond>.Context context)
						throws IOException, InterruptedException {
			long totalCount = 0L;
			double totalRespond = 0.0;
			for (AveRespond respond : arg1) {
				totalCount += respond.getCount().get();
				totalRespond += respond.getTotalRespond().get();
			}
			context.write(urlInter, new AveRespond(urlInter.getTime().toString(), totalCount, totalRespond));
		}

	}

	private static class LogReducer extends Reducer<URLInterface, AveRespond, NullWritable, Text> {
		MultipleOutputs<NullWritable, Text> mos = null;

		@Override
		protected void setup(Reducer<URLInterface, AveRespond, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(URLInterface urlInter, Iterable<AveRespond> arg1,
				Reducer<URLInterface, AveRespond, NullWritable, Text>.Context context)
						throws IOException, InterruptedException {

			long sumCount = 0L, totalCount = 0L;
			double sumRespond = 0.0, totalRespond = 0.0;
			Text lastTime = null;
			Date date = null;
			StringBuilder sb = new StringBuilder();

			for (AveRespond currentTime : arg1) {
				if (lastTime != null && !lastTime.equals(currentTime.getTime())) {
					if (totalCount == 0L)
						totalCount = 1L;
					try {
						date = Tools.parseToDate2(lastTime);
						date.setHours(date.getHours() + 1);
						sb.append(Tools.parseToString4(lastTime) + " - " + Tools.parseDate(date) + " :   "
								+ Tools.format(totalRespond / totalCount));
						sb.append(System.getProperty("line.separator", "\n"));
					} catch (ParseException e) {
						return;
					}
					totalCount = 0L;
				}
				totalCount += currentTime.getCount().get();
				totalRespond += currentTime.getTotalRespond().get();
				lastTime = new Text(currentTime.getTime().toString());
				sumCount += currentTime.getCount().get();
				sumRespond += currentTime.getTotalRespond().get();
			}

			try {
				if (totalCount == 0L)
					totalCount = 1L;
				date = Tools.parseToDate2(lastTime);
				date.setHours(date.getHours() + 1);
				sb.append(Tools.parseToString4(lastTime) + " - " + Tools.parseDate(date) + " :   "
						+ Tools.format(totalRespond / totalCount));
				sb.append(System.getProperty("line.separator", "\n"));
			} catch (ParseException e) {
				return;
			}

			if (sumCount == 0L)
				sumCount = 1L;

			mos.write(NullWritable.get(),
					new Text("/" + urlInter.getUrl().toString() + ":   " + Tools.format(sumRespond / sumCount)
							+ System.getProperty("line.separator", "\n") + sb.toString()),
					urlInter.getUrl().toString().replace("/", "-") + ".txt");
		}

		@Override
		protected void cleanup(Reducer<URLInterface, AveRespond, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}

	}

}
