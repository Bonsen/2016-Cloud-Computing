package com.hadoop.log.predict;

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
import org.apache.hadoop.util.ToolRunner;

public class FrequencyPrediction extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FrequencyPrediction(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length < 2) {
			System.err.println("There are no enough parameters! It requires at least 2");
			System.exit(1);
		}
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(FrequencyPrediction.class);
		job.setJobName("FrequencyPrediction");

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		job.setMapperClass(PredictMapper.class);
		job.setCombinerClass(PredictCombiner.class);
		job.setReducerClass(PredictReducer.class);
		job.setNumReduceTasks(1);
		job.setSortComparatorClass(Comparators.SortComparatorForPredict.class);
		job.setCombinerKeyGroupingComparatorClass(Comparators.SortComparatorForPredict.class);
		job.setGroupingComparatorClass(Comparators.GroupingComparatorForPredict.class);
		job.setMapOutputKeyClass(URLInterface.class);
		job.setMapOutputValueClass(URLTimeCount.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

		Tools.changeFileName(new Path(arg0[1]), getConf());

		return 0;
	}

	private static class PredictMapper extends Mapper<LongWritable, Text, URLInterface, URLTimeCount> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, URLInterface, URLTimeCount>.Context context)
						throws IOException, InterruptedException {
			URLInterface urlInter = new URLInterface();
			try {
				urlInter.setUrl(new Text(Tools.grabURL(value)));
				urlInter.setTime(new Text(Tools.grabHour(value)));
				if (!Tools.checkDate(Tools.parseToDate2(urlInter.getTime())))
					return;
			} catch (Exception e) {
				return;
			}
			context.write(urlInter, new URLTimeCount(urlInter.getUrl().toString(), urlInter.getTime().toString(), 1L));
		}

	}

	private static class PredictCombiner extends Reducer<URLInterface, URLTimeCount, URLInterface, URLTimeCount> {

		@Override
		protected void reduce(URLInterface urlInter, Iterable<URLTimeCount> arg1,
				Reducer<URLInterface, URLTimeCount, URLInterface, URLTimeCount>.Context context)
						throws IOException, InterruptedException {
			long sum = 0L;
			for (URLTimeCount urlTimeCount : arg1) {
				sum += urlTimeCount.getCount().get();
			}
			context.write(urlInter, new URLTimeCount(urlInter.getUrl().toString(), urlInter.getTime().toString(), sum));
		}

	}

	private static class PredictReducer extends Reducer<URLInterface, URLTimeCount, NullWritable, Text> {
		MultipleOutputs<NullWritable, Text> mos = null;

		@Override
		protected void setup(Reducer<URLInterface, URLTimeCount, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(URLInterface urlInter, Iterable<URLTimeCount> arg1,
				Reducer<URLInterface, URLTimeCount, NullWritable, Text>.Context context)
						throws IOException, InterruptedException {

			long sum = 0L;
			Text lastUrl = null;
			Date lastDay = null, predictDay = null;
			StringBuilder sb = new StringBuilder();
			long[] originalData = new long[15];
			for (int i = 0; i < 15; i++)
				originalData[i] = 0L;
			String predictData = "";

			for (URLTimeCount currentUrlTimeCount : arg1) {
				Date currentDay = null;
				try {
					currentDay = Tools.parseToDate2(currentUrlTimeCount.getTime());
					currentDay.setHours(0);
				} catch (ParseException e) {
					continue;
				}
				if (lastUrl != null && !lastUrl.equals(currentUrlTimeCount.getUrl())) {
					originalData[currentDay.getDate() - new Date(115, 8, 8).getDate()] = sum;
					predictData = Tools.predict(originalData);
					sb.append(lastUrl.toString() + " : " + predictData + System.getProperty("line.separator", "\n"));
					Tools.clearOriginal(originalData);
					sum = 0L;
				} else if (lastUrl != null) {
					lastDay.setHours(0);
					if (lastDay.compareTo(currentDay) != 0) {
						originalData[currentDay.getDate() - new Date(115, 8, 8).getDate()] = sum;
						sum = 0L;
					}
				}

				try {
					lastDay = Tools.parseToDate2(currentUrlTimeCount.getTime());
					lastUrl = new Text(currentUrlTimeCount.getUrl());
				} catch (ParseException e) {
					continue;
				}
				sum += currentUrlTimeCount.getCount().get();
			}

			originalData[lastDay.getDate() - new Date(115, 8, 8).getDate()] = sum;
			predictData = Tools.predict(originalData);
			sb.append(lastUrl.toString() + " : " + predictData + System.getProperty("line.separator", "\n"));

			predictDay = lastDay;
			predictDay.setDate(23);
			String str = Tools.parseDate2(predictDay);
			predictDay.setHours(predictDay.getHours() + 1);
			str = str + " - " + Tools.parseDate(predictDay) + System.getProperty("line.separator", "\n");

			mos.write(NullWritable.get(), new Text(str + sb.toString()), "5.txt");
		}

		@Override
		protected void cleanup(Reducer<URLInterface, URLTimeCount, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}

	}

}
