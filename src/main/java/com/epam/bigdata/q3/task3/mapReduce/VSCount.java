package com.epam.bigdata.q3.task3.mapReduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;

/**
 * MR jobs to count amount of visits (count(*)) by IP and spends (sum(Bidding
 * price)) by IP. Output is saved as Sequence file compressed with Snappy (key
 * is IP, and value is custom object for visits and spends). Counters are used
 * to get stats how many records of various browser were detected.
 * 
 * @author Maryna_Maroz
 *
 */
public class VSCount {
	public static final String USAGE_ERROR = "Usage: VSCount <in> <out>";
	public static final String JOB_NAME = "VS count";

	private static final String regIp = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.(\\d{1,3}|\\*)";
	private static final String regUA = "[a-zA-Z0-9]+\\s[0-9]+\\s[a-zA-Z0-9]+\\s(.*)";

	public static class Map extends Mapper<LongWritable, Text, Text, VisitSpend> {

		private Text contentIp = new Text();
		private VisitSpend vs = new VisitSpend();

		Pattern patIP = Pattern.compile(regIp);
		Pattern patUA = Pattern.compile(regUA);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] tokens = line.split("\\s+");
			Matcher m = patIP.matcher(line);

			if (m.find()) {
				// Get IP
				String ip = m.group().trim();
				contentIp.set(ip);

				// Get Bidding price
				Integer bprice = Integer.parseInt(tokens[tokens.length - 3]);

				vs.setAmountVisits(1);
				vs.setAmountSpends(bprice.intValue());
				context.write(contentIp, vs);

				/*
				 * Get first part of input line (before ip) for extracting User
				 * Agent
				 */
				String first = line.split(ip)[0].trim();

				Matcher m2 = patUA.matcher(first);
				if (m2.find()) {
					String userAgent = m2.group(1);
					UserAgent ua = new UserAgent(userAgent);
					context.getCounter(ua.getBrowser()).increment(1);
				}
			}
		}

	}

	public static class Reduce extends Reducer<Text, VisitSpend, Text, VisitSpend> {
		private VisitSpend result = new VisitSpend();

		public void reduce(Text key, Iterable<VisitSpend> values, Context context) {
			try {
				int visitSum = 0;
				int bpriceSum = 0;
				for (VisitSpend val : values) {
					visitSum += val.getAmountVisits();
					bpriceSum += val.getAmountSpends();
				}
				result.setAmountVisits(visitSum);
				result.setAmountSpends(bpriceSum);
				context.write(key, result);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println(USAGE_ERROR);
			System.exit(2);
		}

		// Create a new job
		Job job = new Job(conf, JOB_NAME);
		job.setJarByClass(VSCount.class);
		
		//Sets mapper & reduce
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VisitSpend.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		boolean result = job.waitForCompletion(true);

		for (Counter counter : job.getCounters().getGroup(Browser.class.getCanonicalName())) {
			System.out.println(counter.getDisplayName() + "- " + counter.getValue());
		}

		System.exit(result ? 0 : 1);
	}
}
