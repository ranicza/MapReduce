package com.epam.bigdata.q3.task3.mapReduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * MapReduce jobs to count amount of all the tags in the dataset. 
 * Combiner coincides with Reduce.
 * 
 * @author Maryna_Maroz
 *
 */
public class TagCount {

	public static final String SET_UP_ERROR = "Exception in setup: ";
	public static final String READ_FILE_ERROR = "Exception while reading file with tags for excluding: ";
	public static final String TAGCOUNT_ERROR = "Usage: Tag count <in> [<in>...] <out>";
	public static final String JOB_NAME = "Tag count";

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);

		// Tags for excluding
		private Set<String> excludeTags = new HashSet();

		private Text tag = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// Get file from the DistributedCache
			try {
				Path[] excludeTagFile = context.getLocalCacheFiles();
				if (excludeTagFile != null && excludeTagFile.length > 0) {
					for (Path path : excludeTagFile) {
						readFile(path);
					}
				}
			} catch (IOException e) {
				System.err.println(SET_UP_ERROR + e.getMessage());
			}
		}

		private void readFile(Path file) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(file.toString()));
				String excludeTag = null;
				while ((excludeTag = br.readLine()) != null) {
					excludeTags.add(excludeTag.toUpperCase());
				}
			} catch (IOException e) {
				System.err.println(READ_FILE_ERROR + e.getMessage());
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String input = value.toString();

			// Get tags from text
			String[] tokens = input.split("\\s+");
			String[] tags = tokens[1].toUpperCase().split(",");
			for (String t : tags) {
				if (t.matches("\\D+") && !excludeTags.contains(t)) {
					tag.set(t);
					context.write(tag, one);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println(TAGCOUNT_ERROR);
			System.exit(2);
		}

		// Create a new Job
		Job job = Job.getInstance(conf, JOB_NAME);
		job.setJarByClass(TagCount.class);

		job.setMapperClass(TagCount.TokenizerMapper.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		if (otherArgs.length > 2) {
			job.addCacheFile(new Path(otherArgs[2]).toUri());
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
