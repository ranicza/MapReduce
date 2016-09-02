package com.epam.bigdata.q3.task3.mapReduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class CountOfVisits {
	public static final String USAGE_ERROR = "Usage: VisitsSpendsCount <in> <out>";
	public static final String JOB_NAME = "Visits Spends count";  
	public static final String BROWSER_COUNTER = "Browsers Counter:";
	
	private static final String patternIp = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.(\\d{1,3}|\\*)";
	private static final String patternUA = "[a-zA-Z0-9]+\\s[0-9]+\\s[a-zA-Z0-9]+\\s(.*)";
	 
	public static class Map extends Mapper<Object, Text, Text, VisitSpend> {

	        private Text contentIp = new Text();
	        private VisitSpend visitSpend = new VisitSpend();
	        Pattern ipPattern = Pattern.compile(patternIp);
	        Pattern userAgentPattern = Pattern.compile(patternUA);

	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	            String line = value.toString();
	            String[] params = line.split("\\s+");
	            Matcher m = ipPattern.matcher(line);
	            if (m.find()) {
	                String ip = m.group().trim();
	                contentIp.set(ip);
	                Integer bp = Integer.parseInt(params[params.length - 3]);

	                visitSpend.setSpendsCount(bp.intValue());
	                visitSpend.setVisitsCount(1);
	                context.write(contentIp, visitSpend);

	                //Get first part of line (before ip)
	                String firstPart = line.split(ip)[0].trim();

	                Matcher m2 = userAgentPattern.matcher(firstPart);
	                if (m2.find()) {
	                    String userAgent = m2.group(1);
	                    UserAgent ua = new UserAgent(userAgent);
	                    String br = ua.getBrowser().getName();
	                    context.getCounter(ua.getBrowser()).increment(1);
	                }
	            }
	        }

	    }

	    public static class Reduce extends Reducer<Text, VisitSpend, Text, VisitSpend> {
	        private VisitSpend result = new VisitSpend();

	        public void reduce(Text key, Iterable<VisitSpend> values, Context context)  {
	            try {
	                int visitCount = 0;
	                int sumbidPrice = 0;
	                for (VisitSpend val : values) {
	                    visitCount += val.getVisitsCount();
	                    sumbidPrice += val.getSpendsCount();
	                }
	                result.setVisitsCount(visitCount);
	                result.setSpendsCount(sumbidPrice);
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
	        Job job = new Job(conf, JOB_NAME);
	        job.setJarByClass(CountOfVisits.class);
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

	        System.out.println(BROWSER_COUNTER);
	        for (Counter counter : job.getCounters().getGroup(Browser.class.getCanonicalName())) {
	            System.out.println(counter.getDisplayName() + "- " + counter.getValue());
	        }

	        System.exit(result ? 0 : 1);
	    }
	
	
}
