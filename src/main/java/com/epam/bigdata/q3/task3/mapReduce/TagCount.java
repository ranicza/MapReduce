package com.epam.bigdata.q3.task3.mapReduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TagCount {
	
	public static final String SET_UP_ERROR = "Exception in mapper setup: ";
	public static final String READ_FILE_ERROR = "Exception while reading stop words file: ";
	public static final String TAGCOUNT_ERROR = "Usage: tagcount <in> [<in>...] <out>";
	public static final String INSTANCE = "tag count";
	
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        //Tags for excluding 
        private Set<String> excludeTags = new HashSet();
        
    	private static final IntWritable one = new IntWritable(1);
        private Text tag = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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
        
        private void readFile(Path filePath) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
                String excludeTag = null;
                while ((excludeTag = br.readLine()) != null) {
                	excludeTags.add(excludeTag.toUpperCase());
                }
            } catch (IOException e) {
                System.err.println(READ_FILE_ERROR + e.getMessage());
            }
        }
        
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();

            String[] params = input.split("\\s+");
            String[] tags = params[1].toUpperCase().split(",");
            for (String t : tags) {  
            	if(t.matches("\\D+") && !excludeTags.contains(t)){
            		tag.set(t);
                    context.write(tag, one);
            	}    
            }

        }
    }
    
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

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
        
        if(otherArgs.length < 2) {
            System.err.println(TAGCOUNT_ERROR);
            System.exit(2);
        }

        Job job = Job.getInstance(conf, INSTANCE);
        job.setJarByClass(TagCount.class);
        job.setMapperClass(TagCount.TokenizerMapper.class);
        
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

//        for(int i = 0; i < otherArgs.length - 1; ++i) {
//            //FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
        
        if (otherArgs.length > 2) {
            job.addCacheFile(new Path(otherArgs[2]).toUri());
        }


        System.exit(job.waitForCompletion(true)?0:1);
    }




}
