package com.epam.bigdata.q3.task3.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TagCountTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;	
	
	private String input = "282163091263	automobile,cleaning,gun,gun,oil,oil,oil 45t	ON	CPC	BROAD	http://www.miniinthebox.com/oil-pollution-cleaning-automobile-engine-pipe-with-reinigungspistole-spray-gun-tool_p4815979.html";
	private String str1 = "AUTOMOBILE";
	private String str2 = "CLEANING";
	private String str3 = "GUN";
	private String str4 = "OIL";
	
	@Before
	  public void setUp() {
		TagCount.TokenizerMapper mapper = new TagCount.TokenizerMapper();
		TagCount.Reduce reducer = new TagCount.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(input));
		mapDriver.withOutput(new Text(str1), new IntWritable(1));
		mapDriver.withOutput(new Text(str2), new IntWritable(1));
		mapDriver.withOutput(new Text(str3), new IntWritable(1));
		mapDriver.withOutput(new Text(str3), new IntWritable(1));
		mapDriver.withOutput(new Text(str4), new IntWritable(1));
		mapDriver.withOutput(new Text(str4), new IntWritable(1));
		mapDriver.withOutput(new Text(str4), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values1 = new ArrayList<IntWritable>();
		values1.add(new IntWritable(1));
		reduceDriver.withInput(new Text(str1), values1);
		
		List<IntWritable> values2 = new ArrayList<IntWritable>();
		values2.add(new IntWritable(1));
		reduceDriver.withInput(new Text(str2), values2);
		
		List<IntWritable> values3 = new ArrayList<IntWritable>();
		values3.add(new IntWritable(1));
		values3.add(new IntWritable(1));
		reduceDriver.withInput(new Text(str3), values3);
 
		List<IntWritable> values4 = new ArrayList<IntWritable>();
		values4.add(new IntWritable(1));
		values4.add(new IntWritable(1));
		values4.add(new IntWritable(1));
		reduceDriver.withInput(new Text(str4), values4);
 
		reduceDriver.withOutput(new Text(str1), new IntWritable(1));
		reduceDriver.withOutput(new Text(str2), new IntWritable(1));
		reduceDriver.withOutput(new Text(str3), new IntWritable(2));
		reduceDriver.withOutput(new Text(str4), new IntWritable(3));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(input));
 
		mapReduceDriver.withOutput(new Text(str1), new IntWritable(1));
		mapReduceDriver.withOutput(new Text(str2), new IntWritable(1));
		mapReduceDriver.withOutput(new Text(str3), new IntWritable(2));
		mapReduceDriver.withOutput(new Text(str4), new IntWritable(3));
 
		mapReduceDriver.runTest();
	}
}
