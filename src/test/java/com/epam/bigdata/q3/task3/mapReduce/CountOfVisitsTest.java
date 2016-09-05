package com.epam.bigdata.q3.task3.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CountOfVisitsTest {
	MapDriver<Object, Text, Text, VisitSpend> mapDriver;
	ReduceDriver<Text, VisitSpend, Text, VisitSpend> reduceDriver;
	MapReduceDriver<Object, Text, Text, VisitSpend, Text, VisitSpend> mapReduceDriver;	

	private String input1 = "2d34c0a50472ba3a4e3c83903437eae0	20130606222224950	Vh16L7SiOo1hJCC	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	116.1.44.*	238	241	3	tMxYQ19aM98	202f6e8052731b38647f987dbc4a5db	null	LV_1001_LDVi_LD_ADX_1	300	250	0	0	100	00fccc64a1ee2809348509b7ac2a97a5	241	3427	282825712767	0";
	private String input2 = "70937d8c62d86921c54cc21255e885c1	20130606222224950	Vh16L7SiOo1hJCC	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	116.1.44.*	238	241	3	tMxYQ19aM98	202f6e8052731b38647f987dbc4a5db	null	LV_1001_LDVi_LD_ADX_2	300	250	0	0	100	00fccc64a1ee2809348509b7ac2a97a5	241	3427	282825712806	0";
	private String input3 ="119672f6d14fdd1cbadede252643b441	20130606222224953	Z0Ti16KyPZ5-kBC	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQDownload 734)	115.225.225.*	94	100	3	tMxYQ19aM98	e29481d5e03f2d39e527f86eb86f7a65	null	LV_1001_LDVi_LD_ADX_2	300	250	0	0	100	00fccc64a1ee2809348509b7ac2a97a5	241	3427	282825712806	0";
	
	private String ip1 = "116.1.44.*";
	private String ip2 = "115.225.225.*";
	
	@Before
    public void setUp() {
		VSCount.Map mapper = new VSCount.Map();
		VSCount.Reduce reducer = new VSCount.Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
	
    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input1));
        mapDriver.withInput(new LongWritable(), new Text(input2));
        mapDriver.withInput(new LongWritable(), new Text(input3));
        mapDriver.withOutput(new Text(ip1), new VisitSpend(1, 3427));
        mapDriver.withOutput(new Text(ip1), new VisitSpend(1, 3427));
        mapDriver.withOutput(new Text(ip2), new VisitSpend(1, 3427));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<VisitSpend> values1 = new ArrayList<VisitSpend>();
        values1.add(new VisitSpend(1, 3427));
        values1.add(new VisitSpend(1, 3427));
        reduceDriver.withInput(new Text(ip1), values1);

        List<VisitSpend> values2 = new ArrayList<VisitSpend>();
        values2.add(new VisitSpend(1, 3427));
        reduceDriver.withInput(new Text(ip2), values2);

        reduceDriver.withOutput(new Text(ip1), new VisitSpend(2, 6854));
        reduceDriver.withOutput(new Text(ip2), new VisitSpend(1, 3427));
        reduceDriver.runTest();
    }
    
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(input1));
        mapReduceDriver.withInput(new LongWritable(), new Text(input2));
        mapReduceDriver.withInput(new LongWritable(), new Text(input3));
        
        mapReduceDriver.withOutput(new Text(ip2), new VisitSpend(1, 3427));
        mapReduceDriver.withOutput(new Text(ip1), new VisitSpend(2, 6854));


        mapReduceDriver.runTest();
    }
}
