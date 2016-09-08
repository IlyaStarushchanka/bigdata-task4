package com.epam.bigdata.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ilya_Starushchanka on 9/8/2016.
 */
public class SecondarySortJobTest {
    private MapDriver<LongWritable, Text, CikWritable, Text> mapDriver;
    private ReduceDriver<CikWritable, Text, NullWritable, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, CikWritable, Text, NullWritable, Text> mapReduceDriver;

    private String strIn1 = "8c66f1538798b7ab57e2da7be11c5696 20130606222224949 Z0KpO7S8PQpNDBa Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) 116.1.44.* 276 279 3 tMxYQ19aM98 8318e6e7deadc6405ee01f01d31d985a null LV_1001_LDVi_LD_ADX_2 300 250 0 0 100 e1af08818a6cd6bbba118bb54a651961 254 3476 282825712806 1";
    private String strIn2 = "2d34c0a50472ba3a4e3c83903437eae0 20130606222224950 Vh16L7SiOo1hJCC Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1 116.1.44.* 238 241 3 tMxYQ19aM98 202f6e8052731b38647f987dbc4a5db null LV_1001_LDVi_LD_ADX_1 300 250 0 0 100 00fccc64a1ee2809348509b7ac2a97a5 241 3427 282825712767 0";
    private String strIn3 = "8c66f1538798b7ab57e2da7be11c5696 20130606222224943 Z0KpO7S8PQpNDBa Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) 116.1.44.* 276 279 3 tMxYQ19aM98 8318e6e7deadc6405ee01f01d31d985a null LV_1001_LDVi_LD_ADX_2 300 250 0 0 100 e1af08818a6cd6bbba118bb54a651961 254 3476 282825712806 1";
    private String strIn4 = "2d34c0a50472ba3a4e3c83903437eae0 20130606222224958 Vh16L7SiOo1hJCC Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1 116.1.44.* 238 241 3 tMxYQ19aM98 202f6e8052731b38647f987dbc4a5db null LV_1001_LDVi_LD_ADX_1 300 250 0 0 100 00fccc64a1ee2809348509b7ac2a97a5 241 3427 282825712767 1";

    @Before
    public void setup(){
        SecondarySortJob.SortMapper mapper = new SecondarySortJob.SortMapper();
        SecondarySortJob.SortReduce reduce = new SecondarySortJob.SortReduce();
        mapDriver = new MapDriver<>(mapper);
        reduceDriver = new ReduceDriver<>(reduce);
        mapReduceDriver = new MapReduceDriver<>(mapper, reduce);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(strIn1));
        mapDriver.withInput(new LongWritable(1), new Text(strIn2));
        mapDriver.withInput(new LongWritable(1), new Text(strIn3));
        mapDriver.withInput(new LongWritable(1), new Text(strIn4));
        mapDriver.withOutput(new CikWritable("Z0KpO7S8PQpNDBa", 20130606222224949L), new Text(strIn1));
        mapDriver.withOutput(new CikWritable("Vh16L7SiOo1hJCC", 20130606222224950L), new Text(strIn2));
        mapDriver.withOutput(new CikWritable("Z0KpO7S8PQpNDBa", 20130606222224943L), new Text(strIn3));
        mapDriver.withOutput(new CikWritable("Vh16L7SiOo1hJCC", 20130606222224958L), new Text(strIn4));

        mapDriver.runTest();
    }

    @Test
    public void testReduce() throws IOException{
        List<Text> values1 = new ArrayList<Text>();
        values1.add(new Text(strIn1));
        List<Text> values3 = new ArrayList<Text>();
        values3.add(new Text(strIn3));

        //reduceDriver.withOutput(new Text(ip1), new CikWritable(1, 227));

        List<Text> values2 = new ArrayList<Text>();
        values2.add(new Text(strIn2));
        List<Text> values4 = new ArrayList<Text>();
        values4.add(new Text(strIn4));
        reduceDriver.withInput(new CikWritable("Vh16L7SiOo1hJCC", 20130606222224950L), values2);
        reduceDriver.withInput(new CikWritable("Vh16L7SiOo1hJCC", 20130606222224958L), values4);
        reduceDriver.withInput(new CikWritable("Z0KpO7S8PQpNDBa", 20130606222224943L), values3);
        reduceDriver.withInput(new CikWritable("Z0KpO7S8PQpNDBa", 20130606222224949L), values1);


        reduceDriver.withOutput(NullWritable.get(), new Text(strIn2));
        reduceDriver.withOutput(NullWritable.get(), new Text(strIn4));
        reduceDriver.withOutput(NullWritable.get(), new Text(strIn3));
        reduceDriver.withOutput(NullWritable.get(), new Text(strIn1));

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException{
        mapReduceDriver.withInput(new LongWritable(), new Text(strIn1));
        mapReduceDriver.withInput(new LongWritable(), new Text(strIn2));
        mapReduceDriver.withInput(new LongWritable(), new Text(strIn3));
        mapReduceDriver.withInput(new LongWritable(), new Text(strIn4));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(strIn2));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(strIn4));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(strIn3));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(strIn1));

        mapReduceDriver.runTest();
    }
}
