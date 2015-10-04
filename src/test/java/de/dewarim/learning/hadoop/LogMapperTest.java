package de.dewarim.learning.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 */
public class LogMapperTest {


    String testLine = "302 317 [09/Jan/2015:00:06:30 +0100] \"GET /favicon.ico HTTP/1.1\" \"-\" \"littlegoblin.de\" " +
            "\"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0\"";

    @Test
    public void processesValidRecord() throws IOException, InterruptedException {

        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<>();
        mapDriver.withMapper(new LogMapper());
        mapDriver.withInput(new LongWritable(0), new Text(testLine));
        mapDriver.withOutput(new Text("littlegoblin.de/favicon.ico"), new IntWritable(1));
        mapDriver.runTest();
    }
}
