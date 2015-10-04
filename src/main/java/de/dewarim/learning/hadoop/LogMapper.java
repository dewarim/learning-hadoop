package de.dewarim.learning.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * First mapper: extract the url part of a apache log file message.
 * 
 */
public class LogMapper implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    /*
     * Example:
     * 302 317 [09/Jan/2015:00:06:30 +0100] "GET /favicon.ico HTTP/1.1" "-" "littlegoblin.de" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0"
     */

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        
        // Note: use old API of org.apache.hadoop.mapred.Mapper instead of org.apache.hadoop.mapreduce.Mapper because
        // MRUnit is unhappy otherwise.
        //        public void map(LongWritable key, Text value, Context context)
        //            throws IOException, InterruptedException {

        String line = text.toString();
        String[] fields = line.split("\\s+");
        if (fields.length < 8) {
            return;
        }
        
        String hostname = fields[8].replace("\"", "");
        String file = fields[5];
        outputCollector.collect(new Text(hostname + file), new IntWritable(1));
    }

    @Override
    public void configure(JobConf jobConf) {
        
    }

    @Override
    public void close() throws IOException {
        
    }
}
