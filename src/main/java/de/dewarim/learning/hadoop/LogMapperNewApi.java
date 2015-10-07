package de.dewarim.learning.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.http.HttpStatus;

import java.io.IOException;

/**
 * Mapper using new API: extract the url part of a apache log file message.
 */
public class LogMapperNewApi extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    /*
     * Example:
     * 302 317 [09/Jan/2015:00:06:30 +0100] "GET /favicon.ico HTTP/1.1" "-" "littlegoblin.de" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0"
     */

    public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\\s+");
        if (fields.length < 8) {
            return;
        }

        String hostname = fields[8].replace("\"", "");
        String file = fields[5];

        int statusCode = Integer.parseInt(fields[0]);
        try {
            HttpStatus status = HttpStatus.valueOf(statusCode);
            context.getCounter(status).increment(1);
        }
        catch (IllegalArgumentException e){
            System.err.println("Http status code not found for: "+statusCode);
        }

        context.write(new Text(hostname + file), new IntWritable(1));
    }

}
