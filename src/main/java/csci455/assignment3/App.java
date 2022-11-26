package csci455.assignment3;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class App
{
    
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        //parses input one line at a time
        //need to change this to parse and store peers in some structure
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) { //one line per loop
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        
        //right now this adds up the number of each word
        //in final implemetation, this will identify common peers of two nodes
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception
    {
    //sets up job configuration for hadoop
    JobConf conf = new JobConf(App.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);//splits lines into tokens
    conf.setCombinerClass(Reduce.class);//combines tokens 
    conf.setReducerClass(Reduce.class);//finds common pairs

    conf.setInputFormat(TextInputFormat.class);//sets up input
    conf.setOutputFormat(TextOutputFormat.class);//sets up output

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);//runs the job
    }
}