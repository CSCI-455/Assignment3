package csci455.assignment3;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App
{
    private static final Collection<java.util.Map<Text,IntWritable>> TOKEN_MAP_LIST = new LinkedList<>();
    
    public static class Map extends Mapper<Object, Text, Text, IntWritable>
    {
            //parses input one line at a time
            //need to change this to parse and store peers in some structure
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            java.util.Map<Text, IntWritable> tokenMap = new HashMap<>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) 
            {
                Text word = new Text();
                word.set(itr.nextToken());
                tokenMap.put(word, new IntWritable(1));
                context.write(word, new IntWritable(1));
            }
            TOKEN_MAP_LIST.add(tokenMap);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
    {

    //right now this adds up the number of each word
    //in final implemetation, this will identify common peers of two nodes
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
        {
            java.util.Map<Text,IntWritable> tokenMap = flatten();
            Collection<Text> tokenSet = tokenMap.keySet();
            for (Text token : tokenSet)
            {
                context.write(token, tokenMap.get(token));
            }
        }
    }
    
    private static java.util.Map<Text, IntWritable> flatten()
    {
        java.util.Map<Text,IntWritable> result = new HashMap<>();
        for (java.util.Map<Text,IntWritable> current : TOKEN_MAP_LIST)
        {
            Collection<Text> tokensSet = current.keySet();
            for (Text token : tokensSet)
            {
                IntWritable temp = current.get(token);
                if (result.containsKey(token))
                {
                    int frequency = temp.get();
                    temp.set(frequency + 1);
                }
                result.put(token, temp);
            }
        }
        return result;
    }
    
    public static void main(String[] args) throws Exception {
        //sets up job configuration for hadoop
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(App.class);
    job.setMapperClass(Map.class);//splits lines into tokens
    job.setCombinerClass(IntSumReducer.class);//combines tokens
    job.setReducerClass(IntSumReducer.class);//finds common pairs
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}