package csci455.assignment3;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
    private static List<java.util.Map<Text,IntWritable>> tokenMapList = new LinkedList<>();
    
    public static class Map extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
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
                tokenMap.put(word, one);
                context.write(word, one);
            }
            tokenMapList.add(tokenMap);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
    {
        private IntWritable result = new IntWritable();

    //right now this adds up the number of each word
    //in final implemetation, this will identify common peers of two nodes
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
        {
            java.util.Map<Text,IntWritable> tokenMap = flatten();
            Set<Text> tokenSet = tokenMap.keySet();
            for (Text token : tokenSet)
            {
                context.write(token, tokenMap.get(token));
            }
        }
    }
    
    private static java.util.Map<Text, IntWritable> flatten()
    {
        java.util.Map<Text,IntWritable> result = new HashMap<>();
        for (java.util.Map<Text,IntWritable> current : tokenMapList)
        {
            Set<Text> tokensSet = current.keySet();
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