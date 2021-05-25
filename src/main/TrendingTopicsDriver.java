package main;

import cleanUp.CleanUpDriver;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import trendingTopics.TrendingTopicsMapper;
import trendingTopics.TrendingTopicsReducer;

import java.net.URI;

public class TrendingTopicsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        //JobControl jobControl = new JobControl("Trending Topics");

        Job job = Job.getInstance(conf, "Trending Topics");

        job.setJarByClass(trendingTopics.TrendingTopicsDriver.class);

        // Setting the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);

        fs.delete(outputPath, true);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        CleanUpDriver.cleanUp(job);

        ChainMapper.addMapper(job, TrendingTopicsMapper.class,
                Object.class, Text.class, Text.class, IntWritable.class,
                new Configuration(false));

        ChainReducer.setReducer(job, TrendingTopicsReducer.class,
                Text.class, IntWritable.class, IntWritable.class, Text.class,
                conf);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new trendingTopics.TrendingTopicsDriver(), args);
        System.exit(exitCode);
    }
}
