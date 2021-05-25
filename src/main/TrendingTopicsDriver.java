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
import topN.TopNMapper;
import topN.TopNReducer;
import trendingTopics.TrendingTopicsMapper;
import trendingTopics.TrendingTopicsReducer;

import java.net.URI;

public class TrendingTopicsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        //JobControl jobControl = new JobControl("Trending Topics");

        Job job1 = Job.getInstance(conf, "Trending Topics");
        Job job2 = Job.getInstance(conf, "TopN");

        job1.setJarByClass(main.TrendingTopicsDriver.class);
        job2.setJarByClass(topN.TopNDriver.class);

        // Setting the input and output path
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);

        fs.delete(outputPath, true);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        CleanUpDriver.cleanUp(job1);

        ChainMapper.addMapper(job1, TrendingTopicsMapper.class,
                Object.class, Text.class, Text.class, IntWritable.class,
                new Configuration(false));

        ChainReducer.setReducer(job1, TrendingTopicsReducer.class,
                Text.class, IntWritable.class, IntWritable.class, Text.class,
                new Configuration(false));

        ChainMapper.addMapper(job2, TopNMapper.class,
                Object.class, Text.class, Text.class, IntWritable.class,
                new Configuration(false));

        ChainReducer.setReducer(job2, TopNReducer.class,
                Text.class, IntWritable.class, IntWritable.class, Text.class,
                new Configuration(false));

        return (job2.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new trendingTopics.TrendingTopicsDriver(), args);
        System.exit(exitCode);
    }
}
