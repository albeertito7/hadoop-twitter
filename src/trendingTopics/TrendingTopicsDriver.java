package trendingTopics;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class TrendingTopicsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true); // delete output directory if already exists

        Job job = Job.getInstance(conf, "Trending Topics");

        job.setJarByClass(TrendingTopicsDriver.class);
        job.setMapperClass(TrendingTopicsMapper.class);
        job.setReducerClass(TrendingTopicsReducer.class);

        /* As the mapper and reducer out types differ... */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // The input can be can be a file, directory or a file pattern
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // The output directory must not exist

        return (job.waitForCompletion(true) ? 0 : 1); // Submit mapreduce job and wait for completion
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TrendingTopicsDriver(), args);
        System.exit(exitCode);
    }
}
