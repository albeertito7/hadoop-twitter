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

import static main.TimeHelper.getTIME;

public class TrendingTopicsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path inputPath = new Path(args[0]),
                outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true); // delete output directory if already exists

        Job job = Job.getInstance(conf, "Trending Topics");

        job.setJarByClass(this.getClass());
        job.setMapperClass(TrendingTopicsMapper.class);
        job.setCombinerClass(TrendingTopicsReducer.class);
        job.setReducerClass(TrendingTopicsReducer.class);

        // As the mapper and reducer out types differ
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath); // The input can be can be a file, directory or a file pattern
        FileOutputFormat.setOutputPath(job, outputPath); // The output directory must not exist

        long timeStart = System.currentTimeMillis();
        if(job.waitForCompletion(true)) {
            System.out.println("Elapsed time: " + getTIME(System.currentTimeMillis() - timeStart));
            return 0;
        }
        return 1;
    }

    /***
     * arg[0] = input .txt file
     * arg[1] = output directory
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TrendingTopicsDriver(), args);
        System.exit(exitCode);
    }
}
