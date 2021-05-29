package main;

import cleanUp.CleanUpDriver;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import topN.TopNDriver;
import topN.TopNMapper;
import topN.TopNReducer;
import trendingTopics.TrendingTopicsMapper;
import trendingTopics.TrendingTopicsReducer;

import java.net.URI;

import static cleanUp.CleanUpDriver.cleanUp;

public class TrendingTopicsDriver  {

    private static final String language = "es";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("N", Integer.parseInt(args[2]));
        conf.setStrings("lang", new String[]{language});

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        JobControl jobControl = new JobControl("Trending Topics");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]),
                trendingTopicOutputPath = new Path(outputPath + "/trendingTopics"),
                cleanUpOutputPath = new Path(outputPath + "/cleanUp"),
                topNOutputPath = new Path(outputPath + "/topN");
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job1 = Job.getInstance(conf, "Clean Up");
        job1.setJarByClass(CleanUpDriver.class);
        cleanUp(job1);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        /*SequenceFileOutputFormat.setCompressOutput(job1, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job1, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job1, SequenceFile.CompressionType.BLOCK);*/
        FileInputFormat.setInputDirRecursive(job1, true);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, cleanUpOutputPath);
        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);

        Job job2 = Job.getInstance(conf, "Trending Topics");
        job2.setJarByClass(TrendingTopicsDriver.class);
        job2.setMapperClass(TrendingTopicsMapper.class);
        job2.setReducerClass(TrendingTopicsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        /*SequenceFileOutputFormat.setCompressOutput(job2, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job2, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job2, SequenceFile.CompressionType.BLOCK);*/
        FileInputFormat.addInputPath(job2, cleanUpOutputPath);
        FileOutputFormat.setOutputPath(job2, trendingTopicOutputPath);
        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);

        Job job3 = Job.getInstance(conf, "TopN");
        job3.setJarByClass(TopNDriver.class);
        job3.setMapperClass(TopNMapper.class);
        job3.setReducerClass(TopNReducer.class);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, trendingTopicOutputPath);
        FileOutputFormat.setOutputPath(job3, topNOutputPath);
        ControlledJob controlledJob3 = new ControlledJob(conf);
        controlledJob3.setJob(job3);

        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);
        controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob2);

        Thread thread = new Thread(jobControl);
        thread.start();

        while(true){
            if(jobControl.allFinished()){
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                break;
            }
        }
    }
}
