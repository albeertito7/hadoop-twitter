package sentimentAnalysis;

import java.net.URI;

import cleanUp.LanguageMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static cleanUp.CleanUpDriver.cleanUp;
import static main.TimeHelper.getTIME;

public class SentimentAnalysis extends Configured implements Tool {

    private static final String language = "es";

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        conf.setStrings("lang", new String[]{language});
        args = (new GenericOptionsParser(conf, args)).getRemainingArgs();

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]),
                positivePath = new Path(args[2]), negativePath = new Path(args[3]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.addCacheFile(new URI(positivePath.toString()));
        job.addCacheFile(new URI(negativePath.toString()));
        //job.setCombinerClass(SentimentReducer.class);

        cleanUp(job);

        ChainMapper.addMapper(job, SentimentMapper.class,
                NullWritable.class, Text.class, Text.class, SentimentWritable.class,
                new Configuration(false));

        ChainReducer.setReducer(job, SentimentReducer.class,
                Text.class, SentimentWritable.class, Text.class, Text.class,
                new Configuration(false));

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        long timeStart = System.currentTimeMillis();
        if(job.waitForCompletion(true)) {
            System.out.println("Elapsed time: " + getTIME(System.currentTimeMillis() - timeStart));
            return 0;
        }

        return 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SentimentAnalysis(), args);
        System.exit(exitCode);
    }
}
