package cleanUp;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import sentimentAnalysis.SentimentMapper;
import trendingTopics.TrendingTopicsDriver;

import java.net.URI;

import static main.TimeHelper.getTIME;

public class CleanUpDriver extends Configured implements Tool {

    private static final String language = "es";

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.setStrings("lang", new String[]{language});

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "Clean Up");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        cleanUp(job);

        long timeStart = System.currentTimeMillis();
        if(job.waitForCompletion(true)) {
            System.out.println("Elapsed time: " + getTIME(System.currentTimeMillis() - timeStart));
            return 0;
        }
        return 1;
    }

    public static void cleanUp(Job job) throws java.io.IOException {
        ChainMapper.addMapper(job, LanguageMapper.class,
                LongWritable.class, Text.class, LongWritable.class, Text.class,
                new Configuration(false));

        ChainMapper.addMapper(job, EmptyFieldsMapper.class,
                LongWritable.class, Text.class, LongWritable.class, Text.class,
                new Configuration(false));

        ChainMapper.addMapper(job, CustomFieldsMapper.class,
                LongWritable.class, Text.class, LongWritable.class, Text.class,
                new Configuration(false));

        ChainMapper.addMapper(job, LowerCaseMapper.class,
                LongWritable.class, Text.class, NullWritable.class, Text.class,
                new Configuration(false));
    }

    /**
     * arg[0] = input directory or file (format .json)
     * arg[1] = output directory
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CleanUpDriver(), args);
        System.exit(exitCode);
    }
}
