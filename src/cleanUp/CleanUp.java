package cleanUp;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
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

import java.net.URI;

public class CleanUp extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);

        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "Clean Up");

        job.setJarByClass(trendingTopics.TrendingTopics.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        cleanUp(job);

        return (job.waitForCompletion(true) ? 0 : 1);
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
                LongWritable.class, Text.class, Text.class, Text.class,
                new Configuration(false));
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CleanUp(), args);
        System.exit(exitCode);
    }
}
