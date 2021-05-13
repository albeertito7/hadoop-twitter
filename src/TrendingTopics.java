import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;

public class TrendingTopics extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TrendingTopics(), args);
        System.exit(exitCode);
    }
}
