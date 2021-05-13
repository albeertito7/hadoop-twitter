import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;

public class TrendingTopics extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        // regular expression to access the hashtag
        // "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)"

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TrendingTopics(), args);
        System.exit(exitCode);
    }
}
