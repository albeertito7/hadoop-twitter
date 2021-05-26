package trendingTopics;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingTopicsReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int count = 0;
        for (IntWritable val : values)
        {
            count += val.get();
        }

        context.write(new IntWritable(count), new Text(key.toString()));
    }
}
