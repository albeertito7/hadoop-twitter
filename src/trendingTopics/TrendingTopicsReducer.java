package trendingTopics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TrendingTopicsReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

    private TreeMap<Integer, String> tMapRed;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
        tMapRed = new TreeMap<>();
    }

    @Override
    public void reduce(Text hashTag, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int count = 0;
        for (IntWritable val : values)
            count += val.get();

        tMapRed.put(count, hashTag.toString());
    }

    /**
     * This method is called once after all key/value pairs have been presented to the
     * map method.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        for (Map.Entry<Integer, String> entry : tMapRed.entrySet())
        {
            int hashTagCount = entry.getKey();
            String name = entry.getValue();
            context.write(new IntWritable(hashTagCount), new Text(name));
        }
    }
}
