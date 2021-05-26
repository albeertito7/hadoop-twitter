package topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private TreeMap<Integer, String> tMap;
    private static int N;

    @Override
    public void setup (Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>();
        Configuration conf = context.getConfiguration();
        N = conf.getInt("N", 5);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String name = key.toString();
        int count = 0;

        for (Text val : values)
        {
            tMap.put(Integer.parseInt(key.toString()), val.toString());
        }

        if (tMap.size() > N) {
            tMap.remove(tMap.firstKey());
        }
    }

    @Override
    public void cleanup (Context context) throws IOException, InterruptedException
    {
        for (Map.Entry<Integer, String> entry : tMap.entrySet())
        {
            int count = entry.getKey();
            String name = entry.getValue();
            context.write(new IntWritable(count), new Text(name));
        }
    }
}