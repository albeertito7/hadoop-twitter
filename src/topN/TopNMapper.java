package topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {

    private TreeMap<String, Integer> tMap; // to sort the top N hashtags
    private int N_2;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>();
        Configuration conf = context.getConfiguration();
        N_2 = 2*conf.getInt("N", 5);
    }

    // To get a better approximation to the Top-N hastags, it is recommendable that the mapper
    // calculates the Top-2N hashtags and the reducer the Top-N.
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        /*String[] tokens = value.toString().split("\t");
        tMap.put(tokens[1], Integer.parseInt(tokens[0]));*/

        tMap.put(value.toString(), Integer.parseInt(key.toString())); // <hashtag, count>

        if (tMap.size() > N_2) {
            tMap.remove(tMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<String, Integer> entry : tMap.entrySet())
        {
            int count = entry.getValue();
            context.write(new Text(entry.getKey()), new IntWritable(count));
        }
    }
}