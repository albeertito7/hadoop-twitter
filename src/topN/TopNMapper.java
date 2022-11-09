package topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private TreeMap<Integer, String> tMap; // to sort the top N hashtags
    private static int N_2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>();
        Configuration conf = context.getConfiguration();
        N_2 = conf.getInt("N", 5) * 2;
    }

    // To get a better approximation to the Top-N hastags, it is recommendable that the mapper
    // calculates the Top-2N hashtags and the reducer the Top-N.
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        tMap.put(Integer.parseInt(tokens[1]), tokens[0]); // <count, hashtag>

        if (tMap.size() > N_2) {
            tMap.remove(tMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, String> entry : tMap.entrySet())
        {
            context.write(NullWritable.get(), new Text(Integer.parseInt(entry.getKey().toString())+ "\t" + entry.getValue()));
        }
    }
}