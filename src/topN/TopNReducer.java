package topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<Integer, Text> tMap;
    private static int N;

    @Override
    protected void setup (Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>();
        Configuration conf = context.getConfiguration();
        N = conf.getInt("N", 5);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text val: values)
        {
            String[] tokens = val.toString().split("\t");
            tMap.put(Integer.parseInt(tokens[0]), new Text(tokens[0] + "\t" + tokens[1]));

            if (tMap.size() > N) {
                tMap.remove(tMap.firstKey());
            }
        }
    }

    @Override
    protected void cleanup (Context context) throws IOException, InterruptedException
    {
        for (Map.Entry<Integer, Text> entry : tMap.entrySet())
        {
            context.write(NullWritable.get(), new Text(entry.getValue()));
        }
    }
}