import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingTopicsReducer extends Reducer<Text, IntWritable, IntWritable,Text> {

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {

    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {

    }
}
