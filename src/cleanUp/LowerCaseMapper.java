package cleanUp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LowerCaseMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String word = key.toString().toLowerCase();
        // change tweets letter case to either lower-case

        context.write(new Text(word), new Text(value.toString().toLowerCase()));
    }
}
