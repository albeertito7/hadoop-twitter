package sentimentAnalysis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class SentimentReducer extends Reducer<Text, SentimentWritable, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<SentimentWritable> values, Context context) throws IOException, InterruptedException {
        float ratio = 0.0f;

        for (SentimentWritable value: values) {
            ratio += value.getRatio().get() / value.getLength().get();
        }

        if (ratio != 0.0f && ratio != -0.0f) {
            BigDecimal bd = new BigDecimal(Math.abs(ratio) * 100f).setScale(2, RoundingMode.HALF_UP);
            ratio = bd.floatValue();
            context.write(NullWritable.get(), new Text(key + ";" + ratio + "%;" + (ratio < 0.0f ? "negative" : "positive")));
        }
    }
}
