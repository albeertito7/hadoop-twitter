package sentimentAnalysis;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SentimentReducer extends Reducer<Text, SentimentWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<SentimentWritable> values, Context context) throws IOException, InterruptedException {
        SentimentWritable sentiment;
        float ratio = 0.0f;

        for (SentimentWritable value: values) {
            ratio += (float)value.getRatio().get() / (float) value.getLength().get();
        }

        if (ratio != 0.0f) {
            ratio = Math.abs(ratio) * 100f;
            context.write(key, new Text(ratio + "% " + (ratio < 0.0F ? "negative" : "positive")));
        }
    }
}
