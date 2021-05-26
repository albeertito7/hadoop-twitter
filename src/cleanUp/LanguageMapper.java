package cleanUp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.log.Log;

import java.io.IOException;

public class LanguageMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static String language;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        language = conf.getStrings("lang")[0];
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString());

            if (!json.getString("lang").equals(language)) {
                return;
            }

            context.write(key, new Text(json.toString()));
        }
        catch (JSONException e) {
            Log.info("JSONException LanguageMapper", e.toString());
        }
    }
}
