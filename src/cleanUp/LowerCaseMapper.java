package cleanUp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.Locale;

public class LowerCaseMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // change tweets letter case to either lower-case

        try {
            JSONObject json = new JSONObject(value.toString());
            json.put("text", json.getString("text").toLowerCase());

            context.write(new Text(key.toString()), new Text(value.toString()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
