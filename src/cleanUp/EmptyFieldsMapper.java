package cleanUp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.log.Log;

import java.io.IOException;

public class EmptyFieldsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString()), entities;
            if(!json.has("text") || (entities = json.getJSONObject("entities")) == null || entities.getJSONArray("hashtags").length() == 0) {
                return;
            }

            context.write(key, new Text(json.toString()));

        } catch (JSONException e) {
            Log.info("JSONException CustomFieldsMapper", e.toString());
        }
    }
}