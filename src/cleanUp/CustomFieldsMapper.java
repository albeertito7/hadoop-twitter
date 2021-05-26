package cleanUp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.log.Log;

import java.io.IOException;

public class CustomFieldsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString()),
                    jsonObject = new JSONObject();

            jsonObject.put("hashtags", json.getJSONObject("entities").getJSONArray("hashtags"));
            jsonObject.put("text", json.getString("text"));
            jsonObject.put("lang", json.getString("lang"));

            context.write(key, new Text(jsonObject.toString()));
        }
        catch (JSONException e) {
            Log.info("JSONException CustomFieldsMapper", e.toString());
        }
    }
}