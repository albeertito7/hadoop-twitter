package cleanUp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class LowerCaseMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // change tweets letter case to either lower-case

        try {
            JSONObject json = new JSONObject(value.toString());

            if (json.has("text")) { // maybe this it's not necessary cuz of EmptyFieldsMapper
                json.put("text", json.getString("text").toLowerCase());
            }

            context.write(NullWritable.get(), new Text(json.toString()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
