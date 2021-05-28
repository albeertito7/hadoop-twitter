package trendingTopics;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrendingTopicsMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {

    private final static IntWritable one = new IntWritable(1);

    /*public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        Pattern pattern = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)"); // regular expression to access the hashtag
        Matcher matcher = pattern.matcher(value.toString());

        while (matcher.find()) {
            String word = matcher.group(1).trim();
            context.write(new Text(word), one);
        }
    }*/

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString());
            JSONArray values = json.getJSONArray("hashtags");

            for(int i = 0; i < values.length(); ++i) {
                JSONObject hashtag = values.getJSONObject(i);
                String text = hashtag.getString("text");
                if (!text.isEmpty()) {
                    context.write(new Text(text), one);
                }
            }
        } catch (JSONException var9) {
            var9.printStackTrace();
        }
    }
}
