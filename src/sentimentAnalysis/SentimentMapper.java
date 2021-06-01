package sentimentAnalysis;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

public class SentimentMapper extends Mapper<NullWritable, Text, Text, SentimentWritable> {

    private static Set<String> positiveWords = new HashSet<>();
    private static Set<String> negativeWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();

        Scanner scanner;
        try {
            scanner = new Scanner(new FileReader(files[0].getPath()));
            while (scanner.hasNextLine()) {
                positiveWords.add(scanner.nextLine().toLowerCase());
            }

            scanner = new Scanner(new FileReader(files[1].getPath()));
            while (scanner.hasNextLine()) {
                negativeWords.add(scanner.nextLine().toLowerCase());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            Pattern pattern = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
            JSONObject json = new JSONObject(value.toString()); // data from cleanUp
            String[] tokens = json.getString("text").split(" ");
            JSONArray hashtags = json.getJSONArray("hashtags");

            int counter = 0;
            for (int i = 0; i < tokens.length; i++) {
                if (pattern.matcher(tokens[i]).find()) {
                    continue;
                }
                else if(positiveWords.contains(tokens[i])) {
                    counter++;
                }
                else if (negativeWords.contains(tokens[i])) {
                    counter--;
                }
            }

            for (int i = 0; i < hashtags.length(); i++) {
                JSONObject hashtag = hashtags.getJSONObject(i);
                context.write(new Text(hashtag.getString("text")), new SentimentWritable(new FloatWritable(counter), new IntWritable(tokens.length)));
            }


        } catch (JSONException e) {
            e.printStackTrace();
        }

    }
}
