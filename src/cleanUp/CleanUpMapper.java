package cleanUp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CleanUpMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String word = key.toString().toLowerCase();

        // change tweets letter case to either lower-case
        // Removal of tweets with lacking some of the processed fields (hashtag, text)
        // Filter the fields not used in the processing (we use only the following fields: hashtag, text and language).
        // Filter the tweets with a different language ("lang":"es" or "lang":"en")

        /* Tips
        *   1. Use the chain mapper to perform the tweets clean-up
        *   2. You can use the hadoop-predefined FieldSelectionMapper mappers to filter the fields
        */

        if (!value.toString().isEmpty()) {
            context.write(new Text(word), new Text(value.toString().toLowerCase()));
        }
    }
}
