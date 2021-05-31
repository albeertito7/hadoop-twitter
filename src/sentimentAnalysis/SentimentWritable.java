package sentimentAnalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SentimentWritable implements Writable {
    private final IntWritable ratio;
    private final IntWritable tweetLength;

    public SentimentWritable() {
        this.ratio = new IntWritable(0);
        this.tweetLength = new IntWritable(0);
    }

    public SentimentWritable(IntWritable ratio, IntWritable len) {
        this.ratio = ratio;
        this.tweetLength = len;
    }

    public IntWritable getRatio() {
        return this.ratio;
    }

    public IntWritable getLength() {
        return this.tweetLength;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.tweetLength.write(dataOutput);
        this.ratio.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.tweetLength.readFields(dataInput);
        this.ratio.readFields(dataInput);
    }
}