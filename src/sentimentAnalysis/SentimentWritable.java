package sentimentAnalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SentimentWritable implements Writable {
    private FloatWritable ratio;
    private IntWritable tweetLength;

    public SentimentWritable() {
        this.ratio = new FloatWritable(0);
        this.tweetLength = new IntWritable(0);
    }

    public SentimentWritable(FloatWritable ratio, IntWritable length) {
        this.ratio = ratio;
        this.tweetLength = length;
    }

    public FloatWritable getRatio() {
        return this.ratio;
    }

    public IntWritable getLength() {
        return this.tweetLength;
    }

    public void setRatio(FloatWritable ratio){
        this.ratio = ratio;
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